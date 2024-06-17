import argparse
import dask.dataframe as dd
import dask.array as da
import xgboost as xgb
from dask.distributed import LocalCluster, Client
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, mean_squared_error, r2_score
import json
import os
import numpy as np
import random
import mmap

def load_data(csv_path):
    df = dd.read_csv(csv_path)
    df = df.fillna('')
    return df

def preprocess_data(df, params_columns, target_column=None, is_test_data=False):
    encoders = {}
    scalers = {}

    def encode_and_scale(df):
        df = df.copy()
        for column in params_columns:
            if df[column].dtype == 'object':
                encoder = LabelEncoder()
                df[column] = encoder.fit_transform(df[column].astype(str))
                encoders[column] = encoder
            else:
                scaler = StandardScaler()
                df[column] = scaler.fit_transform(df[[column]]).flatten()
                scalers[column] = scaler

        if target_column and not is_test_data:
            if df[target_column].dtype == 'object':
                encoder = LabelEncoder()
                df[target_column] = encoder.fit_transform(df[target_column].astype(str))
                encoders[target_column] = encoder

        return df

    meta = {col: 'f8' if df[col].dtype != 'object' else 'i8' for col in df.columns}
    df = df.map_partitions(encode_and_scale, meta=meta)
    return df, encoders, scalers

def split_data(df):
    if 'XGB_TYPE' in df.columns:
        train_df = df[df['XGB_TYPE'] == 'TRAIN']
        validate_df = df[df['XGB_TYPE'] == 'VALIDATE']
        test_df = df[df['XGB_TYPE'] == 'TEST']
    else:
        train_df, test_df = df.random_split([0.8, 0.2], random_state=42)
        validate_df = dd.from_pandas(pd.DataFrame(), npartitions=1)

    return train_df, validate_df, test_df


def train_xgb_model(client, train_df, validate_df, params_columns, target_column, config, is_classification=True):

    config['verbosity'] = 0
    X_train = train_df[params_columns]
    y_train = train_df[target_column]

    d_train = xgb.dask.DaskDMatrix(client, X_train, y_train, enable_categorical=True)

    evals = []
    if len(validate_df) > 0:
        X_validate = validate_df[params_columns]
        y_validate = validate_df[target_column]
        d_validate = xgb.dask.DaskDMatrix(client, X_validate, y_validate, enable_categorical=True)
        evals = [(d_validate, 'validation')]

    model = xgb.dask.train(client, config, d_train, evals=evals)

    return model['booster']



def save_model(model, params_columns, model_dir, model_name):
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"{model_name}.json")
    model.save_model(model_path)

def load_model_and_feature_names(model_path):
    model = xgb.Booster()
    model.load_model(model_path)
    feature_names = model.feature_names
    if feature_names is None:
        raise ValueError("Feature names are not found in the model.")
    return model, feature_names

def evaluate_model(client, model, test_df, params_columns, target_column=None, is_classification=True):
    # Ensure test_df contains only test data
    test_data = test_df[test_df['XGB_TYPE'] == 'TEST'].copy()
    X_test = test_data[params_columns]
    y_test = test_data[target_column]
    unique_ids = test_data['unique_id']

    # Use DaskDMatrix for the test data
    d_test = xgb.dask.DaskDMatrix(client, X_test, y_test, enable_categorical=True)
    predictions = xgb.dask.predict(client, model, d_test)
    rounded_predictions = da.round(predictions, 2)

    y_test = y_test.compute()
    predictions = rounded_predictions.compute()

    if is_classification:
        y_test = y_test.astype(int)
        predictions = predictions.astype(int)
        report = classification_report(y_test, predictions, zero_division=0, output_dict=True)
    else:
        y_test = y_test.astype(float)
        predictions = predictions.astype(float)
        mse = mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)
        report = {'mean_squared_error': mse, 'r2_score': r2}

    return da.from_array(predictions), unique_ids, report

def invoke_model(client, model, df, params_columns, prediction_column):
    X = df[params_columns]
    dmatrix = xgb.dask.DaskDMatrix(client, X)
    predictions = xgb.dask.predict(client, model, dmatrix)
    rounded_predictions = da.round(predictions, 2)

    return rounded_predictions

def randomize_xgb_config(base_config):
    randomized_config = base_config.copy()
    randomized_config['max_depth'] = random.randint(3, 10)
    randomized_config['learning_rate'] = random.uniform(0.01, 0.3)
    randomized_config['gamma'] = random.uniform(0, 1)
    randomized_config['min_child_weight'] = random.uniform(1, 10)
    randomized_config['subsample'] = random.uniform(0.5, 1.0)
    randomized_config['colsample_bytree'] = random.uniform(0.5, 1.0)
    randomized_config['lambda'] = random.uniform(0, 10)
    randomized_config['alpha'] = random.uniform(0, 10)
    randomized_config['scale_pos_weight'] = random.uniform(0.5, 5.0)
    randomized_config['max_delta_step'] = random.uniform(0, 10)
    return randomized_config

def calculate_deviation(base_config, config):
    deviation = {}
    for key in base_config.keys():
        if isinstance(base_config[key], (int, float)) and isinstance(config[key], (int, float)):
            deviation[key] = config[key] - base_config[key]
        else:
            deviation[key] = "N/A"
    return deviation

def main():
    parser = argparse.ArgumentParser(description='XGBoost Model Training and Prediction Script')
    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)
    parser.add_argument('--csv_path', type=str, help='Path to the CSV file', required=True)
    parser.add_argument('--params', type=str, help='Comma-separated column names to use as parameters for model training', required=True)
    parser.add_argument('--target_column', type=str, help='Name of the target column', required=False)
    parser.add_argument('--prediction_column', type=str, help='Name of the prediction column', required=True)
    parser.add_argument('--model_path', type=str, help='Path to a pre-trained XGBoost model', required=False)
    parser.add_argument('--model_dir', type=str, help='Directory to save the trained model', required=False)
    parser.add_argument('--model_name', type=str, help='Specify the name of the trained model', required=False)
    parser.add_argument('--objective', type=str, default='binary:logistic', help='Objective function')
    parser.add_argument('--max_depth', type=int, default=6, help='Maximum tree depth for base learners')
    parser.add_argument('--learning_rate', type=float, default=0.05, help='Boosting learning rate')
    parser.add_argument('--n_estimators', type=int, default=200, help='Number of boosting rounds')
    parser.add_argument('--gamma', type=float, default=0.0, help='Minimum loss reduction required to make a further partition on a leaf node')
    parser.add_argument('--min_child_weight', type=float, default=1, help='Minimum sum of instance weight (hessian) needed in a child')
    parser.add_argument('--subsample', type=float, default=0.8, help='Subsample ratio of the training instances')
    parser.add_argument('--colsample_bytree', type=float, default=0.8, help='Subsample ratio of columns when constructing each tree')
    parser.add_argument('--reg_lambda', type=float, default=1.0, help='L2 regularization term on weights')
    parser.add_argument('--reg_alpha', type=float, default=0.0, help='L1 regularization term on weights')
    parser.add_argument('--scale_pos_weight', type=float, default=1.0, help='Balancing of positive and negative weights')
    parser.add_argument('--max_delta_step', type=float, default=0.0, help='Maximum delta step we allow each tree’s weight estimation to be')
    parser.add_argument('--booster', type=str, default='gbtree', help='Which booster to use')
    parser.add_argument('--tree_method', type=str, default='auto', help='Specify the tree construction algorithm used in XGBoost')
    parser.add_argument('--grow_policy', type=str, default='depthwise', help='Controls a way new nodes are added to the tree')
    parser.add_argument('--eval_metric', type=str, default='rmse', help='Evaluation metric for validation data')
    parser.add_argument('--early_stopping_rounds', type=int, default=10, help='Activates early stopping. Validation metric needs to improve at least once in every *early_stopping_rounds* round(s) to continue training')
    parser.add_argument('--device', type=str, default='cpu', help='Device to run the training on (e.g., "cpu", "cuda")')
    parser.add_argument('--cv', type=int, default=5, help='Number of cross-validation folds')
    parser.add_argument('--interaction_constraints', type=str, help='Constraints for interaction between variables')
    parser.add_argument('--hyperparameter_optimization_attempts', type=int, default=0, help='Set to above 0 to activate')
    parser.add_argument('--hyperparameter_optimization_result_display_limit', type=int, default=3, help='Adjust this to change how many rankings of hyperparameter optimizations are returned')
    parser.add_argument('--dask_workers', type=int, default=4, help='Number of dask workers')
    parser.add_argument('--dask_threads_per_worker', type=int, default=1, help='Number of threads per dask worker')

    args = parser.parse_args()
    params_columns = [param.strip() for param in args.params.split(',')]
    prediction_column = args.prediction_column

    base_xgb_config = {
        "objective": args.objective,
        "max_depth": args.max_depth,
        "learning_rate": args.learning_rate,
        "n_estimators": args.n_estimators,
        "gamma": args.gamma,
        "min_child_weight": args.min_child_weight,
        "subsample": args.subsample,
        "colsample_bytree": args.colsample_bytree,
        "lambda": args.reg_lambda,
        "alpha": args.reg_alpha,
        "scale_pos_weight": args.scale_pos_weight,
        "max_delta_step": args.max_delta_step,
        "booster": args.booster,
        "tree_method": args.tree_method,
        "grow_policy": args.grow_policy,
        "eval_metric": args.eval_metric,
        "early_stopping_rounds": args.early_stopping_rounds,
        "device": args.device
    }

    # Convert string arguments to integers
    args.hyperparameter_optimization_attempts = int(args.hyperparameter_optimization_attempts)
    args.dask_workers = int(args.dask_workers)
    args.dask_threads_per_worker = int(args.dask_threads_per_worker)


    with LocalCluster(n_workers=args.dask_workers, threads_per_worker=args.dask_threads_per_worker) as cluster:
        with cluster.get_client() as client:
            df = load_data(args.csv_path)
            df = df.reset_index(drop=True).persist()
            df['unique_id'] = df.index
            df = df.persist()


            if args.model_path:
                model, feature_names = load_model_and_feature_names(args.model_path)
                if set(params_columns) != set(feature_names):
                    raise ValueError(f"Feature names mismatch: expected {feature_names} but got {params_columns}")
                df_preprocessed, encoders, scalers = preprocess_data(df, params_columns, is_test_data=True)
                predictions = invoke_model(client, model, df_preprocessed, params_columns, args.prediction_column)
                df[args.prediction_column] = predictions
                df = df.drop(columns=['unique_id'])

                df_computed = df.compute()
                # Extract headers and rows for output
                headers = list(df_computed.columns)
                rows = df_computed.astype(str).values.tolist()

                # Prepare output
                output = {
                    "headers": headers,
                    "rows": rows,
                }

            else:

                df_preprocessed, encoders, scalers = preprocess_data(df, params_columns, args.target_column)
                train_df, validate_df, test_df = split_data(df_preprocessed)
                train_df = train_df.reset_index(drop=True).persist()
                validate_df = validate_df.reset_index(drop=True).persist()
                test_df = test_df.reset_index(drop=True).persist()

                is_classification = "binary" in args.objective or "multi" in args.objective
                if args.hyperparameter_optimization_attempts == 0:
                    model = train_xgb_model(client, train_df, validate_df, params_columns, args.target_column, base_xgb_config, is_classification)
                    save_model(model, params_columns, args.model_dir, args.model_name)
                    # Evaluate the model
                    predictions, unique_ids, report = evaluate_model(client, model, test_df, params_columns, args.target_column, is_classification)
                else:
                    best_score = float('inf') if "reg:" in args.objective else 0
                    best_config = base_xgb_config
                    trials = []

                    for attempt in range(args.hyperparameter_optimization_attempts):
                        if attempt == 0:
                            xgb_config = base_xgb_config
                        else:
                            xgb_config = randomize_xgb_config(base_xgb_config)
                        
                        model = train_xgb_model(client, train_df, validate_df, params_columns, args.target_column, xgb_config, is_classification)
                        predictions, unique_ids, report = evaluate_model(client, model, test_df, params_columns, args.target_column, is_classification)

                        if "reg:" in args.objective:
                            trials.append({
                                "params": xgb_config, 
                                "mean_squared_error": report['mean_squared_error'], 
                                "r2_score": report['r2_score'],
                                "deviation_from_base": calculate_deviation(base_xgb_config, xgb_config)
                            })
                            score = report['mean_squared_error']
                        else:
                            trials.append({
                                "params": xgb_config, 
                                "accuracy": report['accuracy'],
                                "macro_avg": report['macro avg'],
                                "weighted_avg": report['weighted avg'],
                                "deviation_from_base": calculate_deviation(base_xgb_config, xgb_config)
                            })
                            score = report['accuracy']

                        if ("reg:" in args.objective and score < best_score) or ("reg:" not in args.objective and score > best_score):
                            best_score = score
                            best_config = xgb_config

                    xgb_config = best_config
                    model = train_xgb_model(client, train_df, validate_df, params_columns, args.target_column, xgb_config, is_classification)
                    save_model(model, params_columns, args.model_dir, args.model_name)
                    predictions, unique_ids, report = evaluate_model(client, model, test_df, params_columns, args.target_column, is_classification)

                    if "reg:" in args.objective:
                        sorted_trials = sorted(trials, key=lambda x: x['mean_squared_error'], reverse="reg:" not in args.objective)
                    else:
                        sorted_trials = sorted(trials, key=lambda x: x['accuracy'], reverse="reg:" not in args.objective)

                    num_attempts = len(sorted_trials)
                    report["best_params"] = [{f"rank_{i+1}": sorted_trials[i]} for i in range(num_attempts)]
                    report["best_params"] = report["best_params"][:args.hyperparameter_optimization_result_display_limit]
                    report["best_params"] = report["best_params"][::-1]

                    if "reg:" in args.objective:
                        report["best_mean_squared_error"] = report["mean_squared_error"]
                        report["best_r2_score"] = report["r2_score"]

                        # Remove the old keys
                        del report["mean_squared_error"]
                        del report["r2_score"]
                    else:
                        report["best_rank_stats"] = {
                                "best_accuracy": report["accuracy"],
                                "best_macro_avg": report["macro avg"],
                                "best_weighted_avg": report["weighted avg"],
                            }
                        del report["accuracy"]
                        del report["macro avg"]
                        del report["weighted avg"]
                        del report["0"]
                        del report["1"]



                    predictions_df = dd.from_dask_array(predictions, columns=[prediction_column])
                    predictions_df['unique_id'] = unique_ids
                    predictions_df['unique_id'] = predictions_df['unique_id'].astype('int64')
                    final_df = df.merge(predictions_df, on='unique_id', how='left')
                    final_df = final_df.drop(columns=['unique_id'])
                    final_df[prediction_column] = final_df[prediction_column].round(2)
                    final_df[prediction_column] = final_df[prediction_column].astype('str')
                    final_df[prediction_column] = final_df[prediction_column].replace('nan', '')
                    results = final_df.compute().values.tolist()
                    headers = final_df.columns.tolist()
                    output = {"headers": headers, "rows": [[str(item) for item in row] for row in results], "report": report}



                # STEP 2: Once the predictions are in along with their corresponding unique ids, you can add it back to the original df
                # Create a new Dask DataFrame for predictions with unique_id
                predictions_df = dd.from_dask_array(predictions, columns=[prediction_column])
                predictions_df['unique_id'] = unique_ids
                predictions_df['unique_id'] = predictions_df['unique_id'].astype('int64')

                # Merge predictions into the original test DataFrame
                final_df = df.merge(predictions_df, on='unique_id', how='left')
                final_df = final_df.drop(columns=['unique_id'])
                final_df[prediction_column] = final_df[prediction_column].round(2)
                final_df[prediction_column] = final_df[prediction_column].astype('str')
                final_df[prediction_column] = final_df[prediction_column].replace('nan', '')


                # Compute results
                results = final_df.compute().values.tolist()
                headers = final_df.columns.tolist()

                output = {
                    "headers": headers,
                    "rows": [[str(item) for item in row] for row in results],
                    "report": report
                }

            #print("IGNORE_POINT")
            #print(json.dumps(output, indent=4))
            json_output = json.dumps(output, indent=4)
            filename = f"rgwml_{args.uid}.json"
            with open(filename, 'wb') as f:
                # Resize the file to the size of the JSON output
                f.write(b' ' * len(json_output))

            with open(filename, 'r+b') as f:
                mm = mmap.mmap(f.fileno(), 0)
                mm.write(json_output.encode('utf-8'))
                mm.close()



if __name__ == "__main__":
    main()

