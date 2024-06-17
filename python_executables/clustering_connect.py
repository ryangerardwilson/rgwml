import pandas as pd
import numpy as np
import argparse
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering, MeanShift, SpectralClustering, Birch
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
import json
import mmap

def perform_kmeans(X, n_clusters):
    kmeans = KMeans(n_clusters=n_clusters, random_state=0)
    return kmeans.fit_predict(X)

def perform_dbscan(X, eps, min_samples):
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    return dbscan.fit_predict(X)

def perform_agglomerative(X, n_clusters):
    agglomerative = AgglomerativeClustering(n_clusters=n_clusters)
    return agglomerative.fit_predict(X)

def perform_mean_shift(X):
    mean_shift = MeanShift()
    return mean_shift.fit_predict(X)

def perform_gmm(X, n_clusters):
    gmm = GaussianMixture(n_components=n_clusters, random_state=0)
    gmm.fit(X)
    return gmm.predict(X)

def perform_spectral(X, n_clusters):
    spectral = SpectralClustering(n_clusters=n_clusters, random_state=0)
    return spectral.fit_predict(X)

def perform_birch(X, n_clusters):
    birch = Birch(n_clusters=n_clusters)
    return birch.fit_predict(X)

def find_optimal_clusters_elbow(X):
    wcss = []
    for i in range(1, 11):
        kmeans = KMeans(n_clusters=i, random_state=0)
        kmeans.fit(X)
        wcss.append(kmeans.inertia_)

    # Automatically determine the "elbow" point
    deltas = np.diff(wcss)
    second_deltas = np.diff(deltas)
    optimal_clusters = np.argmax(second_deltas) + 2  # +2 to offset the second difference
    return optimal_clusters

def find_optimal_clusters_silhouette(X):
    silhouette_scores = []
    for i in range(2, min(11, len(X))):
        kmeans = KMeans(n_clusters=i, random_state=0)
        labels = kmeans.fit_predict(X)
        if len(np.unique(labels)) > 1:
            silhouette_scores.append(silhouette_score(X, labels))
        else:
            silhouette_scores.append(-1)  # Invalid score if only one cluster

    # Automatically determine the optimal number of clusters
    optimal_clusters = np.argmax(silhouette_scores) + 2  # +2 because range starts from 2
    return optimal_clusters

def parse_optimal_clustering_method(optimal_clustering_method):
    if optimal_clustering_method.startswith('FIXED:'):
        return int(optimal_clustering_method.split(':')[1])
    elif optimal_clustering_method in ['ELBOW', 'SILHOUETTE']:
        return optimal_clustering_method
    else:
        raise ValueError("Unsupported optimal clustering method. Choose from 'FIXED:n', 'ELBOW' or 'SILHOUETTE'.")

def main(uid, csv_path, features, operation, cluster_column_name, optimal_n_cluster_finding_method, dbscan_eps, dbscan_min_samples):
    # Load the CSV file
    data = pd.read_csv(csv_path)

    # Select the features for clustering
    feature_list = [feature.strip() for feature in features.split(',')]
    X = data[feature_list]

    # Determine the optimal number of clusters if required
    n_clusters = None
    if optimal_n_cluster_finding_method and operation != 'DBSCAN':
        optimal_method = parse_optimal_clustering_method(optimal_n_cluster_finding_method)
        if isinstance(optimal_method, int):
            n_clusters = optimal_method
        elif optimal_method == 'ELBOW':
            n_clusters = find_optimal_clusters_elbow(X)
        elif optimal_method == 'SILHOUETTE':
            n_clusters = find_optimal_clusters_silhouette(X)
    elif operation != 'DBSCAN' and not optimal_clustering_method:
        raise ValueError("Optimal clustering method must be specified for algorithms that require n_clusters")

    # Perform the chosen clustering operation
    if operation == 'KMEANS':
        data[cluster_column_name] = perform_kmeans(X, n_clusters)
    elif operation == 'DBSCAN':
        data[cluster_column_name] = perform_dbscan(X, dbscan_eps, dbscan_min_samples)
    elif operation == 'AGGLOMERATIVE':
        data[cluster_column_name] = perform_agglomerative(X, n_clusters)
    elif operation == 'MEAN_SHIFT':
        data[cluster_column_name] = perform_mean_shift(X)
    elif operation == 'GMM':
        data[cluster_column_name] = perform_gmm(X, n_clusters)
    elif operation == 'SPECTRAL':
        data[cluster_column_name] = perform_spectral(X, n_clusters)
    elif operation == 'BIRCH':
        data[cluster_column_name] = perform_birch(X, n_clusters)
    else:
        raise ValueError("Operation must be one of 'KMEANS', 'DBSCAN', 'AGGLOMERATIVE', 'MEAN_SHIFT', 'GMM', 'SPECTRAL', or 'BIRCH'")

    # Print the resulting clusters
    #print(data)

    # Prepare the final output
    headers = list(data.columns)
    rows = data.values.tolist()
    if operation in ['DBSCAN', 'MEAN_SHIFT']:
        report = f'Clustering performed using {operation}'
    else:
        report = f'Clustering performed using {operation} with {n_clusters} clusters'

    output = {
        "headers": headers,
        "rows": [[str(item) for item in row] for row in rows],
        "report": report
    }

    json_output = json.dumps(output, indent=4)
    filename = f"rgwml_{uid}.json"
    with open(filename, 'wb') as f:
        # Resize the file to the size of the JSON output
        f.write(b' ' * len(json_output))

    with open(filename, 'r+b') as f:
        mm = mmap.mmap(f.fileno(), 0)
        mm.write(json_output.encode('utf-8'))
        mm.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster customers using various clustering algorithms.')
    
    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--csv_path', type=str, required=True, help='Path to the CSV file.')
    parser.add_argument('--features', type=str, required=True, help='Comma-separated list of features to use for clustering.')
    parser.add_argument('--cluster_column_name', type=str, required=True, help='Name of the clustering column.')
    parser.add_argument('--operation', type=str, required=True, choices=['KMEANS', 'DBSCAN', 'AGGLOMERATIVE', 'MEAN_SHIFT', 'GMM', 'SPECTRAL', 'BIRCH'], help='Clustering operation to perform.')
    # optimal_n_cluster_finding_method is not relevant for MEAN_SHIFT and DBSCAN
    parser.add_argument('--optimal_n_cluster_finding_method', type=str, help='Method to find the optimal number of clusters. Options: FIXED:n, ELBOW, SILHOUETTE. This value is not relevant for MEAN_SHIFT and DBSCAN operations')
    # dbscan_eps and dbscan_min_samples are only relevant for DBSCAN
    parser.add_argument('--dbscan_eps', type=float, default=0.5, help='The maximum distance between two samples for one to be considered as in the neighborhood of the other (used in DBSCAN). This value is only relevant for DBSCAN operations')
    parser.add_argument('--dbscan_min_samples', type=int, default=5, help='The number of samples (or total weight) in a neighborhood for a point to be considered as a core point (used in DBSCAN). This value is only relevant for DBSCAN operations')

    args = parser.parse_args()

    main(args.uid, args.csv_path, args.features, args.operation, args.cluster_column_name, args.optimal_n_cluster_finding_method, args.dbscan_eps, args.dbscan_min_samples)

