use reqwest::header::HeaderMap;
use reqwest::{Client, Error, Method, Response};
use serde::Serialize;
use serde_json::{self, json, Value};
use std::collections::HashMap;
use std::error::Error as StdError;
use tokio;

pub enum HeaderOption {
    None,
    Custom(HeaderMap),
}

pub async fn async_api_call<T: Serialize>(
    method: Method,
    url: &str,
    header_option: HeaderOption,
    payload: Option<T>,
) -> Result<String, Error> {
    fn header_option_to_header_map(header_option: HeaderOption) -> Option<HeaderMap> {
        match header_option {
            HeaderOption::None => None,
            HeaderOption::Custom(headers) => Some(headers),
        }
    }

    let client = Client::new();
    // let mut request_builder = client.request(method, url);
    let mut request_builder = client.request(method.clone(), url);

    // Convert HeaderOption to HeaderMap if needed
    if let Some(headers_map) = header_option_to_header_map(header_option) {
        request_builder = request_builder.headers(headers_map);
    }

    // Check if method is POST and payload is provided
    if Method::POST == method && payload.is_some() {
        request_builder = request_builder.json(&payload.unwrap());
    }

    // Send the request and await the response
    let response: Response = request_builder.send().await?;

    if response.status().is_success() {
        response.text().await.map_err(From::from)
    } else {
        Err(response.error_for_status().unwrap_err())
    }
}

type DataFrame = Vec<HashMap<String, Value>>;

fn convert_json_string_to_dataframe(json_data: &str) -> Result<DataFrame, Box<dyn StdError>> {
    let data: Value = serde_json::from_str(json_data)?;

    let mut data_frame = DataFrame::new();

    if let Value::Array(objects) = data {
        for object in objects {
            if let Value::Object(map) = object {
                let mut row = HashMap::new();
                for (key, value) in map {
                    row.insert(key, value);
                }
                data_frame.push(row);
            }
        }
    }

    Ok(data_frame)
}

#[tokio::main]
async fn main() {
    let result = async_api_call(
        Method::POST,
        "https://labs-api.wiom.in/query",
        HeaderOption::None,
        Some(json!({ "query": "
	select a.*, b.lat, b.lng, /*b.address as t_address, b.locality, b.city, b.pincode, b.state,*/ install_latitude, install_longitude
	from
	(
		select a.id as lead_id, added_time as lead_time, mobile, name, address, google_address_id, 
		json_value(address, '$.home') as house_floor,
		json_value(address, '$.street') as street,
		json_value(address, '$.address') as remaining_address,
		json_value(address, '$.pincode') as pincode,
		json_value(address, '$.city') as city,
		case when google_address_id is null then 1 else 0 end as null_address,
		status,
		case when a.id = b.lead_id then 1 else 0 end as lco_interested,
		case when status in (-5) then 1 else 0 end as lead_rejected_lco,
		case when status in (-6) then 1 else 0 end as lead_rejected_wiom_team,
		case when status = 20 then 1 else 0 end as lco_allotted,
		case when status >= 30 then 1 else 0 end as lead_accepted_lco,
		case when first_recharge_time is not null then 1 else 0 end as installed,
		case when status in (-2,2) then 1 else 0 end as waiting_list,
		case when status in (0,1) then 1 else 0 end as not_verified_customer,
		-- what are -10 and -6 status
		case when status in (-5) then 'A. Lead rejected LCO'
		when status in (-6) then 'A. Lead rejected InternalTeam'
		when status in (-10) then 'A.2. Lead cancelled by customer'
		when status in (0,1) then 'C. Lead not verified - Tell lead to download app and complete verification'
		when status in (30) then 'D. Lead accepted by LCO - Tell customer that Installation will start soon and check status in App - Take action: Play message in loop and disconnect call'
		when status in (40)  then 'E. Installation has started - Tell customer that Installation Work started and expect closure soon'
		when status in (41) then 'E. Installation has started - Tell customer that Installation work is ongoing'
		when status in (41) and wifi_status in (60,80,100) then 'A. Installed'
		when first_recharge_time is not null and status > 0 and wifi_status >=60 then 'A. Installed'
		when status in (-2,0,2) then 'F. Lead is in Wait list - Tell customer to keep checking the status of the lead on App - Take action: Play message in loop and disconnect call'
		when status = 20 then 'G. LCO has been allotted - Tell customer that Installation will start soon and check status in App - Take action: Play message in loop and disconnect call'
		else 'N/A'
		end as final_lead_status, nasid, agent_account_id,
		b.shard_id, b.partner_id, c.partner_name, c.zone, c.zone_map
		from t_wiom_lead a left join
		(
			select lead_id, partner_id, shard_id
			from
			(
				select lead_id, json_value(dbo.IDPARSER(partner_id), '$.ShardID') as shard_id, json_value(dbo.IDPARSER(partner_id), '$.LocalID') as partner_id,
				row_number() over (partition by lead_id order by audit_timestamp desc) as row_cnt
				from lead_allocation_audit where  status not in ('Declined', 'NOTIF_SENT')
				and cast(audit_timestamp as date) >= cast(dateadd(dd,-60,getdatE() ) as date)
			) a
			group by lead_id, partner_id, shard_id
		) b on a.id = b.lead_id
		left join
		(
			select a.*, 
			case when zone like '%Delisted%' then 'Delisted'
			when zone like '%lead_allocation_blocked%' then 'Lead Allocation Blocked'
			else 'Active' end as partner_status,
			case when zone like '%Centre Mumbai%' then 'Centre Mumbai'
			when zone like '%North West Mumbai%' then 'North West Mumbai'
			when zone like '%East Delhi%' or zone like '%Delisted_East%' then 'East Delhi'
			when zone like '%Centre Delhi%' or zone like '%Delisted_Centre%' then 'Centre Delhi'
			when zone like '%North Delhi%' or zone like '%Delisted_North%' then 'North Delhi'
			when zone like '%South Delhi%' or zone like '%Delisted_South%' then 'South Delhi'
			when zone like '%West Delhi%' or zone like '%Delisted_West%' then 'West Delhi'
			when zone like '%Ghaziabad%' then 'Ghaziabad'
			when zone like '%Meerut%' then 'Meerut'
			when zone is NULL then 'Invalid Zone'
			when zone in ('ISP LCO', 'NULL') or zone like '%office%' or zone like '%Anugrah%'or zone like '%PDO%' then 'Invalid Zone'
			else 'Check Zone' end as zone_map
			from
			(
				select id, name as partner_name, logical_group as zone, 0 as shard_id from t_account
				UNION
				select id, name as partner_name, logical_group as zone, 1 as shard_id from t_account
			) a
		) c on b.partner_id = c.id and b.shard_id = c.shard_id
		where cast(added_time as date) >= cast(dateadd(dd,-60,getdatE() ) as date)
	)a left join 
	(
		select *, dbo.IDMAKER(0,7,id) as long_gid,0 as shard_id from i2e1.dbo.t_address
		UNION
		select *,dbo.IDMAKER(1,7,id) as long_gid,1 as shard_id  from shard_01.dbo.t_address
	) b on a.google_address_id = b.long_gid
	left join
	(
		select dbo.IDMAKER(0,0,router_nas_id) as router_nas_id,lat as install_latitude,lng as install_longitude from
		(
			select * from i2e1.dbo.t_store where added_time > '2023-01-01'
		)a
		left join
		(
			select *, dbo.IDMAKER(0,7,id) as long_gid,0 as shard_id from i2e1.dbo.t_address
		)b
		on a.google_address_id = b.id
		UNION
		select dbo.IDMAKER(1,0,router_nas_id) as router_nas_id,lat as install_latitude,lng as install_longitude from
		(
			select * from shard_01.dbo.t_store where added_time > '2023-01-01'
		)a
		left join
		(
			select *, dbo.IDMAKER(1,7,id) as long_gid,0 as shard_id from shard_01.dbo.t_address
		)b
		on a.google_address_id = b.id
	) c on a.nasid = c.router_nas_id
	where null_address = 0
	and zone_map not in ('Check Zone', 'Invalid zone') and zone_map is not null
	and a.shard_id = 0
            " })),
    ).await.unwrap();

    let df1 = convert_json_string_to_dataframe(&result).unwrap();

    dbg!(df1);

    // dbg!(result);
}
