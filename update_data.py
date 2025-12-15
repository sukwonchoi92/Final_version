import requests
import pandas as pd
import os
import json
from datetime import date

api_key = os.environ.get('BLS_API_KEY', '7a6307dc6b6c46ac92183e79ed6a2837') 

series_ids = {
    'CES0000000001': 'Total Nonfarm Payrolls',
    'LNS14000000': 'Unemployment Rate',
    'LNS11300000': 'Labor Force Participation Rate',
    'CES0500000003': 'Average Hourly Earnings',
    'LNS12300000': 'Employment-Population Ratio',
    'CES0600000007': 'Average Weekly Hours (Private)'
}

start_year = str(date.today().year - 10)
end_year = str(date.today().year)

headers = {'Content-type': 'application/json'}
data = json.dumps({
    "seriesid": list(series_ids.keys()),
    "startyear": start_year,
    "endyear": end_year,
    "registrationkey": api_key,
    "catalog": True,
    "calculations": True,
    "annualaverage": False
})

print("Requesting data from BLS API...")
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', 
                  data=data, 
                  headers=headers)

if p.status_code != 200:
    print(f"Error fetching data: {p.text}")
    exit(1)

json_data = p.json()

all_data = []
try:
    for series in json_data['Results']['series']:
        series_id = series['seriesID']
        series_name = series_ids.get(series_id, series_id)
        
        for item in series['data']:
            all_data.append({
                'Series': series_name,
                'Year': item['year'],
                'Period': item['period'],
                'Value': float(item['value']),
                'Date': pd.to_datetime(f"{item['year']}-{item['period'][1:]}-01")
            })
except KeyError:
    print("Error parsing JSON. 'Results' key not found or data is empty.")
    print(f"API Response: {json_data}")
    exit(1)

if not all_data:
    print("No data returned from API.")
    exit(0)

print("Data processing...")
df = pd.DataFrame(all_data)

df_pivot = df.pivot_table(index='Date', columns='Series', values='Value')
df_pivot = df_pivot.sort_index(ascending=False)

if not os.path.exists('data'):
    os.makedirs('data')

output_path = 'data/bls_data.csv'
df_pivot.to_csv(output_path)

print(f"Data successfully saved to {output_path}")
