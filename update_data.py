import requests
import pandas as pd
import os
import json
from datetime import date

# 1. Get API Key from environment variable (best practice for GitHub Actions)
# Defaults to the provided key for local testing
api_key = os.environ.get('BLS_API_KEY', '7a6307dc6b6c46ac92183e79ed6a2837')

# 2. Define Series IDs
series_ids = {
    'CES0000000001': 'Total Nonfarm Payrolls',
    'LNS14000000': 'Unemployment Rate',
    'LNS11300000': 'Labor Force Participation Rate',
    'CES0500000003': 'Average Hourly Earnings',
    'LNS12300000': 'Employment-Population Ratio',
    'CES0600000007': 'Average Weekly Hours (Private)'
}

# 3. Set Date Range (Last 9 years to present)
start_year = str(date.today().year - 9)
end_year = str(date.today().year)

headers = {
    'Content-type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
}

# 4. Prepare Request Payload
# catalog, calculations, annualaverage set to False to reduce API load
data = json.dumps({
    "seriesid": list(series_ids.keys()),
    "startyear": start_year,
    "endyear": end_year,
    "registrationkey": api_key,
    "catalog": False,
    "calculations": False,
    "annualaverage": False
})

print("Requesting data from BLS API...")

# Send Request
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

# Check Status Code
if p.status_code != 200:
    print(f"Error fetching data: {p.text}")
    exit(1)

json_data = p.json()

# Check if 'Results' exists in response
if 'Results' not in json_data:
    print(f"API returned error: {json_data.get('message', 'Unknown Error')}")
    exit(1)

all_data = []

print("Processing data...")

# 5. Parse JSON Data
for series in json_data['Results']['series']:
    series_id = series['seriesID']
    series_name = series_ids.get(series_id, series_id)

    for item in series['data']:
        # [SAFETY CHECK] Ignore 'M13' (Annual Averages) to prevent date parsing errors
        if 'M13' in item['period']:
            continue
            
        # [SAFETY CHECK] Handle non-numeric values
        try:
            value = float(item['value'])
        except ValueError:
            continue

        all_data.append({
            'Series': series_name,
            'Year': item['year'],
            'Period': item['period'],
            'Value': value,
            # Convert 'M01' -> '2023-01-01'
            'Date': f"{item['year']}-{item['period'][1:]}-01"
        })

if not all_data:
    print("No data returned from API.")
    exit(0)

# 6. Create DataFrame
df = pd.DataFrame(all_data)
df['Date'] = pd.to_datetime(df['Date'])

# Pivot Table: Date as index, Series as columns
df_pivot = df.pivot_table(index='Date', columns='Series', values='Value')
df_pivot = df_pivot.sort_index(ascending=False)

# 7. Save to CSV
if not os.path.exists('data'):
    os.makedirs('data')

output_path = 'data/bls_data.csv'
df_pivot.to_csv(output_path)

print(f"SUCCESS: Data saved to {output_path}")
print(df_pivot.head())
