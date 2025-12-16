import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone

# =========================
# CONFIGURATION
# =========================
BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
START_YEAR = 2006
END_YEAR = datetime.now(timezone.utc).year

SERIES_IDS = {
    "CES0000000001": "Total Nonfarm Payrolls",
    "LNS14000000": "Unemployment Rate",
    "LNS11300000": "Labor Force Participation Rate",
    "CES0500000003": "Average Hourly Earnings",
    "LNS12300000": "Employment-Population Ratio",
    "CES0600000007": "Average Weekly Hours (Private)",
}

HEADERS = {
    "Content-type": "application/json",
    "User-Agent": "Mozilla/5.0"
}

DATA_DIR = "data"
OUTPUT_PATH = os.path.join(DATA_DIR, "bls_data.csv")

# =========================
# FUNCTIONS
# =========================
def fetch_bls_chunk(start_year: int, end_year: int, api_key: str) -> pd.DataFrame:
    payload = {
        "seriesid": list(SERIES_IDS.keys()),
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key,
        "catalog": False,
        "calculations": False,
        "annualaverage": False,
    }

    response = requests.post(BLS_URL, data=json.dumps(payload), headers=HEADERS)
    response.raise_for_status()
    js = response.json()

    if js.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(js.get("message"))

    rows = []
    for series in js["Results"]["series"]:
        name = SERIES_IDS.get(series["seriesID"], series["seriesID"])
        for item in series["data"]:
            if item["period"] == "M13":
                continue
            try:
                value = float(item["value"])
            except ValueError:
                continue
            month = item["period"][1:].zfill(2)
            rows.append({
                "Date": f"{item['year']}-{month}-01",
                "Series": name,
                "Value": value
            })

    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"])
    return df


def pivot_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pivot_table(index="Date", columns="Series", values="Value")
          .sort_index()
    )

# =========================
# MAIN LOGIC
# =========================
def main():
    api_key = os.environ.get("BLS_API_KEY")
    if not api_key:
        raise RuntimeError("BLS_API_KEY environment variable not set.")

    if os.path.exists(OUTPUT_PATH):
        existing = pd.read_csv(OUTPUT_PATH, parse_dates=["Date"], index_col="Date")
        last_date = existing.index.max()
        start_year = last_date.year
    else:
        existing = None
        last_date = None
        start_year = START_YEAR

    chunks = []
    if existing is None and start_year <= 2014:
        chunks = [(2006, 2014), (2015, END_YEAR)]
    else:
        chunks = [(start_year, END_YEAR)]

    parts = []
    for sy, ey in chunks:
        parts.append(fetch_bls_chunk(sy, ey, api_key))

    df_long = pd.concat(parts, ignore_index=True)
    df_new = pivot_data(df_long)

    if existing is not None:
        df_new = df_new[df_new.index > last_date]
        if df_new.empty:
            print("No new data to append.")
            return
        df_final = pd.concat([existing, df_new]).sort_index()
    else:
        df_final = df_new

    os.makedirs(DATA_DIR, exist_ok=True)
    df_final.to_csv(OUTPUT_PATH)

    print("SUCCESS")
    print("Date range:", df_final.index.min(), "to", df_final.index.max())


if __name__ == "__main__":
    main()
