import os
import json
import requests
import pandas as pd
from datetime import date

BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

SERIES_IDS = {
    "CES0000000001": "Total Nonfarm Payrolls",
    "LNS14000000": "Unemployment Rate",
    "LNS11300000": "Labor Force Participation Rate",
    "CES0500000003": "Average Hourly Earnings",
    "LNS12300000": "Employment-Population Ratio",
    "CES0600000007": "Average Weekly Hours (Private)",
}

DATA_DIR = "data"
OUTPUT_PATH = os.path.join(DATA_DIR, "bls_data.csv")


def get_api_key() -> str:
    """Read BLS API key from env var (recommended for GitHub Actions)."""
    key = os.environ.get("BLS_API_KEY")
    if not key:
        raise RuntimeError(
            "BLS_API_KEY is not set. Set it as an environment variable "
            "(or GitHub Actions Secret) before running."
        )
    return key


def get_request_years(existing_csv: str) -> tuple[str, str]:
    """
    If we already have a CSV, request a small buffered range (last year -> current year)
    so we can append only new months.
    If not, request last 2 years (meets the '>= 1 year' requirement).
    """
    this_year = date.today().year

    if os.path.exists(existing_csv):
        existing = pd.read_csv(existing_csv, parse_dates=["Date"])
        if not existing.empty:
            last_date = existing["Date"].max()
            start_year = str(max(last_date.year - 1, this_year - 5))  # small buffer
            end_year = str(this_year)
            return start_year, end_year

    return str(this_year - 2), str(this_year)


def fetch_bls_json(api_key: str, start_year: str, end_year: str) -> dict:
    headers = {
        "Content-type": "application/json",
        "User-Agent": "Econ8320-BLS-Dashboard/1.0",
    }

    payload = {
        "seriesid": list(SERIES_IDS.keys()),
        "startyear": start_year,
        "endyear": end_year,
        "registrationkey": api_key,
        "catalog": True,
        "calculations": True,
        "annualaverage": False,
    }

    print(f"Requesting BLS data ({start_year}-{end_year})...")
    resp = requests.post(BLS_URL, data=json.dumps(payload), headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_to_wide_df(json_data: dict) -> pd.DataFrame:
    """
    Parse API response into a wide monthly DataFrame indexed by Date.
    Drops M13 (annual average) and keeps only M01..M12.
    """
    if "Results" not in json_data or "series" not in json_data["Results"]:
        raise KeyError(f"Unexpected API response format: {json_data}")

    rows = []
    for series in json_data["Results"]["series"]:
        sid = series.get("seriesID")
        sname = SERIES_IDS.get(sid, sid)

        for item in series.get("data", []):
            period = item.get("period", "")
            if not period.startswith("M") or period == "M13":
                continue  # keep only monthly data

            year = int(item["year"])
            month = int(period[1:])  # "M01" -> 1

            rows.append(
                {
                    "Date": pd.Timestamp(year=year, month=month, day=1),
                    "Series": sname,
                    "Value": float(item["value"]),
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    wide = df.pivot_table(index="Date", columns="Series", values="Value", aggfunc="mean")
    wide = wide.sort_index()  # ascending dates
    return wide


def append_and_save(wide_new: pd.DataFrame, output_path: str) -> None:
    """
    Append new months to existing CSV (do NOT re-download on dashboard load).
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        existing = pd.read_csv(output_path, parse_dates=["Date"]).set_index("Date")
        combined = pd.concat([existing, wide_new], axis=0)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    else:
        combined = wide_new.copy()

    combined.reset_index().to_csv(output_path, index=False)
    print(f"Saved updated data to {output_path} (rows: {len(combined)})")


def main():
    api_key = get_api_key()
    start_year, end_year = get_request_years(OUTPUT_PATH)

    json_data = fetch_bls_json(api_key, start_year, end_year)
    wide_new = parse_to_wide_df(json_data)

    if wide_new.empty:
        print("No monthly data returned (after filtering). Nothing to update.")
        return

    append_and_save(wide_new, OUTPUT_PATH)


if __name__ == "__main__":
    main()
