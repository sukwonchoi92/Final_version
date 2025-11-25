import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")

@st.cache_data
def load_data(filepath):
    if os.path.exists(filepath):
        df = pd.read_csv(filepath, index_col='Date', parse_dates=True)
        return df
    else:
        return None

st.title("US Labor Market Key Indicators Dashboard")
st.markdown("This dashboard displays the latest data from the US Bureau of Labor Statistics (BLS).")

df = load_data('data/bls_data.csv')

if df is None:
    st.error("Error: 'data/bls_data.csv' file not found.")
    st.info("Please wait for GitHub Actions to run and generate the data file, "
            "or run 'update_data.py' locally.")
else:
    latest_date = df.index[0].strftime('%B %d, %Y')
    st.subheader(f"Latest Data (As of {latest_date})")

    latest_data = df.iloc[0]
    prev_data = df.iloc[1]

    col1, col2, col3 = st.columns(3)
    
    col1.metric(
        label="Unemployment Rate",
        value=f"{latest_data['Unemployment Rate']}%",
        delta=f"{latest_data['Unemployment Rate'] - prev_data['Unemployment Rate']:.2f}% P"
    )

    col2.metric(
        label="Total Nonfarm Payrolls",
        value=f"{latest_data['Total Nonfarm Payrolls'] / 1000:.1f} Million",
        delta=f"{latest_data['Total Nonfarm Payrolls'] - prev_data['Total Nonfarm Payrolls']:.0f} Thousands"
    )

    col3.metric(
        label="Average Hourly Earnings",
        value=f"${latest_data['Average Hourly Earnings']:.2f}",
        delta=f"${latest_data['Average Hourly Earnings'] - prev_data['Average Hourly Earnings']:.2f}"
    )

    st.markdown("---")

    st.subheader("Key Indicator Trends (All Time)")
    
    row1_col1, row1_col2 = st.columns(2)
    row2_col1, row2_col2 = st.columns(2)

    with row1_col1:
        st.write("Unemployment Rate")
        st.line_chart(df['Unemployment Rate'])

    with row1_col2:
        st.write("Total Nonfarm Payrolls")
        st.line_chart(df['Total Nonfarm Payrolls'])

    with row2_col1:
        st.write("Labor Force Participation Rate")
        st.line_chart(df['Labor Force Participation Rate'])

    with row2_col2:
        st.write("Employment-Population Ratio")
        st.line_chart(df['Employment-Population Ratio'])

    st.markdown("---")

    st.subheader("Full Data Table")
    st.dataframe(df)
