# app.py (FINAL v5 - Reads Final Professional Format)
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

# --- Configuration & Connection (no changes) ---
GOOGLE_SHEET_NAME = "FPL-Data-Pep"

@st.cache_resource(ttl=600)
def connect_to_gsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(".streamlit/google_credentials.json", scopes=scopes)
    except FileNotFoundError:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME)

@st.cache_data(ttl=600)
def read_from_gsheet(worksheet_name):
    spreadsheet = connect_to_gsheet()
    worksheet = spreadsheet.worksheet(worksheet_name)
    return pd.DataFrame(worksheet.get_all_records())

# --- Main App Logic ---
st.set_page_config(page_title="FPL Mini-League Dashboard", layout="wide")
st.title("🏆 FPL Mini-League Awards Dashboard")

try:
    metadata = read_from_gsheet("metadata").iloc[0]
    last_gw = metadata['last_finished_gw']
    last_updated = metadata['last_updated_utc']
    
    st.markdown(f"**Awards are calculated based on all data up to Gameweek {last_gw}.**")
    st.caption(f"Last updated: {last_updated} UTC")

    award_names = ["golden_boot", "playmaker", "golden_glove", "best_gk", "best_def", "best_mid", "best_fwd", "best_vc"]
    leaderboards_wide = {name: read_from_gsheet(name) for name in award_names}

    st.header(f"🏆 Season Award Leaders (as of GW{last_gw})")

    # --- Metric Card Grid ---
    col1, col2, col3 = st.columns(3)
    def get_current_leader(award_name):
        df = leaderboards_wide[award_name]
        return df.iloc[0]

    gb_winner, pm_winner, gg_winner = get_current_leader("golden_boot"), get_current_leader("playmaker"), get_current_leader("golden_glove")
    
    col1.metric("🥇 Golden Boot", gb_winner['Manager'], f"{gb_winner.iloc[3]} Goals")
    col2.metric("🅰️ Playmaker", pm_winner['Manager'], f"{pm_winner.iloc[3]} Assists")
    col3.metric("🧤 Golden Glove", gg_winner['Manager'], f"{gg_winner.iloc[3]} Clean Sheets")
    st.divider()

    colA, colB, colC, colD = st.columns(4)
    gk_winner, def_winner = get_current_leader("best_gk"), get_current_leader("best_def")
    mid_winner, fwd_winner = get_current_leader("best_mid"), get_current_leader("best_fwd")
    vc_winner = get_current_leader("best_vc")

    colA.metric("👑 Best Goalkeeper", gk_winner['Manager'], f"{gk_winner.iloc[3]} Pts")
    colB.metric("🛡️ Best Defenders", def_winner['Manager'], f"{def_winner.iloc[3]} Pts")
    colC.metric("🎩 Best Midfielders", mid_winner['Manager'], f"{mid_winner.iloc[3]} Pts")
    colD.metric("💥 Best Forwards", fwd_winner['Manager'], f"{fwd_winner.iloc[3]} Pts")
    st.metric("🥈 Best Vice-Captain", vc_winner['Manager'], f"{vc_winner.iloc[3]} Points")

    # --- Detailed Historical Leaderboards in Expanders ---
    st.divider()
    st.subheader("Historical Award Races")
    
    award_titles = { "golden_boot": "🥇 Golden Boot Race", "playmaker": "🅰️ Playmaker Race", "golden_glove": "🧤 Golden Glove Race", "best_gk": "👑 Best Goalkeeper Race", "best_def": "🛡️ Best Defenders Race", "best_mid": "🎩 Best Midfielders Race", "best_fwd": "💥 Best Forwards Race", "best_vc": "🥈 Best Vice-Captain Race" }

    for name, title in award_titles.items():
        with st.expander(title):
            wide_df = leaderboards_wide[name]
            
            gameweek_cols = [f"GW{i}" for i in range(1, last_gw + 1)]
            
            long_df = wide_df.melt(id_vars=['Manager'], value_vars=gameweek_cols, var_name='gameweek', value_name='score')
            long_df['gameweek'] = long_df['gameweek'].str.replace('GW', '').astype(int)

            chart_df = long_df.pivot(index='gameweek', columns='Manager', values='score')
            st.line_chart(chart_df)
            
            st.markdown("**Full Standings**")
            st.dataframe(wide_df.set_index('Standings'), use_container_width=True) # RANKING FIX

except (SpreadsheetNotFound, WorksheetNotFound) as e:
    st.error(f"A required worksheet was not found. The data may not have been generated yet.")
    st.info("Please run the `data_pipeline.py` script and ensure it completes successfully before running the app.")
except Exception as e:
    st.error("An unexpected error occurred. Please check the logs.")
    st.exception(e)