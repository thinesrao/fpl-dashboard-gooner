# app.py (FINAL v12 - Displays All 20 Awards Correctly)
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

# --- Configuration & Connection ---
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
    try:
        spreadsheet = connect_to_gsheet()
        worksheet = spreadsheet.worksheet(worksheet_name)
        return pd.DataFrame(worksheet.get_all_records())
    except WorksheetNotFound:
        return None

# --- Main App Logic ---
st.set_page_config(page_title="FPL Mini-League Dashboard", layout="wide")
st.title("🏆 FPL Mini-League Awards Dashboard")

try:
    metadata_df = read_from_gsheet("metadata")
    if metadata_df is None or metadata_df.empty:
        st.error("Metadata not found. Please run the data pipeline.")
    else:
        metadata = metadata_df.iloc[0]
        last_gw = metadata['last_finished_gw']
        st.markdown(f"**Awards calculated up to Gameweek {last_gw}.**")

        AWARD_CONFIG = {
            "golden_boot": ["🥇 Golden Boot", "Goals"], "playmaker": ["🅰️ Playmaker", "Assists"],
            "golden_glove": ["🧤 Golden Glove", "Clean Sheets"], "best_gk": ["👑 Best Goalkeeper", "Pts"],
            "best_def": ["🛡️ Best Defenders", "Pts"], "best_mid": ["🎩 Best Midfielders", "Pts"],
            "best_fwd": ["💥 Best Forwards", "Pts"], "best_vc": ["🥈 Best Vice-Captain", "Pts"],
            "transfer_king": ["🔀 Transfer King", "Pts"], "bench_king": ["🪑 Bench King", "Pts"],
            "dream_team": ["🌟 Dream Team King", "DT Score"], "shooting_stars": [" Shooting Stars", "Rank Rise"],
            "defensive_king": ["🧱 Defensive King", "Contribution"], "best_underdog": ["🥊 Best Underdog", "Wins"],
            "penalty_king": [" Penalty King", "Pts"], "steady_king": ["🧘 Steady King", "Pts/Transfer"], 
            "highest_gw_score": ["🚀 Highest GW Score", "Pts"], "freehit_king": ["🃏 Free Hit King", "Pts"], 
            "benchboost_king": ["📈 Bench Boost King", "Pts"], "triplecaptain_king": ["©️³ Triple Captain King", "Pts"]
        }
        
        award_sheets = {name: read_from_gsheet(name) for name in AWARD_CONFIG.keys()}

        st.header(f"🏆 Season Award Leaders (as of GW{last_gw})")
        
        all_award_names = list(AWARD_CONFIG.keys())
        cols = st.columns(4)
        col_idx = 0
        for name in all_award_names:
            df = award_sheets.get(name)
            if df is not None and not df.empty:
                leader = df.iloc[0]
                title, suffix = AWARD_CONFIG[name]
                score_col_name = df.columns[3]
                with cols[col_idx % 4]:
                    st.metric(title, leader['Manager'], f"{leader[score_col_name]} {suffix}")
                col_idx += 1
        
        st.divider()
        st.subheader("Detailed Award Standings")
        for name, (title, _) in AWARD_CONFIG.items():
            wide_df = award_sheets.get(name)
            if wide_df is not None and not wide_df.empty:
                 with st.expander(f"{title}"):
                    gameweek_cols = [col for col in wide_df.columns if col.startswith('GW')]
                    
                    if not gameweek_cols:
                        st.dataframe(wide_df.set_index('Standings'), use_container_width=True)
                    else:
                        long_df = wide_df.melt(id_vars=['Manager'], value_vars=gameweek_cols, var_name='gameweek', value_name='score')
                        long_df['gameweek'] = long_df['gameweek'].str.replace('GW', '').astype(int)
                        chart_df = long_df.pivot(index='gameweek', columns='Manager', values='score')
                        st.line_chart(chart_df)
                        st.markdown("**Full Standings**")
                        st.dataframe(wide_df.set_index('Standings'), use_container_width=True)

except Exception as e:
    st.error("An unexpected error occurred.")
    st.info("Please run the `data_pipeline.py` script and ensure it completes successfully.")
    st.exception(e)