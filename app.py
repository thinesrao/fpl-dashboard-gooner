# app.py (CORRECTED with proper API scopes)
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

# --- Configuration ---
GOOGLE_SHEET_NAME = "FPL-Data-Pep"

# --- Google Sheets Connection ---
@st.cache_resource(ttl=600)
def connect_to_gsheet():
    """
    Establishes a connection to the Google Sheet.
    Uses local file for local development, and st.secrets for deployment.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Try to load credentials from the local file first (for local development)
    try:
        creds = Credentials.from_service_account_file(".streamlit/google_credentials.json", scopes=scopes)
    # If the file is not found, it means we're on the deployed server.
    # In that case, load the credentials from Streamlit's secrets manager.
    except FileNotFoundError:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],  # The name of the section you created
            scopes=scopes
        )
        
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME)

@st.cache_data(ttl=600)
def read_from_gsheet(worksheet_name):
    """Reads a specific worksheet from the Google Sheet into a DataFrame."""
    spreadsheet = connect_to_gsheet()
    worksheet = spreadsheet.worksheet(worksheet_name)
    return pd.DataFrame(worksheet.get_all_records())

# --- Main App Logic ---
st.set_page_config(page_title="FPL Mini-League Dashboard", layout="wide")
st.title("🏆 FPL Mini-League Awards Dashboard")

try:
    # --- Load metadata first ---
    metadata = read_from_gsheet("metadata").iloc[0]
    last_gw = metadata['last_finished_gw']
    last_updated = metadata['last_updated_utc']
    
    st.markdown(f"**Awards are calculated based on all data up to Gameweek {last_gw}.**")
    st.caption(f"Last updated: {last_updated} UTC")

    # --- Load all award leaderboards ---
    leaderboards = {
        "golden_boot": read_from_gsheet("golden_boot"),
        "playmaker": read_from_gsheet("playmaker"),
        "golden_glove": read_from_gsheet("golden_glove"),
        "best_gk": read_from_gsheet("best_gk"),
        "best_def": read_from_gsheet("best_def"),
        "best_mid": read_from_gsheet("best_mid"),
        "best_fwd": read_from_gsheet("best_fwd"),
        "best_vc": read_from_gsheet("best_vc")
    }

    st.header(f"🏆 Season Award Leaders (as of GW{last_gw})")

    # --- Metric Card Grid ---
    col1, col2, col3 = st.columns(3)
    
    gb_winner = leaderboards['golden_boot'].iloc[0]
    pm_winner = leaderboards['playmaker'].iloc[0]
    gg_winner = leaderboards['golden_glove'].iloc[0]
    gk_winner = leaderboards['best_gk'].iloc[0]
    def_winner = leaderboards['best_def'].iloc[0]
    mid_winner = leaderboards['best_mid'].iloc[0]
    fwd_winner = leaderboards['best_fwd'].iloc[0]
    vc_winner = leaderboards['best_vc'].iloc[0]

    col1.metric("🥇 Golden Boot", gb_winner['manager_name'], f"{gb_winner.iloc[1]} Goals")
    col2.metric("🅰️ Playmaker", pm_winner['manager_name'], f"{pm_winner.iloc[1]} Assists")
    col3.metric("🧤 Golden Glove", gg_winner['manager_name'], f"{gg_winner.iloc[1]} Clean Sheets")
    st.divider()

    colA, colB, colC, colD = st.columns(4)
    colA.metric("👑 Best Goalkeeper", gk_winner['manager_name'], f"{gk_winner.iloc[1]} Pts")
    colB.metric("🛡️ Best Defenders", def_winner['manager_name'], f"{def_winner.iloc[1]} Pts")
    colC.metric("🎩 Best Midfielders", mid_winner['manager_name'], f"{mid_winner.iloc[1]} Pts")
    colD.metric("💥 Best Forwards", fwd_winner['manager_name'], f"{fwd_winner.iloc[1]} Pts")
    st.metric("🥈 Best Vice-Captain", vc_winner['manager_name'], f"{vc_winner.iloc[1]} Points")

    # --- Detailed Leaderboards in Expanders ---
    st.divider()
    st.subheader("Detailed Leaderboards")
    
    award_titles = {
        "golden_boot": "🥇 Golden Boot Standings", "playmaker": "🅰️ Playmaker Standings",
        "golden_glove": "🧤 Golden Glove Standings", "best_gk": "👑 Best Goalkeeper Standings",
        "best_def": "🛡️ Best Defenders Standings", "best_mid": "🎩 Best Midfielders Standings",
        "best_fwd": "💥 Best Forwards Standings", "best_vc": "🥈 Best Vice-Captain Standings"
    }
    for name, df in leaderboards.items():
        with st.expander(award_titles[name]):
            st.dataframe(df.set_index(df.columns[0]), use_container_width=True)

except (SpreadsheetNotFound, WorksheetNotFound):
    st.error("The required Google Sheet or a specific worksheet was not found.")
    st.info("Please run the `data_pipeline.py` script and ensure it completes successfully before running the app.")
except Exception as e:
    st.error("An unexpected error occurred. Please check the logs.")
    st.exception(e)