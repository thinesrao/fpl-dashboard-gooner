# app.py (FINAL v16.3 - Corrected AttributeError for Multi-Index)
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
import plotly.express as px

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

@st.cache_data(ttl=3600)
def get_all_sheet_names():
    try:
        spreadsheet = connect_to_gsheet()
        return [sh.title for sh in spreadsheet.worksheets()]
    except Exception:
        return []

# --- Styling Helper Function ---
def highlight_manager(row, manager_name):
    """Highlights a specific manager in a DataFrame row with a multi-index."""
    color = 'background-color: #37003c; color: white;'
    # --- THIS IS THE DEFINITIVE FIX ---
    # For a multi-index, the values are in a tuple in the row's 'name' attribute.
    # The manager's name is the second element (index 1).
    if row.name[1] == manager_name:
        return [color] * len(row)
    else:
        return [''] * len(row)

# --- Main App Logic ---
st.set_page_config(page_title="PepRoulette™ FPL Dashboard", layout="wide")
st.markdown("<h1 style='text-align: center;'>🏆 PepRoulette™ FPL Awards Dashboard 🏆</h1>", unsafe_allow_html=True)

try:
    metadata_df = read_from_gsheet("metadata")
    if metadata_df is None or metadata_df.empty:
        st.error("Metadata not found. Please run the data pipeline.")
    else:
        metadata = metadata_df.iloc[0]
        last_gw = metadata['last_finished_gw']
        st.markdown(f"<h5 style='text-align: center;'>Awards calculated up to Gameweek {last_gw}</h5>", unsafe_allow_html=True)
        
        all_sheets = get_all_sheet_names()

        # --- Award Configurations ---
        SPECIAL_AWARD_CONFIG = {
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

        # --- Sidebar for Interactivity ---
        st.sidebar.markdown("## ⚙️ Dashboard Controls")
        
        classic_standings = read_from_gsheet("classic_league_standings")
        all_managers = sorted(classic_standings['Manager'].unique()) if classic_standings is not None else []
        selected_manager = st.sidebar.selectbox("Highlight a Manager", ["None"] + all_managers)
        
        if last_gw > 1:
            gw_range = st.sidebar.slider( "Select Gameweek Range (for charts)", min_value=1, max_value=int(last_gw), value=(1, int(last_gw)) )
        else:
            gw_range = (1, 1)
            st.sidebar.info("📊 Range slider for charts will be available from Gameweek 2 onwards.")

        # --- Main App Structure with Tabs ---
        tab_standard, tab_special, tab_details = st.tabs(["🏆 Standard Awards", "🏅 Special Awards", "📊 Detailed Standings"])

        with tab_standard:
            st.markdown("### League Standings & Core Awards")
            col1, col2 = st.columns(2)
            with col1:
                with st.container(border=True):
                    st.markdown("#### Classic League Top 4")
                    if classic_standings is not None:
                        # The app will now correctly handle the multi-index styling
                        styled_classic = classic_standings.head(4).set_index(['Standings', 'Manager']).style
                        if selected_manager != "None":
                            styled_classic = styled_classic.apply(highlight_manager, manager_name=selected_manager, axis=1)
                        st.dataframe(styled_classic, use_container_width=True)
            with col2:
                with st.container(border=True):
                    st.markdown("#### Head-to-Head Top 4")
                    h2h_standings = read_from_gsheet("h2h_league_standings")
                    if h2h_standings is not None:
                        styled_h2h = h2h_standings.head(4).set_index(['Standings', 'Manager']).style
                        if selected_manager != "None":
                            styled_h2h = styled_h2h.apply(highlight_manager, manager_name=selected_manager, axis=1)
                        st.dataframe(styled_h2h, use_container_width=True)

            if last_gw >= 34:
                with st.container(border=True):
                    cup_winner_df = read_from_gsheet("cup_winner")
                    if cup_winner_df is not None and not cup_winner_df.empty:
                        st.metric("League Cup Champion", cup_winner_df.iloc[0]['Winner'], "")

            with st.container(border=True):
                st.markdown("#### Monthly & Weekly Winners")
                sub_tab_classic, sub_tab_h2h, sub_tab_weekly, sub_tab_challenge = st.tabs(["Classic Monthly", "H2H Monthly", "Manager of the Week", "FPL Challenge"])
                with sub_tab_classic:
                    classic_monthly_sheets = sorted([s for s in all_sheets if s.startswith('classic_monthly_')])
                    for sheet_name in classic_monthly_sheets:
                        st.markdown(f"##### {sheet_name.replace('classic_monthly_', '').replace('_', ' ').title()}")
                        df = read_from_gsheet(sheet_name)
                        if df is not None: st.dataframe(df.set_index('Standings'), use_container_width=True)
                with sub_tab_h2h:
                    h2h_monthly_sheets = sorted([s for s in all_sheets if s.startswith('h2h_monthly_')])
                    for sheet_name in h2h_monthly_sheets:
                        st.markdown(f"##### {sheet_name.replace('h2h_monthly_', '').replace('_', ' ').title()}")
                        df = read_from_gsheet(sheet_name)
                        if df is not None: st.dataframe(df.set_index('Standings'), use_container_width=True)
                with sub_tab_weekly:
                    weekly_log = read_from_gsheet("weekly_manager_log")
                    if weekly_log is not None: st.dataframe(weekly_log.set_index('Gameweek'), use_container_width=True)
                with sub_tab_challenge:
                    challenge_standings = read_from_gsheet("fpl_challenge_standings")
                    if challenge_standings is not None: st.dataframe(challenge_standings.set_index('Standings'), use_container_width=True)

        with tab_special:
            st.markdown("### Special Award Winners")
            special_award_sheets = {name: read_from_gsheet(name) for name in SPECIAL_AWARD_CONFIG.keys()}
            
            all_special_award_names = list(SPECIAL_AWARD_CONFIG.keys())
            cols = st.columns(4)
            col_idx = 0
            for name in all_special_award_names:
                df = special_award_sheets.get(name)
                if df is not None and not df.empty:
                    leader = df.iloc[0]
                    title, suffix = SPECIAL_AWARD_CONFIG[name]
                    score_col_name = df.columns[3]
                    leader_score = pd.to_numeric(leader[score_col_name], errors='coerce')
                    
                    with cols[col_idx % 4]:
                        with st.container(border=True):
                            if leader_score > 0:
                                st.metric(title, leader['Manager'], f"{leader[score_col_name]} {suffix}")
                            else:
                                st.metric(title, "Not Available", "0")
                    col_idx += 1

        with tab_details:
            st.markdown("### Detailed Award Standings")
            st.info(f"Showing data for Gameweeks {gw_range[0]} to {gw_range[1]}")
            
            special_award_sheets = {name: read_from_gsheet(name) for name in SPECIAL_AWARD_CONFIG.keys()}
            for name, (title, _) in SPECIAL_AWARD_CONFIG.items():
                wide_df = special_award_sheets.get(name)
                if wide_df is not None and not wide_df.empty:
                    with st.expander(f"**{title}**"):
                        gameweek_cols = [col for col in wide_df.columns if col.startswith('GW')]
                        
                        wide_df_indexed = wide_df.set_index(['Standings', 'Manager'])

                        if not gameweek_cols:
                            styled_df = wide_df_indexed.style
                            if selected_manager != "None":
                                styled_df = styled_df.apply(highlight_manager, manager_name=selected_manager, axis=1)
                            st.dataframe(styled_df, use_container_width=True)
                        else:
                            long_df = wide_df.melt(id_vars=['Manager'], value_vars=gameweek_cols, var_name='gameweek', value_name='score')
                            long_df['gameweek'] = long_df['gameweek'].str.replace('GW', '').astype(int)
                            
                            long_df_filtered = long_df[long_df['gameweek'].between(gw_range[0], gw_range[1])]

                            if not long_df_filtered.empty:
                                fig = px.line(
                                    long_df_filtered, x='gameweek', y='score', color='Manager', title=f"{title}: Gameweek Progression",
                                    labels={'gameweek': 'Gameweek', 'score': 'Score', 'Manager': 'Manager'}, markers=True
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            
                            st.markdown("**Full Standings**")
                            styled_wide = wide_df_indexed.style
                            if selected_manager != "None":
                                styled_wide = styled_wide.apply(highlight_manager, manager_name=selected_manager, axis=1)
                            st.dataframe(styled_wide, use_container_width=True)

except Exception as e:
    st.error("An unexpected error occurred.")
    st.info("Please run the `data_pipeline.py` script and ensure it completes successfully.")
    st.exception(e)