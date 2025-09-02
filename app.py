# app.py (v20.7 - Chart now directly reflects sheet order)
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError
import plotly.express as px

# --- Configuration & Connection ---
GOOGLE_SHEET_NAME = "FPL-Data-Pep"


# --- THIS IS THE DEFINITIVE FIX: ROBUST RETRY MECHANISM ---
def safe_gspread_api_call(api_call_func, max_retries=3, initial_delay=2):
    """Wrapper to handle gspread API calls with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return api_call_func()
        except APIError as e:
            if e.response.status_code == 429:
                wait_time = initial_delay * (2 ** attempt)
                time.sleep(wait_time)
            else:
                raise e
    raise Exception("Gspread API call failed after multiple retries.")

@st.cache_resource(ttl=600)
def connect_to_gsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)
    return safe_gspread_api_call(lambda: client.open(GOOGLE_SHEET_NAME))

@st.cache_data(ttl=600)
def read_from_gsheet(worksheet_name):
    try:
        spreadsheet = connect_to_gsheet()
        worksheet = safe_gspread_api_call(lambda: spreadsheet.worksheet(worksheet_name))
        return pd.DataFrame(safe_gspread_api_call(lambda: worksheet.get_all_records()))
    except WorksheetNotFound:
        return None

@st.cache_resource(ttl=600)
def connect_to_gsheet():
    """Connects to the Google Sheets API and returns the spreadsheet object."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(".streamlit/google_credentials.json", scopes=scopes)
    except FileNotFoundError:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME)

# --- Centralized data loading architecture ---
def _read_from_gsheet_uncached(spreadsheet, worksheet_name):
    """Internal function to read a single sheet. No caching here."""
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        return pd.DataFrame(worksheet.get_all_records())
    except WorksheetNotFound:
        st.warning(f"⚠️ Worksheet '{worksheet_name}' not found in your Google Sheet.")
        return None

@st.cache_data(ttl=600)
def load_all_data():
    """Loads all required worksheets in a single batch to avoid hitting API limits."""
    spreadsheet = connect_to_gsheet()
    all_worksheet_titles = [sh.title for sh in spreadsheet.worksheets()]

    data_dictionary = {}
    for title in all_worksheet_titles:
        data_dictionary[title] = _read_from_gsheet_uncached(spreadsheet, title)
    return data_dictionary

# --- Styling Helper Function ---
def highlight_manager(row, manager_name):
    """Highlights a specific manager in a DataFrame row with a multi-index."""
    color = 'background-color: #2bfca4; color: #0D1117;' # Updated highlight colors
    if row.name[1] == manager_name:
        return [color] * len(row)
    else:
        return [''] * len(row)

# --- Function to inject custom CSS for fonts and styles ---
def inject_custom_css():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Archivo+Black&family=Inter:wght@400;700&display=swap');
        
        /* Set header font */
        h1, h2, h3, h4, h5, h6 {
            font-family: 'Archivo Black', sans-serif;
        }

        /* Reduce font size for the metric value (winner's name) */
        div[data-testid="stMetricValue"] {
            font-size: 2rem;
        }

        /* --- NEW: Reduce top padding of the main app container --- */
        div.block-container {
            padding-top: 3rem;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# --- Main App Logic ---
st.set_page_config(page_title="PepRoulette™ FPL Dashboard", layout="wide")

# Inject custom fonts and styles
inject_custom_css()

# --- CHANGE: Use HTML/CSS for robust, mobile-friendly centering ---
LOGO_URL = "https://raw.githubusercontent.com/thinesrao/deepsync/refs/heads/main/logo-word.svg"
st.markdown(
    f"""
    <div style="display: flex; justify-content: center;">
        <img src="{LOGO_URL}" alt="Logo" width="80">
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown("<h1 style='text-align: center;'>🏆 PepRoulette™ FPL Awards Dashboard 🏆</h1>", unsafe_allow_html=True)

try:
    all_data = load_all_data()

    metadata_df = all_data.get("metadata")
    if metadata_df is None or metadata_df.empty:
        st.error("Metadata not found. Please run the data pipeline.")
    else:
        metadata = metadata_df.iloc[0]
        last_gw = metadata['last_finished_gw']
        st.markdown(f"<h5 style='text-align: center;'>Awards calculated up to Gameweek {last_gw}</h5>", unsafe_allow_html=True)

        all_sheets = list(all_data.keys())

        SPECIAL_AWARD_CONFIG = {
            "golden_boot": ["🥇 Golden Boot", "Goals"], "playmaker": ["🅰️ Playmaker", "Assists"],
            "golden_glove": ["🧤 Golden Glove", "Clean Sheets"], "best_gk": ["👑 Best Goalkeeper", "Pts"],
            "best_def": ["🛡️ Best Defenders", "Pts"], "best_mid": ["🎩 Best Midfielders", "Pts"],
            "best_fwd": ["💥 Best Forwards", "Pts"], "best_vc": ["🥈 Best Vice-Captain", "Pts"],
            "transfer_king": ["🔀 Transfer King", "Pts"], "bench_king": ["🪑 Bench King", "Pts"],
            "dream_team": ["🌟 Dream Team King", "DT Score"], "shooting_stars": ["🌠 Shooting Stars", "Rank Rise"],
            "defensive_king": ["🧱 Defensive King", "Contribution"], "best_underdog": ["🥊 Best Underdog", "Wins"],
            "penalty_king": ["🎯 Penalty King", "Pts"], "steady_king": ["🧘 Steady King", "Pts/Transfer"],
            "highest_gw_score": ["🚀 Highest GW Score", "Pts"], "freehit_king": ["🃏 Free Hit King", "Pts"],
            "benchboost_king": ["📈 Bench Boost King", "Pts"], "triplecaptain_king": ["©️³ Triple Captain King", "Pts"]
        }

        st.sidebar.markdown("## ⚙️ Dashboard Controls")

        classic_standings = all_data.get("classic_league_standings")
        all_managers = sorted(classic_standings['Manager'].unique()) if classic_standings is not None else []
        selected_manager = st.sidebar.selectbox("Highlight a Manager", ["None"] + all_managers)

        if last_gw > 1:
            gw_range = st.sidebar.slider("Select Gameweek Range (for charts)", min_value=1, max_value=int(last_gw), value=(1, int(last_gw)))
        else:
            gw_range = (1, 1)
            st.sidebar.info("📊 Range slider for charts will be available from Gameweek 2 onwards.")

        tab_standard, tab_special, tab_details = st.tabs(["🏆 Standard Awards", "🏅 Special Awards", "📊 Detailed Standings"])

        with tab_standard:
            st.markdown("### League Standings & Core Awards")
            col1_main, col2_main = st.columns(2)
            with col1_main:
                with st.container(border=True):
                    st.markdown("#### Classic League Top 10")
                    if classic_standings is not None and not classic_standings.empty:
                        try:
                            # --- ROBUST FIX: Explicitly define the points column name ---
                            # --- Change 'Total' to match your sheet's column header for points ---
                            points_column_name = 'Total'
                            
                            cs_copy = classic_standings.copy()
                            cs_copy[points_column_name] = pd.to_numeric(cs_copy[points_column_name], errors='coerce')
                            
                            top_10_classic = cs_copy.head(10)
                            
                            fig = px.bar(top_10_classic, x=points_column_name, y='Manager', orientation='h', title="Classic League Race", text=points_column_name)
                            
                            fig.update_layout(
                                yaxis_title="", 
                                xaxis_title="Total Points", 
                                showlegend=False, 
                                height=400, 
                                plot_bgcolor='rgba(0,0,0,0)', 
                                paper_bgcolor='rgba(0,0,0,0)',
                                yaxis={'autorange': 'reversed'}
                            )
                            fig.update_traces(marker_color='#2bfca4', textposition='inside')
                            st.plotly_chart(fig, use_container_width=True)
                        except (KeyError, IndexError):
                            st.error(f"⚠️ **Error processing Classic League data.**\n\n"
                                     f"Could not find the points column: '{points_column_name}'.\n\n"
                                     f"**Available columns found:** `{list(classic_standings.columns)}`")
            with col2_main:
                # --- CHANGE: Updated H2H section to be a Top 10 bar chart ---
                with st.container(border=True):
                    st.markdown("#### Head-to-Head Top 10")
                    h2h_standings = all_data.get("h2h_league_standings")
                    if h2h_standings is not None and not h2h_standings.empty:
                        try:
                            # --- Change 'Total' to match your sheet's column header for points ---
                            points_column_name = 'Total H2H Point'

                            h2h_copy = h2h_standings.copy()
                            h2h_copy[points_column_name] = pd.to_numeric(h2h_copy[points_column_name], errors='coerce')

                            top_10_h2h = h2h_copy.head(10)

                            fig_h2h = px.bar(top_10_h2h, x=points_column_name, y='Manager', orientation='h', title="Head-to-Head Race", text=points_column_name)
                            
                            fig_h2h.update_layout(
                                yaxis_title="",
                                xaxis_title="Total Points",
                                showlegend=False,
                                height=400,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                yaxis={'autorange': 'reversed'}
                            )
                            fig_h2h.update_traces(marker_color='#2bfca4', textposition='inside')
                            st.plotly_chart(fig_h2h, use_container_width=True)
                        except (KeyError, IndexError):
                            st.error(f"⚠️ **Error processing Head-to-Head data.**\n\n"
                                     f"Could not find the points column: '{points_column_name}'.\n\n"
                                     f"**Available columns found:** `{list(h2h_standings.columns)}`")

            cup_winner_df = all_data.get("cup_winner")
            if last_gw >= 34 and cup_winner_df is not None and not cup_winner_df.empty:
                with st.container(border=True):
                    st.metric("League Cup Champion", cup_winner_df.iloc[0]['Winner'])

            with st.container(border=True):
                st.markdown("#### Monthly & Weekly Winners")
                sub_tab_classic, sub_tab_h2h, sub_tab_weekly, sub_tab_challenge = st.tabs(["Classic Monthly", "H2H Monthly", "Manager of the Week", "FPL Challenge"])
                with sub_tab_classic:
                    classic_monthly_sheets = sorted([s for s in all_sheets if s.startswith('classic_monthly_')])
                    for sheet_name in classic_monthly_sheets:
                        st.markdown(f"##### {sheet_name.replace('classic_monthly_', '').replace('_', ' ').title()}")
                        df = all_data.get(sheet_name)
                        if df is not None: st.dataframe(df.set_index('Standings'), use_container_width=True)
                with sub_tab_h2h:
                    h2h_monthly_sheets = sorted([s for s in all_sheets if s.startswith('h2h_monthly_')])
                    for sheet_name in h2h_monthly_sheets:
                        st.markdown(f"##### {sheet_name.replace('h2h_monthly_', '').replace('_', ' ').title()}")
                        df = all_data.get(sheet_name)
                        if df is not None: st.dataframe(df.set_index('Standings'), use_container_width=True)
                with sub_tab_weekly:
                    weekly_log = all_data.get("weekly_manager_log")
                    if weekly_log is not None: st.dataframe(weekly_log.set_index('Gameweek'), use_container_width=True)
                with sub_tab_challenge:
                    challenge_log = all_data.get("fpl_challenge_weekly_log")
                    if challenge_log is not None: st.dataframe(challenge_log.set_index('Gameweek'), use_container_width=True)

        with tab_special:
            st.markdown("### Special Award Winners")
            special_award_sheets = {name: all_data.get(name) for name in SPECIAL_AWARD_CONFIG.keys()}
            cols = st.columns(4)
            col_idx = 0
            for name, df in special_award_sheets.items():
                if df is not None and not df.empty:
                    leader = df.iloc[0]
                    title, suffix = SPECIAL_AWARD_CONFIG[name]
                    score_col_name = df.columns[3]
                    leader_score = pd.to_numeric(leader[score_col_name], errors='coerce')
                    with cols[col_idx % 4]:
                        with st.container(border=True):
                            if pd.notna(leader_score) and leader_score > 0:
                                score_text = f"{leader[score_col_name]} {suffix}"
                                gap_text = ""
                                if len(df) > 1:
                                    second_place_score = pd.to_numeric(df.iloc[1][score_col_name], errors='coerce')
                                    if pd.notna(second_place_score):
                                        gap = leader_score - second_place_score
                                        gap_text = f"({gap:,.1f} ahead)".replace(".0", "")
                                combined_delta = f"{score_text} {gap_text}".strip()
                                st.metric(label=title, value=leader['Manager'], delta=combined_delta)
                            else:
                                st.metric(label=title, value="N/A", delta=f"0 {suffix}")
                    col_idx += 1

        with tab_details:
            st.markdown("### Detailed Award Standings")
            st.info(f"📈 Showing cumulative progression for Gameweeks {gw_range[0]} to {gw_range[1]}")
            special_award_sheets = {name: all_data.get(name) for name in SPECIAL_AWARD_CONFIG.keys()}
            for name, (title, _) in SPECIAL_AWARD_CONFIG.items():
                wide_df = special_award_sheets.get(name)
                if wide_df is not None and not wide_df.empty:
                    with st.expander(f"**{title}**"):
                        gameweek_cols = sorted([col for col in wide_df.columns if col.startswith('GW')])
                        wide_df_indexed = wide_df.set_index(['Standings', 'Manager'])
                        if not gameweek_cols:
                            st.markdown("**Standings**")
                            styled_df = wide_df_indexed.style
                            if selected_manager != "None":
                                styled_df = styled_df.apply(highlight_manager, manager_name=selected_manager, axis=1)
                            st.dataframe(styled_df, use_container_width=True)
                        else:
                            df_for_chart = wide_df.copy()
                            for col in gameweek_cols:
                                df_for_chart[col] = pd.to_numeric(df_for_chart[col], errors='coerce').fillna(0)
                            cumulative_df = df_for_chart[gameweek_cols].cumsum(axis=1)
                            cumulative_df['Manager'] = df_for_chart['Manager']
                            long_df = cumulative_df.melt(id_vars=['Manager'], value_vars=gameweek_cols, var_name='gameweek', value_name='cumulative_score')
                            long_df['gameweek'] = long_df['gameweek'].str.replace('GW', '').astype(int)
                            long_df_filtered = long_df[long_df['gameweek'].between(gw_range[0], gw_range[1])]
                            if not long_df_filtered.empty:
                                fig = px.line(long_df_filtered, x='gameweek', y='cumulative_score', color='Manager', title=f"{title}: Cumulative Progression", labels={'gameweek': 'Gameweek', 'cumulative_score': 'Cumulative Score'}, markers=True)
                                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                                if selected_manager != "None":
                                    for trace in fig.data:
                                        if trace.name == selected_manager:
                                            trace.update(line=dict(width=5, color='#2bfca4'))
                                        else:
                                            trace.update(line=dict(width=2), opacity=0.7)
                                st.plotly_chart(fig, use_container_width=True)
                            st.markdown("**Full Standings (by individual GW score)**")
                            styled_wide = wide_df_indexed.style
                            if selected_manager != "None":
                                styled_wide = styled_wide.apply(highlight_manager, manager_name=selected_manager, axis=1)
                            st.dataframe(styled_wide, use_container_width=True)

except APIError as e:
    st.error(f"🚨 **Google Sheets API Error:** {e}\n\nThis is likely a temporary quota issue. Please wait a minute and refresh.")
except Exception as e:
    st.error("An unexpected error occurred. This could be due to a temporary issue with Google Sheets.")
    st.info("If the problem persists, please check the sheet names and column headers in your Google Sheet.")
    st.exception(e)