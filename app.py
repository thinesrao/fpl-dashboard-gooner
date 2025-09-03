# app.py (v20.7 - Chart now directly reflects sheet order)
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError
import plotly.express as px
from datetime import datetime, timezone
import time

# --- Configuration & Connection ---
GOOGLE_SHEET_NAME = "FPL-Data-Gooner"

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

# --- Centralized, Robust, and Efficient Data Loading ---
def gspread_api_call(api_call_func, max_retries=5, initial_delay=3):
    """
    Definitive wrapper to handle all gspread API calls with exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            return api_call_func()
        except APIError as e:
            if e.response.status_code == 429:
                wait_time = initial_delay * (2 ** attempt)
                print(f"API rate limit hit. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise e # For other API errors, fail immediately
    raise Exception(f"Gspread API call failed after {max_retries} retries.")

@st.cache_data(ttl=600, show_spinner=False)
def load_all_data():
    """
    Loads all worksheets in a single, efficient, and robust batch operation.
    """
    print("Loading all data from Google Sheets...")
    spreadsheet = connect_to_gsheet()
    
    # --- The Definitive Fix: Fetch all worksheets in one batch call ---
    all_worksheets = gspread_api_call(lambda: spreadsheet.worksheets())
    
    data_dictionary = {}
    for worksheet in all_worksheets:
        print(f"  Processing worksheet: {worksheet.title}")
        # Use a lambda to pass the get_all_records call to the retry wrapper
        records = gspread_api_call(lambda: worksheet.get_all_records())
        data_dictionary[worksheet.title] = pd.DataFrame(records)
        
    print("All data loaded successfully.")
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
st.set_page_config(page_title="槍迷之家超級聯賽 FPL Dashboard", layout="wide")

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

st.markdown("<h3 style='text-align: center;'>🏆 槍迷之家超級聯賽 FPL Season 6 🏆</h3>", unsafe_allow_html=True)

try:
        # --- Data Loading with a Progress Bar ---
    progress_text = "Loading all award data. Please wait..."
    my_bar = st.progress(0, text=progress_text)

    # Wrap the slow data loading function with the progress bar
    all_data = load_all_data()

    # Once data is loaded, update the progress bar to 100% and then remove it
    my_bar.progress(100, text="Data loaded successfully!")
    time.sleep(1) # Give a moment for the user to see "Success"
    my_bar.empty()

    metadata_df = all_data.get("metadata")
    if metadata_df is None or metadata_df.empty:
        st.error("Metadata not found. Please run the data pipeline.")
    else:
        metadata = metadata_df.iloc[0]
        last_gw = metadata['last_finished_gw']
        last_updated_utc_str = metadata['last_updated_utc']
        
        # --- THIS IS THE NEW LOGIC BLOCK ---
        # Convert the ISO string from the sheet into a datetime object
        # The 'Z' at the end of older FPL timestamps means UTC, which fromisoformat handles
        dt_object = datetime.fromisoformat(last_updated_utc_str.replace('Z', '+00:00'))
        
        # Format the datetime object into a highly readable string
        # Example: "Sep 02 2025, 10:30 PM"
        readable_timestamp = dt_object.strftime("%d %b %Y, %I:%M %p")

        # Display the gameweek and the last updated time in a clean, centered format
        st.markdown(
            f"""
            <div style='text-align: center;'>
                <h5>Awards calculated up to Gameweek {last_gw}</h5>
                <p><small>Last Updated: {readable_timestamp} (UTC)</small></p>
            </div>
            """,
            unsafe_allow_html=True
        )
                
        all_sheets = list(all_data.keys())

        SPECIAL_AWARD_CONFIG = {
            "golden_boot": ["🥇 Golden Boot", "Goals"], "playmaker": ["🅰️ Playmaker", "Assists"],
            "golden_glove": ["🧤 Golden Glove", "Clean Sheets"], "best_gk": ["👑 Best Goalkeeper", "Pts"],
            "best_def": ["🛡️ Best Defenders", "Pts"], "best_mid": ["🎩 Best Midfielders", "Pts"],
            "best_fwd": ["💥 Best Forwards", "Pts"], "best_vc": ["🥈 Best Vice-Captain", "Pts"],
            "transfer_king": ["🔀 Transfer King", "Pts"], "bench_king": ["🪑 Bench King", "Pts"],
            "dream_team": ["🌟 Dream Team King", "DT Score"], "shooting_stars": ["🌠 Shooting Stars", "Rank Rise"],
            "defensive_king": ["🧱 Defensive King", "Contribution"], 
            "penalty_king": ["🎯 Penalty King", "Pts"], "steady_king": ["🧘 Steady King", "Pts/Transfer"],
            "freehit_king": ["🃏 Free Hit King", "Pts"],
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

        tab_standard, tab_special, tab_details = st.tabs(["🏆 Standard Awards", "🏅 Special Records", "📊 Detailed Standings"])

        with tab_standard:
            st.markdown("### 🏆 League Standings & Main Awards")
            
            # --- New "Command Center" Layout ---
            col_main, col_sidebar = st.columns([2.5, 1]) # Main column is 2.5x wider

            # --- Main Column: The League Race Chart ---
            with col_main:
                with st.container(border=True):
                    st.markdown("#### Classic League Race")
                    if classic_standings is not None and not classic_standings.empty:
                        try:
                            points_column_name = 'Total'
                            cs_copy = classic_standings.copy()
                            cs_copy[points_column_name] = pd.to_numeric(cs_copy[points_column_name], errors='coerce')
                            
                            top_10_classic = cs_copy.head(10)
                            
                            fig = px.bar(
                                top_10_classic, x=points_column_name, y='Manager', orientation='h',
                                title="Current Top 10", text=points_column_name
                            )
                            fig.update_layout(
                                yaxis_title="", xaxis_title="Total Points", showlegend=False, height=500,
                                plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                                yaxis={'autorange': 'reversed'}, # Puts #1 at the top
                                font=dict(color="white"),
                                title_font_size=20
                            )
                            fig.update_traces(marker_color='#2bfca4', textposition='inside', textfont=dict(color='#0D1117', family="Archivo Black"))
                            st.plotly_chart(fig, use_container_width=True)
                        except Exception as e:
                            st.error(f"⚠️ Error creating Classic League chart: {e}")

            # --- Sidebar Column: The Intel Briefing ---
            with col_sidebar:
                # Card 1: Recent Weekly Winners Log
                with st.container(border=True):
                    st.markdown("##### 👑Manager of The Week")
                    weekly_log = all_data.get("weekly_manager_log")
                    if weekly_log is not None and not weekly_log.empty:
                        # Show a clean, compact log of recent winners
                        recent_winners = weekly_log.sort_values(by='Gameweek', ascending=False).head(5)
                        st.dataframe(
                            recent_winners[['Gameweek', 'Manager', 'Score']].set_index('Gameweek'),
                            use_container_width=True
                        )
                # Card 2: Highest GW Score
                with st.container(border=True):
                    st.markdown("##### 🚀 Highest GW Score")
                    highest_score_df = all_data.get("highest_gw_score")
                    if highest_score_df is not None and not highest_score_df.empty:
                        leader = highest_score_df.iloc[0]
                        st.metric("All-Time High Score", leader['Manager'], f"{leader['Score']} Pts")
                
                st.markdown("---", unsafe_allow_html=True)



        with tab_special:
            st.markdown("### Special Record Breakers")
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
            st.markdown("### Detailed Standings")
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