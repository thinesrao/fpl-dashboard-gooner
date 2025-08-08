import streamlit as st
import requests
import pandas as pd
import time
import json # Library to handle JSON files

# --- Configuration ---

# Master switch to toggle between live API and local JSON files for testing
# True = Use local JSON files from the 'data/' folder
# False = Use the live FPL API
USE_LOCAL_DATA = True

# Your FPL mini-league ID
LEAGUE_ID = 665732

# --- FPL API Endpoints (only used if USE_LOCAL_DATA is False) ---
FPL_API_URL = "https://fantasy.premierleague.com/api/"
BOOTSTRAP_STATIC_URL = f"{FPL_API_URL}bootstrap-static/"
LEAGUE_URL = f"{FPL_API_URL}leagues-classic/{LEAGUE_ID}/standings/"
ENTRY_EVENT_URL = f"{FPL_API_URL}entry/{{entry_id}}/event/{{gameweek}}/picks/"

# --- Data Fetching with Caching ---

@st.cache_data(ttl=900)
def get_fpl_data():
    """Fetches general FPL data from local file or live API."""
    if USE_LOCAL_DATA:
        st.info("`USE_LOCAL_DATA` is True. Loading `bootstrap-static.json` from local `data/` folder.")
        with open("data/bootstrap-static.json", "r") as f:
            return json.load(f)
    else:
        response = requests.get(BOOTSTRAP_STATIC_URL)
        response.raise_for_status()
        return response.json()

@st.cache_data(ttl=900)
def get_league_data(league_id):
    """Fetches league standings from local file or live API."""
    if USE_LOCAL_DATA:
        st.info("`USE_LOCAL_DATA` is True. Loading `league-standings.json` from local `data/` folder.")
        with open("data/league-standings.json", "r") as f:
            return json.load(f)
    else:
        response = requests.get(LEAGUE_URL.format(league_id=league_id))
        response.raise_for_status()
        return response.json()

@st.cache_data(ttl=900)
def get_manager_team(manager_id, gameweek):
    """Fetches a manager's team for a gameweek from local file or live API."""
    if USE_LOCAL_DATA:
        filepath = f"data/picks/manager_{manager_id}_gw_{gameweek}.json"
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            st.error(f"Local file not found for this selection: {filepath}")
            return None
    else:
        url = ENTRY_EVENT_URL.format(entry_id=manager_id, gameweek=gameweek)
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            st.error(f"Could not fetch data for manager {manager_id} in GW{gameweek}: {e}")
            return None

# --- Main App Logic ---

st.set_page_config(page_title="FPL Mini-League Dashboard", layout="wide")

st.title(f"🏆 FPL Mini-League Dashboard")
if USE_LOCAL_DATA:
    st.warning("⚠️ App is running in Local Data Mode. Data is not live.")
st.markdown(f"### League Name: PepRoulette™   League ID: {LEAGUE_ID}")

# --- Data Loading and Processing ---
with st.spinner('Loading FPL data...'):
    fpl_data = get_fpl_data()
    league_data = get_league_data(LEAGUE_ID)

    player_map = {player['id']: f"{player['first_name']} {player['web_name']}" for player in fpl_data['elements']}
    team_map = {team['id']: team['name'] for team in fpl_data['teams']}

    # --- Process and determine the current or last finished gameweek ---
    current_gameweek = 0
    for gw_info in fpl_data['events']:
        if gw_info['is_current']:
            current_gameweek = gw_info['id']
            break
    if current_gameweek == 0:
        for gw_info in reversed(fpl_data['events']):
            if gw_info['finished']:
                current_gameweek = gw_info['id']
                break

# --- Sidebar for User Input ---
st.sidebar.header("Filters")
selected_gameweek = st.sidebar.slider(
    "Select Gameweek",
    min_value=1,
    max_value=38, # Hardcode to 38 for local testing
    value=current_gameweek if current_gameweek != 0 else 38,
    step=1
)

# --- Process League and Manager Data ---
managers = league_data['standings']['results']
manager_df = pd.DataFrame(managers)
manager_df = manager_df[['entry', 'player_name', 'entry_name', 'total', 'rank']]
manager_df.rename(columns={'entry': 'manager_id', 'player_name': 'manager_name', 'entry_name': 'team_name'}, inplace=True)

st.header(f"Gameweek {selected_gameweek} Analysis")

# Fetch all manager picks for the selected gameweek
all_manager_picks = {}
progress_bar = st.progress(0)
status_text = st.empty()

for i, manager in enumerate(manager_df.itertuples()):
    status_text.text(f"Fetching data for {manager.manager_name}...")
    picks = get_manager_team(manager.manager_id, selected_gameweek)
    if picks:
        all_manager_picks[manager.manager_id] = picks
    progress_bar.progress((i + 1) / len(manager_df))
    # No need to sleep for local files, it will be instant
    if not USE_LOCAL_DATA:
        time.sleep(0.1)

status_text.text("All data loaded successfully!")
progress_bar.empty()

# --- Award Category Dashboards ---
tab1, tab2, tab3 = st.tabs(["👑 King of the Gameweek", "🪑 Bench Warmers", "©️ Captaincy Report"])

with tab1:
    gw_scores = []
    for manager_id, picks in all_manager_picks.items():
        if picks and picks.get('entry_history'):
            manager_name = manager_df.loc[manager_df['manager_id'] == manager_id, 'manager_name'].iloc[0]
            gw_points = picks['entry_history']['points']
            gw_scores.append({'Manager': manager_name, 'Gameweek Points': gw_points})

    if gw_scores:
        gw_scores_df = pd.DataFrame(gw_scores).sort_values(by='Gameweek Points', ascending=False).reset_index(drop=True)
        st.subheader("Gameweek Scoreboard")
        st.dataframe(gw_scores_df, use_container_width=True)
        if not gw_scores_df.empty:
            winner = gw_scores_df.iloc[0]
            st.metric(label=f"👑 King of Gameweek {selected_gameweek}", value=winner['Manager'], delta=f"{winner['Gameweek Points']} Points")

with tab2:
    bench_scores = []
    for manager_id, picks in all_manager_picks.items():
        if picks and picks.get('entry_history'):
            manager_name = manager_df.loc[manager_df['manager_id'] == manager_id, 'manager_name'].iloc[0]
            bench_points = picks['entry_history']['points_on_bench']
            bench_scores.append({'Manager': manager_name, 'Bench Points': bench_points})

    if bench_scores:
        bench_scores_df = pd.DataFrame(bench_scores).sort_values(by='Bench Points', ascending=False).reset_index(drop=True)
        st.subheader("Bench Performance")
        st.bar_chart(bench_scores_df.set_index('Manager'))
        if not bench_scores_df.empty:
            winner = bench_scores_df.iloc[0]
            st.metric(label="🪑 Top Bench Warmer", value=winner['Manager'], delta=f"{winner['Bench Points']} Points Left on Bench", delta_color="off")

with tab3:
    captain_picks = []
    for manager_id, picks_data in all_manager_picks.items():
        if picks_data and picks_data.get('picks'):
            manager_name = manager_df.loc[manager_df['manager_id'] == manager_id, 'manager_name'].iloc[0]
            for pick in picks_data['picks']:
                if pick['is_captain']:
                    player_id = pick['element']
                    player_name = player_map.get(player_id, "Unknown Player")
                    captain_picks.append({'Manager': manager_name, 'Captain': player_name, 'Multiplier': pick['multiplier']})
                    if picks_data.get('active_chip') == 'trip_capt':
                        captain_picks[-1]['Multiplier'] = 3

    if captain_picks:
        captain_df = pd.DataFrame(captain_picks)
        st.subheader("Captain Choices")
        st.dataframe(captain_df, use_container_width=True)

st.subheader("Overall League Standings")
st.dataframe(manager_df.set_index('rank'), use_container_width=True)