# data_pipeline.py (updated for GitHub Actions)
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import requests
import json
import time
from datetime import datetime
import os # Import the os module

# --- Configuration ---
LEAGUE_ID = 665732
GOOGLE_SHEET_NAME = "FPL-Data-Pep"
CREDENTIALS_FILE = ".streamlit/google_credentials.json"

# --- FPL API Endpoints ---
FPL_API_URL = "https://fantasy.premierleague.com/api/"
BOOTSTRAP_STATIC_URL = f"{FPL_API_URL}bootstrap-static/"
LEAGUE_URL = f"{FPL_API_URL}leagues-classic/{LEAGUE_ID}/standings/"
ENTRY_EVENT_URL = f"{FPL_API_URL}entry/{{entry_id}}/event/{{gameweek}}/picks/"
ELEMENT_SUMMARY_URL = f"{FPL_API_URL}element-summary/{{player_id}}/"

def get_credentials():
    """Gets credentials from environment variable or local file."""
    # Check if the GCP_CREDENTIALS environment variable exists (for GitHub Actions)
    creds_json_str = os.getenv("GCP_CREDENTIALS")
    if creds_json_str:
        print("Authenticating via GitHub Actions secret...")
        creds_json = json.loads(creds_json_str)
        return gspread.service_account_from_dict(creds_json)
    # Otherwise, fall back to the local file (for local development)
    else:
        print("Authenticating via local credentials file...")
        return gspread.service_account(filename=CREDENTIALS_FILE)
    
def get_json_from_url(url):
    """Generic function to get JSON from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def get_player_gameweek_points(player_id, all_player_details):
    """Extracts a player's points for each gameweek from their history."""
    for player_data in all_player_details:
        if player_data and 'id' in player_data and player_data['id'] == player_id:
            return {item['round']: item['total_points'] for item in player_data.get('history', [])}
    return {}

def main():
    print("--- Starting FPL Data Pipeline ---")

    # 1. Connect to Google Sheets using the new credentials function
    gc = get_credentials()
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    print(f"Connected to Google Sheet: '{GOOGLE_SHEET_NAME}'")
    
    # 2. Fetch Base Data
    fpl_data = get_json_from_url(BOOTSTRAP_STATIC_URL)
    league_data = get_json_from_url(LEAGUE_URL)

    if not fpl_data or not league_data:
        print("Failed to fetch base data. Exiting.")
        return

    # *** NEW: Dynamically find the last finished gameweek ***
    finished_gws = [gw['id'] for gw in fpl_data['events'] if gw['finished']]
    if not finished_gws:
        print("No gameweeks have finished yet. Exiting.")
        return
    last_finished_gw = max(finished_gws)
    print(f"Detected last finished gameweek as GW{last_finished_gw}")

    manager_df = pd.DataFrame(league_data['standings']['results'])
    manager_df = manager_df[['entry', 'player_name']].rename(columns={'entry': 'manager_id'})
    elements_df = pd.DataFrame(fpl_data['elements'])
    player_map = elements_df.set_index('id')['web_name'].to_dict()

    # 3. Fetch and Process Picks up to the last finished GW
    all_picks_list = []
    print(f"Fetching data for all managers up to GW{last_finished_gw}...")
    
    print("Pre-fetching details for all players... (This may take a few minutes)")
    all_player_details = [get_json_from_url(ELEMENT_SUMMARY_URL.format(player_id=pid)) for pid in player_map.keys()]
    print("Finished pre-fetching player details.")
    
    # *** MODIFIED: Loop only up to the last finished gameweek ***
    for gw in range(1, last_finished_gw + 1):
        for _, manager in manager_df.iterrows():
            manager_id, manager_name = manager['manager_id'], manager['player_name']
            picks_data = get_json_from_url(ENTRY_EVENT_URL.format(entry_id=manager_id, gameweek=gw))
            
            if picks_data and 'picks' in picks_data:
                for p in picks_data['picks']:
                    player_id = p['element']
                    player_points_map = get_player_gameweek_points(player_id, all_player_details)
                    all_picks_list.append({
                        "gameweek": gw, "manager_id": manager_id, "manager_name": manager_name,
                        "player_id": player_id, "player_name": player_map.get(player_id, "Unknown"),
                        "is_captain": p['is_captain'], "is_vice_captain": p['is_vice_captain'],
                        "points": player_points_map.get(gw, 0) * p['multiplier']
                    })
        print(f"  Processed Gameweek {gw}/{last_finished_gw}")
        if gw < last_finished_gw: time.sleep(1)

    flat_picks_df = pd.DataFrame(all_picks_list)

    # 4. Calculate Season Awards (based on data to date)
    print("Calculating season awards (to date)...")
    # *** MODIFIED: Use the last finished GW squad for awards ***
    current_squad_df = flat_picks_df[flat_picks_df['gameweek'] == last_finished_gw]
    player_season_stats = elements_df[['id', 'goals_scored', 'assists', 'clean_sheets', 'element_type', 'total_points']]
    current_squad_stats = current_squad_df.merge(player_season_stats, left_on='player_id', right_on='id')

    # Award calculations are now based on "current_squad_stats"
    golden_boot = current_squad_stats.groupby('manager_name')['goals_scored'].sum().sort_values(ascending=False).reset_index()
    playmaker = current_squad_stats.groupby('manager_name')['assists'].sum().sort_values(ascending=False).reset_index()
    golden_glove = current_squad_stats[current_squad_stats['element_type'].isin([1,2,3])].groupby('manager_name')['clean_sheets'].sum().sort_values(ascending=False).reset_index()
    best_gk = current_squad_stats[current_squad_stats['element_type'] == 1].groupby('manager_name')['total_points'].sum().sort_values(ascending=False).reset_index()
    best_def = current_squad_stats[current_squad_stats['element_type'] == 2].groupby('manager_name')['total_points'].sum().sort_values(ascending=False).reset_index()
    best_mid = current_squad_stats[current_squad_stats['element_type'] == 3].groupby('manager_name')['total_points'].sum().sort_values(ascending=False).reset_index()
    best_fwd = current_squad_stats[current_squad_stats['element_type'] == 4].groupby('manager_name')['total_points'].sum().sort_values(ascending=False).reset_index()
    best_vc = flat_picks_df[flat_picks_df['is_vice_captain']].groupby('manager_name')['points'].sum().sort_values(ascending=False).reset_index()

    # 5. Write to Google Sheets
    print("Writing processed data to Google Sheets...")
    worksheets = {
        "golden_boot": golden_boot, "playmaker": playmaker, "golden_glove": golden_glove,
        "best_gk": best_gk, "best_def": best_def, "best_mid": best_mid, "best_fwd": best_fwd,
        "best_vc": best_vc
    }
    # *** NEW: Add metadata sheet to inform the app ***
    metadata_df = pd.DataFrame([{'last_finished_gw': last_finished_gw, 'last_updated_utc': datetime.utcnow().isoformat()}])
    worksheets["metadata"] = metadata_df

    for name, df in worksheets.items():
        try:
            worksheet = spreadsheet.worksheet(name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=name, rows=len(df) + 1, cols=len(df.columns) + 1)
        set_with_dataframe(worksheet, df)
        print(f"  Successfully wrote to '{name}' worksheet.")

    print("--- Pipeline finished successfully! ---")

if __name__ == "__main__":
    main()