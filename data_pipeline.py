# data_pipeline.py (FINAL v5 - Competition Ranking & VC Bug Fix)
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import requests
import json
import time
from datetime import datetime
import os

# --- Configuration & Helper Functions ---
LEAGUE_ID = 665732
GOOGLE_SHEET_NAME = "FPL-Data-Pep"
CREDENTIALS_FILE = ".streamlit/google_credentials.json"
FPL_API_URL = "https://fantasy.premierleague.com/api/"
BOOTSTRAP_STATIC_URL = f"{FPL_API_URL}bootstrap-static/"
LEAGUE_URL = f"{FPL_API_URL}leagues-classic/{LEAGUE_ID}/standings/"
ENTRY_EVENT_URL = f"{FPL_API_URL}entry/{{entry_id}}/event/{{gameweek}}/picks/"
ELEMENT_SUMMARY_URL = f"{FPL_API_URL}element-summary/{{player_id}}/"

def get_credentials():
    creds_json_str = os.getenv("GCP_CREDENTIALS")
    if creds_json_str:
        print("Authenticating via GitHub Actions secret...")
        creds_json = json.loads(creds_json_str)
        return gspread.service_account_from_dict(creds_json)
    else:
        print("Authenticating via local credentials file...")
        return gspread.service_account(filename=CREDENTIALS_FILE)

def get_json_from_url(url):
    try:
        response = requests.get(url); response.raise_for_status(); return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}"); return None

def get_player_gameweek_stats(player_id, all_player_details_dict):
    """Returns a map of {gameweek: {points, minutes}} for a player from a dict."""
    player_data = all_player_details_dict.get(player_id)
    if player_data and 'history' in player_data:
        return {item['round']: {'points': item['total_points'], 'minutes': item['minutes']} for item in player_data['history']}
    return {}

def get_active_squad_ids(picks_data):
    if not picks_data or 'picks' not in picks_data: return []
    if picks_data.get('active_chip') == 'bboost':
        return [p['element'] for p in picks_data['picks']]
    active_squad_ids = {p['element'] for p in picks_data['picks'][:11]}
    for sub in picks_data.get('automatic_subs', []):
        active_squad_ids.discard(sub['element_out']); active_squad_ids.add(sub['element_in'])
    return list(active_squad_ids)

def main():
    print("--- Starting FPL Data Pipeline ---")
    gc = get_credentials()
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    print(f"Connected to Google Sheet: '{GOOGLE_SHEET_NAME}'")

    fpl_data = get_json_from_url(BOOTSTRAP_STATIC_URL)
    league_data = get_json_from_url(LEAGUE_URL)
    if not fpl_data or not league_data: print("Failed to fetch base data. Exiting."); return

    finished_gws = [gw['id'] for gw in fpl_data['events'] if gw['finished']]
    if not finished_gws: print("No gameweeks have finished yet. Exiting."); return
    last_finished_gw = max(finished_gws)
    print(f"Detected last finished gameweek as GW{last_finished_gw}")

    manager_df = pd.DataFrame(league_data['standings']['results'])[['entry', 'player_name', 'entry_name']].rename(
        columns={'entry': 'manager_id', 'player_name': 'manager_name', 'entry_name': 'team_name'}
    )
    elements_df = pd.DataFrame(fpl_data['elements'])

    print("Pre-fetching details for all players...")
    # *** VC BUG FIX: Store player details in a dictionary for reliable lookups ***
    all_player_details_dict = {
        pid: get_json_from_url(ELEMENT_SUMMARY_URL.format(player_id=pid)) for pid in elements_df['id']
    }
    print("Finished pre-fetching player details.")

    award_history_long = { "golden_boot": [], "playmaker": [], "golden_glove": [], "best_gk": [], "best_def": [], "best_mid": [], "best_fwd": [], "best_vc": [] }

    print(f"Fetching and processing historical data up to GW{last_finished_gw}...")
    for gw in range(1, last_finished_gw + 1):
        for _, manager in manager_df.iterrows():
            manager_id, manager_name = manager['manager_id'], manager['manager_name']
            picks_data = get_json_from_url(ENTRY_EVENT_URL.format(entry_id=manager_id, gameweek=gw))
            
            if picks_data and 'picks' in picks_data:
                active_squad = get_active_squad_ids(picks_data)
                squad_stats_df = elements_df[elements_df['id'].isin(active_squad)]
                
                award_history_long["golden_boot"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df['goals_scored'].sum()})
                award_history_long["playmaker"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df['assists'].sum()})
                award_history_long["golden_glove"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df[squad_stats_df['element_type'].isin([1,2,3])]['clean_sheets'].sum()})
                award_history_long["best_gk"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df[squad_stats_df['element_type'] == 1]['total_points'].sum()})
                award_history_long["best_def"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df[squad_stats_df['element_type'] == 2]['total_points'].sum()})
                award_history_long["best_mid"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df[squad_stats_df['element_type'] == 3]['total_points'].sum()})
                award_history_long["best_fwd"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df[squad_stats_df['element_type'] == 4]['total_points'].sum()})

                captain_id, vc_id = None, None
                for p in picks_data['picks']:
                    if p['is_captain']: captain_id = p['element']
                    if p['is_vice_captain']: vc_id = p['element']
                
                captain_stats = get_player_gameweek_stats(captain_id, all_player_details_dict)
                vc_stats = get_player_gameweek_stats(vc_id, all_player_details_dict)
                
                captain_minutes, vc_points = captain_stats.get(gw, {}).get('minutes', 0), 0
                if captain_minutes == 0 and vc_id is not None:
                    # Captain didn't play, VC is promoted. VC points for this award are 0.
                    vc_points = 0
                elif vc_id is not None:
                    # Captain played, VC points are their base score for the GW.
                    vc_points = vc_stats.get(gw, {}).get('points', 0)
                
                award_history_long["best_vc"].append({'gameweek': gw, 'manager_name': manager_name, 'score': vc_points})

        print(f"  Processed Gameweek {gw}/{last_finished_gw}")
        if gw < last_finished_gw: time.sleep(1)

    print("Pivoting data to final wide format and writing to Google Sheets...")
    worksheets_to_write = {}
    award_total_cols = { "golden_boot": "Total Goal Score", "playmaker": "Total Assists", "golden_glove": "Total Clean Sheets", "best_gk": "Total GK Points", "best_def": "Total DEF Points", "best_mid": "Total MID Points", "best_fwd": "Total FWD Points", "best_vc": "Total VC Points" }

    for award_name, history_data in award_history_long.items():
        long_df = pd.DataFrame(history_data)
        wide_df = long_df.pivot(index='manager_name', columns='gameweek', values='score').fillna(0).astype(int)
        wide_df.columns = [f"GW{col}" for col in wide_df.columns]
        
        total_col_name = award_total_cols[award_name]
        
        if award_name == 'best_vc':
            wide_df_cumulative = wide_df.cumsum(axis=1)
            wide_df[total_col_name] = wide_df_cumulative.get(f"GW{last_finished_gw}", 0)
        else:
            wide_df[total_col_name] = wide_df.get(f"GW{last_finished_gw}", 0)

        final_df = wide_df.reset_index().merge(manager_df[['manager_name', 'team_name']], on='manager_name')
        
        # *** RANKING FIX: Use competition ranking method ***
        final_df['Standings'] = final_df[total_col_name].rank(method='min', ascending=False).astype(int)
        final_df.sort_values(by='Standings', inplace=True)
        
        final_df.rename(columns={'team_name': 'Team', 'manager_name': 'Manager'}, inplace=True)
        
        gameweek_cols = [f"GW{i}" for i in range(1, last_finished_gw + 1)]
        column_order = ['Standings', 'Team', 'Manager', total_col_name] + gameweek_cols
        worksheets_to_write[award_name] = final_df[column_order]

    metadata_df = pd.DataFrame([{'last_finished_gw': last_finished_gw, 'last_updated_utc': datetime.utcnow().isoformat()}])
    worksheets_to_write["metadata"] = metadata_df

    for name, df in worksheets_to_write.items():
        try:
            worksheet = spreadsheet.worksheet(name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=name, rows=len(df) + 1, cols=len(df.columns) + 1)
        set_with_dataframe(worksheet, df, include_index=False)
        print(f"  Successfully wrote to '{name}' worksheet.")

    print("--- Pipeline finished successfully! ---")

if __name__ == "__main__":
    main()