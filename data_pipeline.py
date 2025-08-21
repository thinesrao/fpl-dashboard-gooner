# data_pipeline.py (FINAL v13 - Corrected Underdog Logic & All 20 Awards)
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import requests
import json
import time
from datetime import datetime, timezone
import os

# --- Configuration ---
CLASSIC_LEAGUE_ID = 665732
H2H_LEAGUE_ID = 818813
GOOGLE_SHEET_NAME = "FPL-Data-Pep"
CREDENTIALS_FILE = ".streamlit/google_credentials.json"

# --- API Endpoints ---
FPL_API_URL = "https://fantasy.premierleague.com/api/"
BOOTSTRAP_STATIC_URL = f"{FPL_API_URL}bootstrap-static/"
CLASSIC_LEAGUE_URL = f"{FPL_API_URL}leagues-classic/{CLASSIC_LEAGUE_ID}/standings/"
H2H_LEAGUE_URL = f"{FPL_API_URL}leagues-h2h/{H2H_LEAGUE_ID}/standings/"
ENTRY_HISTORY_URL = f"{FPL_API_URL}entry/{{TID}}/history/"
ENTRY_TRANSFERS_URL = f"{FPL_API_URL}entry/{{TID}}/transfers/"
ENTRY_PICKS_URL = f"{FPL_API_URL}entry/{{TID}}/event/{{GW}}/picks/"
LIVE_EVENT_URL = f"{FPL_API_URL}event/{{GW}}/live/"
ELEMENT_SUMMARY_URL = f"{FPL_API_URL}element-summary/{{EID}}/"

# --- Helper Functions ---
def get_credentials():
    creds_json_str = os.getenv("GCP_CREDENTIALS")
    if creds_json_str:
        print("Authenticating via GitHub Actions secret...")
        creds_json = json.loads(creds_json_str); return gspread.service_account_from_dict(creds_json)
    else:
        print("Authenticating via local credentials file..."); return gspread.service_account(filename=CREDENTIALS_FILE)

def get_json_from_url(url):
    try:
        response = requests.get(url, timeout=15); response.raise_for_status(); return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}"); return None

def get_active_squad_ids(picks_data):
    if not picks_data or 'picks' not in picks_data: return []
    if picks_data.get('active_chip') == 'bboost': return [p['element'] for p in picks_data['picks']]
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
    classic_league_data = get_json_from_url(CLASSIC_LEAGUE_URL)
    h2h_league_data = get_json_from_url(H2H_LEAGUE_URL)
    if not all([fpl_data, classic_league_data, h2h_league_data]): print("Failed to fetch base data. Exiting."); return

    finished_gws = [gw['id'] for gw in fpl_data['events'] if gw['finished']]
    if not finished_gws: print("No gameweeks have finished yet. Exiting."); return
    last_finished_gw = max(finished_gws)
    print(f"Detected last finished gameweek as GW{last_finished_gw}")

    manager_df = pd.DataFrame(classic_league_data['standings']['results'])[['entry', 'player_name', 'entry_name']].rename(
        columns={'entry': 'manager_id', 'player_name': 'manager_name', 'entry_name': 'team_name'}
    )
    elements_df = pd.DataFrame(fpl_data['elements'])
    
    print("Pre-fetching manager histories, transfers, and player details...")
    manager_histories = {row['manager_id']: get_json_from_url(ENTRY_HISTORY_URL.format(TID=row['manager_id'])) for _, row in manager_df.iterrows()}
    manager_transfers = {row['manager_id']: get_json_from_url(ENTRY_TRANSFERS_URL.format(TID=row['manager_id'])) for _, row in manager_df.iterrows()}
    player_details_dict = {pid: get_json_from_url(ELEMENT_SUMMARY_URL.format(EID=pid)) for pid in elements_df['id']}

    h2h_results_df = pd.DataFrame(h2h_league_data.get('matches', {}).get('results', []))
    
    long_format_data = {
        "golden_boot": [], "playmaker": [], "golden_glove": [], "best_gk": [], "best_def": [], "best_mid": [], "best_fwd": [], "best_vc": [],
        "transfer_king": [], "bench_king": [], "dream_team": [], "defensive_king": [], "shooting_stars": [], "best_underdog": [], "penalty_king": []
    }

    print(f"Processing all gameweeks up to GW{last_finished_gw}...")
    for gw in range(1, last_finished_gw + 1):
        live_gw_data = get_json_from_url(LIVE_EVENT_URL.format(GW=gw))
        if not live_gw_data: print(f"Could not fetch live data for GW{gw}. Skipping."); continue
        
        dream_team_players = {p['id'] for p in live_gw_data.get('elements', []) if p.get('stats', {}).get('in_dreamteam')}
        top_score = 0
        if dream_team_players: top_score = max(p['stats']['total_points'] for p in live_gw_data['elements'] if p['id'] in dream_team_players)
        top_performers = {p['id'] for p in live_gw_data['elements'] if p['id'] in dream_team_players and p['stats']['total_points'] == top_score}

        classic_standings = classic_league_data.get('standings', {}).get('results', [])
        h2h_standings = h2h_league_data.get('standings', {}).get('results', [])
        classic_ranks_prev = {s['entry']: s['rank'] for s in classic_standings} if gw == 1 else {s['entry']: s['last_rank'] for s in classic_standings}
        h2h_ranks_prev = {s['entry']: s['rank'] for s in h2h_standings} if gw == 1 else {s['entry']: s['last_rank'] for s in h2h_standings}

        for _, manager in manager_df.iterrows():
            manager_id, manager_name = manager['manager_id'], manager['manager_name']
            picks_data = get_json_from_url(ENTRY_PICKS_URL.format(TID=manager_id, GW=gw))
            
            if picks_data:
                active_squad_ids = get_active_squad_ids(picks_data)
                bench_squad_ids = [p['element'] for p in picks_data['picks'][11:]]
                squad_stats_df = elements_df[elements_df['id'].isin(active_squad_ids)]
                
                # Original Awards
                long_format_data["golden_boot"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df['goals_scored'].sum()})
                long_format_data["playmaker"].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df['assists'].sum()})
                for award, pos in [("best_gk", 1), ("best_def", 2), ("best_mid", 3), ("best_fwd", 4)]:
                    long_format_data[award].append({'gameweek': gw, 'manager_name': manager_name, 'score': squad_stats_df[squad_stats_df['element_type'] == pos]['total_points'].sum()})
                
                clean_sheets_gw = sum(next((p['stats'].get('clean_sheets', 0) for p in live_gw_data.get('elements', []) if p['id'] == p_id), 0) for p_id in active_squad_ids if elements_df[elements_df['id'] == p_id].iloc[0]['element_type'] in [1, 2, 3])
                long_format_data["golden_glove"].append({'gameweek': gw, 'manager_name': manager_name, 'score': clean_sheets_gw})

                captain_id = next((p['element'] for p in picks_data['picks'] if p['is_captain']), None)
                vc_id = next((p['element'] for p in picks_data['picks'] if p['is_vice_captain']), None)
                captain_minutes = player_details_dict.get(captain_id, {}).get('history', [])[gw-1].get('minutes', 0) if captain_id and len(player_details_dict.get(captain_id, {}).get('history', [])) >= gw else 0
                vc_points = 0
                if captain_minutes == 0 and vc_id: vc_points = 0
                elif vc_id: vc_points = player_details_dict.get(vc_id, {}).get('history', [])[gw-1].get('total_points', 0) if len(player_details_dict.get(vc_id, {}).get('history', [])) >= gw else 0
                long_format_data['best_vc'].append({'gameweek': gw, 'manager_name': manager_name, 'score': vc_points})

                # New Awards
                transfers_in_gw = [t for t in manager_transfers.get(manager_id, []) if t['event'] == gw]
                points_in = sum(player_details_dict.get(t['element_in'], {}).get('history', [])[gw-1].get('total_points', 0) for t in transfers_in_gw)
                points_out = sum(player_details_dict.get(t['element_out'], {}).get('history', [])[gw-1].get('total_points', 0) for t in transfers_in_gw)
                cost = manager_histories.get(manager_id, {}).get('current', [])[gw-1].get('event_transfers_cost', 0)
                long_format_data['transfer_king'].append({'gameweek': gw, 'manager_name': manager_name, 'score': points_in - points_out - cost})

                bench_points = sum(player_details_dict.get(pid, {}).get('history', [])[gw-1].get('total_points', 0) for pid in bench_squad_ids)
                long_format_data['bench_king'].append({'gameweek': gw, 'manager_name': manager_name, 'score': bench_points})
                
                dream_team_score = sum(4 if p_id in top_performers else 1 for p_id in active_squad_ids if p_id in dream_team_players)
                long_format_data['dream_team'].append({'gameweek': gw, 'manager_name': manager_name, 'score': dream_team_score})
                
                defensive_score = sum(next((p['stats'].get('defensive_contribution', 0) for p in live_gw_data.get('elements', []) if p['id'] == p_id), 0) for p_id in active_squad_ids)
                long_format_data['defensive_king'].append({'gameweek': gw, 'manager_name': manager_name, 'score': defensive_score})
                
                history = manager_histories.get(manager_id, {}).get('current', [])
                rank_rise = 0
                if gw > 1 and len(history) >= gw:
                    rank_now, rank_prev = history[gw-1].get('overall_rank', 0), history[gw-2].get('overall_rank', 0)
                    if rank_prev and rank_now: rank_rise = max(0, rank_prev - rank_now)
                long_format_data['shooting_stars'].append({'gameweek': gw, 'manager_name': manager_name, 'score': rank_rise})
                
                penalties_saved_score = squad_stats_df['penalties_saved'].sum() * 3
                penalties_missed_score = squad_stats_df['penalties_missed'].sum() * -2
                long_format_data['penalty_king'].append({'gameweek': gw, 'manager_name': manager_name, 'score': penalties_saved_score + penalties_missed_score})

                # --- Best Underdog: CORRECTED & ROBUST LOGIC HIERARCHY ---
                underdog_score = 0
                if gw > 1:
                    match = h2h_results_df[((h2h_results_df['event'] == gw) & (h2h_results_df['entry_1_entry'] == manager_id)) | ((h2h_results_df['event'] == gw) & (h2h_results_df['entry_2_entry'] == manager_id))]
                    if not match.empty:
                        match = match.iloc[0]
                        opponent_id = None
                        if match['entry_1_entry'] == manager_id and match['entry_1_points'] > match['entry_2_points']: opponent_id = match['entry_2_entry']
                        elif match['entry_2_entry'] == manager_id and match['entry_2_points'] > match['entry_1_points']: opponent_id = match['entry_1_entry']
                        
                        if opponent_id:
                            opp_classic_rank, opp_h2h_rank = classic_ranks_prev.get(opponent_id, 999), h2h_ranks_prev.get(opponent_id, 999)
                            
                            is_classic_leader = (opp_classic_rank == 1)
                            is_h2h_leader = (opp_h2h_rank == 1)
                            is_classic_top4 = (2 <= opp_classic_rank <= 4)
                            is_h2h_top4 = (2 <= opp_h2h_rank <= 4)

                            if is_classic_leader and is_h2h_leader:
                                underdog_score = 3
                            elif is_classic_leader or is_h2h_leader:
                                underdog_score = 2
                            elif is_classic_top4 or is_h2h_top4:
                                underdog_score = 1
                long_format_data['best_underdog'].append({'gameweek': gw, 'manager_name': manager_name, 'score': underdog_score})
        print(f"  Processed Gameweek {gw}/{last_finished_gw}")

    # --- Post-Loop Calculations ---
    print("Calculating final award standings...")
    worksheets_to_write = {}
    
    # Process historical awards
    for award_name, history_data in long_format_data.items():
        if not history_data: continue
        long_df = pd.DataFrame(history_data)
        wide_df = long_df.pivot(index='manager_name', columns='gameweek', values='score').fillna(0).astype(int)
        wide_df.columns = [f"GW{col}" for col in wide_df.columns]
        
        if award_name in ["transfer_king", "bench_king", "shooting_stars", "best_vc", "dream_team", "defensive_king", "best_underdog", "penalty_king", "golden_glove"]:
            total = wide_df.sum(axis=1)
        else:
            total = wide_df.get(f"GW{last_finished_gw}", 0)
        wide_df['Total'] = total
        
        final_df = wide_df.reset_index().merge(manager_df[['manager_name', 'team_name']], on='manager_name')
        final_df['Standings'] = final_df['Total'].rank(method='min', ascending=False).astype(int)
        final_df.sort_values(by=['Standings', 'manager_name'], inplace=True)
        final_df.rename(columns={'team_name': 'Team', 'manager_name': 'Manager'}, inplace=True)
        
        gameweek_cols = [f"GW{i}" for i in range(1, last_finished_gw + 1)]
        column_order = ['Standings', 'Team', 'Manager', 'Total'] + gameweek_cols
        worksheets_to_write[award_name] = final_df[column_order]

    # Process single-value awards
    single_value_awards = {"steady_king": [], "highest_gw_score": [], "freehit_king": [], "benchboost_king": [], "triplecaptain_king": []}
    for _, manager in manager_df.iterrows():
        manager_id, manager_name, team_name = manager['manager_id'], manager['manager_name'], manager['team_name']
        history = manager_histories.get(manager_id, {})
        transfers = manager_transfers.get(manager_id, [])
        
        chip_weeks = {c['event'] for c in history.get('chips', [])} if history else set()
        total_transfers = len([t for t in transfers if t['event'] not in chip_weeks])
        total_points = history.get('current', [])[-1].get('total_points', 0) if history.get('current') else 0
        single_value_awards['steady_king'].append({'Manager': manager_name, 'Team': team_name, 'Score': total_points / total_transfers if total_transfers > 0 else 0})

        fh_scores, bb_scores, tc_scores, normal_scores = [], [], [], []
        if history and 'current' in history:
            for gw_data in history.get('current', []):
                chip_played = next((c['name'] for c in history.get('chips', []) if c['event'] == gw_data['event']), None)
                score = gw_data['points'] - gw_data['event_transfers_cost']
                if chip_played == 'freehit': fh_scores.append(score)
                elif chip_played == 'bboost': bb_scores.append(score)
                elif chip_played == '3xc': tc_scores.append(score)
                elif not chip_played: normal_scores.append(score)
        
        single_value_awards['freehit_king'].append({'Manager': manager_name, 'Team': team_name, 'Score': sum(fh_scores)})
        single_value_awards['benchboost_king'].append({'Manager': manager_name, 'Team': team_name, 'Score': sum(bb_scores)})
        single_value_awards['triplecaptain_king'].append({'Manager': manager_name, 'Team': team_name, 'Score': sum(tc_scores)})
        single_value_awards['highest_gw_score'].append({'Manager': manager_name, 'Team': team_name, 'Score': max(normal_scores) if normal_scores else 0})

    for award_name, data in single_value_awards.items():
        if not data: continue
        df = pd.DataFrame(data)
        if 'Score' in df.columns:
             df = df.sort_values(by='Score', ascending=False).reset_index(drop=True)
             df['Standings'] = df['Score'].rank(method='min', ascending=False).astype(int)
             worksheets_to_write[award_name] = df[['Standings', 'Team', 'Manager', 'Score']]

    metadata_df = pd.DataFrame([{'last_finished_gw': last_finished_gw, 'last_updated_utc': datetime.now(timezone.utc).isoformat()}])
    worksheets_to_write["metadata"] = metadata_df

    print("Writing all processed data to Google Sheets...")
    for name, df in worksheets_to_write.items():
        try:
            worksheet = spreadsheet.worksheet(name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=name, rows=len(df) + 1, cols=len(df.columns) + 1)
        set_with_dataframe(worksheet, df, include_index=False)
        print(f"  Successfully wrote to '{name}' worksheet.")
        print("  Pausing for 3 seconds to respect API rate limits...")
        time.sleep(3)

    print("--- Pipeline finished successfully! ---")

if __name__ == "__main__":
    main()