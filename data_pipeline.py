# data_pipeline.py (FINAL v15.3 - Corrected Final KeyError)
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
FPL_CHALLENGE_LEAGUE_ID = 5008
GOOGLE_SHEET_NAME = "FPL-Data-Pep"

# --- API Endpoints ---
FPL_API_URL = "https://fantasy.premierleague.com/api/"
FPL_CHALLENGE_API_URL = "https://fplchallenge.premierleague.com/api/"
BOOTSTRAP_STATIC_URL = f"{FPL_API_URL}bootstrap-static/"
CLASSIC_LEAGUE_URL = f"{FPL_API_URL}leagues-classic/{CLASSIC_LEAGUE_ID}/standings/"
H2H_LEAGUE_URL = f"{FPL_API_URL}leagues-h2h/{H2H_LEAGUE_ID}/standings/"
FPL_CHALLENGE_LEAGUE_URL = f"{FPL_CHALLENGE_API_URL}leagues-classic/{FPL_CHALLENGE_LEAGUE_ID}/standings/"
CUP_STATUS_URL = f"{FPL_API_URL}leagues-classic/{CLASSIC_LEAGUE_ID}/cup-status/"
ENTRY_HISTORY_URL = f"{FPL_API_URL}entry/{{TID}}/history/"
ENTRY_TRANSFERS_URL = f"{FPL_API_URL}entry/{{TID}}/transfers/"
ENTRY_PICKS_URL = f"{FPL_API_URL}entry/{{TID}}/event/{{GW}}/picks/"
LIVE_EVENT_URL = f"{FPL_API_URL}event/{{GW}}/live/"
ELEMENT_SUMMARY_URL = f"{FPL_API_URL}element-summary/{{EID}}/"
H2H_MATCHES_URL = f"{FPL_API_URL}leagues-h2h-matches/league/{H2H_LEAGUE_ID}/"

# --- Helper Functions ---
def get_secrets():
    """Loads secrets from environment variables or a local secrets.toml file."""
    # Try to load from Streamlit secrets (for deployed app, although not used here) or GitHub Actions env vars
    gcp_creds = os.getenv("GCP_CREDENTIALS")
    
    # If not found, fall back to local secrets.toml file
    if not gcp_creds:
        try:
            import toml
            secrets = toml.load(".streamlit/secrets.toml")
            gcp_creds = secrets.get("gcp_service_account")
        except (FileNotFoundError, ImportError):
            print("Warning: .streamlit/secrets.toml not found. Relying solely on environment variables.")
            gcp_creds = None
            
    # The GCP credentials can be a string (from env var) or dict (from toml)
    if isinstance(gcp_creds, str):
        gcp_creds = json.loads(gcp_creds)
        
    return gcp_creds

def get_credentials(gcp_creds_dict):
    if gcp_creds_dict:
        print("Authenticating via GCP credentials...")
        return gspread.service_account_from_dict(gcp_creds_dict)
    else:
        print("ERROR: GCP credentials not found in secrets.toml or environment variables.")
        return None
    
def get_json_from_url(url, headers=None):
    """Generic function to get JSON from a URL, now with header support."""
    try:
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None
    
def get_active_squad_ids(picks_data):
    if not picks_data or 'picks' not in picks_data: return []
    if picks_data.get('active_chip') == 'bboost': return [p['element'] for p in picks_data['picks']]
    active_squad_ids = {p['element'] for p in picks_data['picks'][:11]}
    for sub in picks_data.get('automatic_subs', []):
        active_squad_ids.discard(sub['element_out']); active_squad_ids.add(sub['element_in'])
    return list(active_squad_ids)

def get_gameweek_to_month_map(fpl_data):
    gw_map = {}
    for gw_info in fpl_data['events']:
        deadline = datetime.fromisoformat(gw_info['deadline_time'].replace('Z', '+00:00'))
        gw_map[gw_info['id']] = deadline.strftime('%B')
    return gw_map

def get_gw_score_from_history(history, gw):
    """Safely gets a score from a player's history list for a specific gameweek."""
    if not history:
        return 0
    return next((item.get('total_points', 0) for item in history if item.get('round') == gw), 0)


def main():
    print("--- Starting FPL Data Pipeline ---")
    # --- THIS IS THE CORRECTED LOGIC BLOCK ---
    gcp_creds = get_secrets()
    if not gcp_creds:
        return # Exit if no credentials found
        
    gc = get_credentials(gcp_creds)
    if not gc:
        return # Exit if authentication fails

    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    print(f"Connected to Google Sheet: '{GOOGLE_SHEET_NAME}'")
    
    # --- END OF CORRECTED LOGIC BLOCK ---
    

    fpl_data = get_json_from_url(BOOTSTRAP_STATIC_URL)
    classic_league_data = get_json_from_url(CLASSIC_LEAGUE_URL)
    h2h_league_data = get_json_from_url(H2H_LEAGUE_URL)
    h2h_matches_data = get_json_from_url(H2H_MATCHES_URL)
    if not all([fpl_data, classic_league_data, h2h_league_data, h2h_matches_data]): print("Failed to fetch base data. Exiting."); return

    finished_gws = [gw['id'] for gw in fpl_data['events'] if gw['finished']]
    if not finished_gws: print("No gameweeks have finished yet. Exiting."); return
    last_finished_gw = max(finished_gws)
    print(f"Detected last finished gameweek as GW{last_finished_gw}")

    gw_month_map = get_gameweek_to_month_map(fpl_data)
    manager_df = pd.DataFrame(classic_league_data['standings']['results'])[['entry', 'player_name', 'entry_name']].rename(
        columns={'entry': 'manager_id', 'player_name': 'manager_name', 'entry_name': 'team_name'}
    )
    elements_df = pd.DataFrame(fpl_data['elements'])
    
    player_id_to_type_map = elements_df.set_index('id')['element_type'].to_dict()
    
    print("Pre-fetching manager histories, transfers, and player details...")
    manager_histories = {row['manager_id']: get_json_from_url(ENTRY_HISTORY_URL.format(TID=row['manager_id'])) for _, row in manager_df.iterrows()}
    manager_transfers = {row['manager_id']: get_json_from_url(ENTRY_TRANSFERS_URL.format(TID=row['manager_id'])) for _, row in manager_df.iterrows()}
    player_details_dict = {pid: get_json_from_url(ELEMENT_SUMMARY_URL.format(EID=pid)) for pid in elements_df['id']}

    # --- THE DEFINITIVE TIME MACHINE (based on your superior logic) ---
    print("Loading historical rank 'Time Machine' from Google Sheet...")
    try:
        time_machine_sheet = spreadsheet.worksheet("_time_machine_ranks")
        time_machine_df = pd.DataFrame(time_machine_sheet.get_all_records())
    except gspread.WorksheetNotFound:
        print("  '_time_machine_ranks' not found. Will be created at the end of this run.")
        time_machine_df = pd.DataFrame(columns=['gameweek', 'manager_id', 'manager_name', 'classic_rank', 'h2h_rank'])
    
    # --- Read the manual penalty data and create player name map ---
    print("Fetching manual penalty data...")
    try:
        manual_penalty_sheet = spreadsheet.worksheet('manual_penalty_data')
        manual_penalty_df = pd.DataFrame(manual_penalty_sheet.get_all_records())
        if not manual_penalty_df.empty:
            # Ensure Gameweek column is numeric for safe comparison
            manual_penalty_df['Gameweek'] = pd.to_numeric(manual_penalty_df['Gameweek'], errors='coerce').dropna()
    except gspread.WorksheetNotFound:
        print("Warning: 'manual_penalty_data' sheet not found. Penalty King award will be zero.")
        manual_penalty_df = pd.DataFrame(columns=['Gameweek', 'Player_Name', 'Event_Type'])

    # --- THIS IS THE CRITICAL MISSING LINE ---
    # Create the 'phonebook' to map player web names to their FPL ID
    player_name_to_id = elements_df.set_index('web_name')['id'].to_dict()
          
    long_format_data = {
        "golden_boot": [], "playmaker": [], "golden_glove": [], "best_gk": [], "best_def": [], "best_mid": [], "best_fwd": [], "best_vc": [],
        "transfer_king": [], "bench_king": [], "dream_team": [], "defensive_king": [], "shooting_stars": [], "best_underdog": [], "penalty_king": []
    }
    fpl_challenge_gw_scores = []
    
    print(f"Processing all gameweeks up to GW{last_finished_gw}...")
    for gw in range(1, last_finished_gw + 1):
        live_gw_data = get_json_from_url(LIVE_EVENT_URL.format(GW=gw))
        if not live_gw_data: print(f"Could not fetch live data for GW{gw}. Skipping."); continue
        
        # Identify Dream Team players and top performers                                                
        dream_team_players = {p['id'] for p in live_gw_data.get('elements', []) if p.get('stats', {}).get('in_dreamteam')}
        top_score = 0
        if dream_team_players: top_score = max(p['stats']['total_points'] for p in live_gw_data['elements'] if p['id'] in dream_team_players)
        top_performers = {p['id'] for p in live_gw_data['elements'] if p['id'] in dream_team_players and p['stats']['total_points'] == top_score}

        classic_standings_results = classic_league_data.get('standings', {}).get('results', [])
        h2h_standings_results = h2h_league_data.get('standings', {}).get('results', [])
        classic_ranks_prev = {s['entry']: s['rank'] for s in classic_standings_results} if gw == 1 else {s['entry']: s['last_rank'] for s in classic_standings_results}
        h2h_ranks_prev = {s['entry']: s['rank'] for s in h2h_standings_results} if gw == 1 else {s['entry']: s['last_rank'] for s in h2h_standings_results}

        for _, manager in manager_df.iterrows():
            manager_id, manager_name = manager['manager_id'], manager['manager_name']
            picks_data = get_json_from_url(ENTRY_PICKS_URL.format(TID=manager_id, GW=gw))
            
            if picks_data:
                active_squad_ids = get_active_squad_ids(picks_data)
                bench_squad_ids = [p['element'] for p in picks_data['picks'][11:]]
                squad_stats_df = elements_df[elements_df['id'].isin(active_squad_ids)]
                
                # --- Golden Boot: CORRECTED & ROBUST GW-by-GW LOGIC ---
                goals_scored_gw = sum(
                    next(
                        (p['stats'].get('goals_scored', 0) for p in live_gw_data.get('elements', []) if p['id'] == player_id), 0
                    ) 
                    for player_id in active_squad_ids
                )
                long_format_data["golden_boot"].append({'gameweek': gw, 'manager_name': manager_name, 'score': goals_scored_gw})
                # --- Playmaker: CORRECTED & ROBUST GW-by-GW LOGIC ---
                assists_gw = sum(
                    next(
                        (p['stats'].get('assists', 0) for p in live_gw_data.get('elements', []) if p['id'] == player_id), 0
                    ) 
                    for player_id in active_squad_ids
                )
                long_format_data["playmaker"].append({'gameweek': gw, 'manager_name': manager_name, 'score': assists_gw})
                
                # --- Best GK/Def/Mid/Fwd: CORRECTED & ROBUST GW-by-GW LOGIC ---                
                # --- Best Positional Awards: CORRECTED & ROBUST GW-by-GW LOGIC ---
                gk_score, def_score, mid_score, fwd_score = 0, 0, 0, 0
                for player_id in active_squad_ids:
                    # Get live stats for this player this gameweek
                    live_player_stats = next((p['stats'] for p in live_gw_data.get('elements', []) if p['id'] == player_id), None)
                    if live_player_stats:
                        player_score = live_player_stats.get('total_points', 0)
                        player_pos = player_id_to_type_map.get(player_id)
                        
                        if player_pos == 1: # Goalkeeper
                            gk_score += player_score
                        elif player_pos == 2: # Defender
                            def_score += player_score
                        elif player_pos == 3: # Midfielder
                            mid_score += player_score
                        elif player_pos == 4: # Forward
                            fwd_score += player_score
                
                long_format_data["best_gk"].append({'gameweek': gw, 'manager_name': manager_name, 'score': gk_score})
                long_format_data["best_def"].append({'gameweek': gw, 'manager_name': manager_name, 'score': def_score})
                long_format_data["best_mid"].append({'gameweek': gw, 'manager_name': manager_name, 'score': mid_score})
                long_format_data["best_fwd"].append({'gameweek': gw, 'manager_name': manager_name, 'score': fwd_score})
                                
                clean_sheets_gw = sum(next((p['stats'].get('clean_sheets', 0) for p in live_gw_data.get('elements', []) if p['id'] == p_id), 0) for p_id in active_squad_ids if elements_df[elements_df['id'] == p_id].iloc[0]['element_type'] in [1, 2, 3])
                long_format_data["golden_glove"].append({'gameweek': gw, 'manager_name': manager_name, 'score': clean_sheets_gw})

                # --- Best Vice-Captain (Corrected Logic) ---
                vc_points = 0
                # Find the Vice-Captain's player ID for the gameweek
                vc_id = next((p['element'] for p in picks_data['picks'] if p['is_vice_captain']), None)
                
                # If a Vice-Captain was chosen, get their normal, single FPL points for that gameweek
                if vc_id:
                    vc_history = player_details_dict.get(vc_id, {}).get('history', [])
                    # Safely get the score for the current gameweek
                    vc_points = get_gw_score_from_history(vc_history, gw)

                long_format_data['best_vc'].append({'gameweek': gw, 'manager_name': manager_name, 'score': vc_points})
                
                # --- Transfer King (with Wildcard / Free Hit exclusion) ---
                transfer_score_gw = 0
                history_data = manager_histories.get(manager_id, {})
                
                # Find the chip played in the current gameweek, if any
                chip_played_this_gw = next((chip['name'] for chip in history_data.get('chips', []) if chip['event'] == gw), None)
                
                # Only calculate score if Wildcard or Free Hit was NOT played
                if chip_played_this_gw not in ['wildcard', 'freehit']:
                    transfers_in_gw = [t for t in manager_transfers.get(manager_id, []) if t['event'] == gw]
                    if transfers_in_gw:
                        points_in = sum(get_gw_score_from_history(player_details_dict.get(t['element_in'], {}).get('history', []), gw) for t in transfers_in_gw)
                        points_out = sum(get_gw_score_from_history(player_details_dict.get(t['element_out'], {}).get('history', []), gw) for t in transfers_in_gw)
                        cost = next((h.get('event_transfers_cost', 0) for h in history_data.get('current', []) if h.get('event') == gw), 0)
                        transfer_score_gw = points_in - points_out - cost

                long_format_data['transfer_king'].append({'gameweek': gw, 'manager_name': manager_name, 'score': transfer_score_gw})
                
                # --- Bench King: CORRECTED LOGIC ---
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
                
                # --- Penalty King: DEFINITIVE HYBRID LOGIC (GW-by-GW) ---
                penalty_score_gw = 0
                
                # Part 1: Process Automated Penalty Saves from LIVE gameweek data
                for player_id in active_squad_ids:
                    live_player_stats = next((p['stats'] for p in live_gw_data.get('elements', []) if p['id'] == player_id), None)
                    if live_player_stats:
                        penalty_score_gw += live_player_stats.get('penalties_saved', 0) * 3
                
                # Part 2: Process Manual Inputs for Scored & Won
                gw_penalty_events = manual_penalty_df[manual_penalty_df['Gameweek'] == gw]
                if not gw_penalty_events.empty:
                    for _, event in gw_penalty_events.iterrows():
                        player_name = event['Player_Name']
                        event_type = event['Event_Type']
                        player_id = player_name_to_id.get(player_name)
                        
                        if player_id and player_id in active_squad_ids:
                            if event_type == 'Penalty Scored':
                                penalty_score_gw += 1
                            elif event_type == 'Penalty Won':
                                penalty_score_gw += 1
                
                long_format_data['penalty_king'].append({'gameweek': gw, 'manager_name': manager_name, 'score': penalty_score_gw})
                
                # --- Best Underdog (Definitive Self-Sufficient Logic) ---
                underdog_score = 0
                if gw > 1 and not time_machine_df.empty:
                    h2h_matches_df = pd.DataFrame(h2h_matches_data.get('results', []))
                    match = h2h_matches_df[((h2h_matches_df['event'] == gw) & (h2h_matches_df['entry_1_entry'] == manager_id)) | ((h2h_matches_df['event'] == gw) & (h2h_matches_df['entry_2_entry'] == manager_id))]
                    
                    if not match.empty:
                        match = match.iloc[0]
                        opponent_id = None
                        
                        if match['entry_1_entry'] == manager_id and match['entry_1_points'] > match['entry_2_points']:
                            opponent_id = match['entry_2_entry']
                        elif match['entry_2_entry'] == manager_id and match['entry_2_points'] > match['entry_1_points']:
                            opponent_id = match['entry_1_entry']
                        
                        if opponent_id:
                            # Use the 'Time Machine' to get the opponent's rank from the PREVIOUS gameweek
                            prev_gw_ranks = time_machine_df[time_machine_df['gameweek'] == gw - 1]
                            opponent_ranks = prev_gw_ranks[prev_gw_ranks['manager_id'] == opponent_id]
                            
                            if not opponent_ranks.empty:
                                opp_classic_rank_prev = opponent_ranks.iloc[0]['classic_rank']
                                opp_h2h_rank_prev = opponent_ranks.iloc[0]['h2h_rank']
                                
                                # Apply the scoring hierarchy correctly
                                if (opp_classic_rank_prev == 1 and opp_h2h_rank_prev == 1):
                                    underdog_score = 3
                                elif (opp_classic_rank_prev == 1 or opp_h2h_rank_prev == 1):
                                    underdog_score = 2
                                elif (2 <= opp_classic_rank_prev <= 4 or 2 <= opp_h2h_rank_prev <= 4):
                                    underdog_score = 1
                                    
                long_format_data['best_underdog'].append({'gameweek': gw, 'manager_name': manager_name, 'score': underdog_score})
                                        
        print(f"  Processed Gameweek {gw}/{last_finished_gw}")
        if gw < last_finished_gw: time.sleep(1)

    print("Calculating final award standings...")
    worksheets_to_write = {}
    
    # Process special historical awards
    for award_name, history_data in long_format_data.items():
        if not history_data: continue
        long_df = pd.DataFrame(history_data)
        wide_df = long_df.pivot(index='manager_name', columns='gameweek', values='score').fillna(0).astype(int)
        wide_df.columns = [f"GW{col}" for col in wide_df.columns]
        
        # --- THIS IS THE DEFINITIVE FIX ---
        # All historical awards should have their gameweek scores summed up for the total.
        # The logic has been simplified to correctly calculate the sum for ALL historical awards.
        wide_df['Total'] = wide_df[[col for col in wide_df.columns if col.startswith('GW')]].sum(axis=1)
                
        final_df = wide_df.reset_index().merge(manager_df[['manager_name', 'team_name']], on='manager_name')
        final_df['Standings'] = final_df['Total'].rank(method='min', ascending=False).astype(int)
        final_df.sort_values(by=['Standings', 'manager_name'], inplace=True)
        final_df.rename(columns={'team_name': 'Team', 'manager_name': 'Manager'}, inplace=True)
        
        gameweek_cols = [f"GW{i}" for i in range(1, last_finished_gw + 1)]
        column_order = ['Standings', 'Team', 'Manager', 'Total'] + [col for col in gameweek_cols if col in final_df.columns]
        worksheets_to_write[award_name] = final_df[column_order]

    # Process single-value special awards
    single_value_awards = {"steady_king": [], "highest_gw_score": [], "freehit_king": [], "benchboost_king": [], "triplecaptain_king": []}
    for _, manager in manager_df.iterrows():
        manager_id, manager_name, team_name = manager['manager_id'], manager['manager_name'], manager['team_name']
        history = manager_histories.get(manager_id, {})
        transfers = manager_transfers.get(manager_id, [])
        
        chip_weeks = {c['event'] for c in history.get('chips', [])} if history else set()
        total_transfers = len([t for t in transfers if t['event'] not in chip_weeks])
        total_points = history.get('current', [])[-1].get('total_points', 0) if history and history.get('current') else 0
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

    # Process Standard Awards
    # --- Corrected Gameweek Score Calculation (with transfer costs) ---
    gw_scores_list = []
    for m_id, hist in manager_histories.items():
        if hist and 'current' in hist:
            for h in hist['current']:
                # The corrected calculation: points - event_transfers_cost
                true_gw_score = h.get('points', 0) - h.get('event_transfers_cost', 0)
                gw_scores_list.append({'manager_id': m_id, 'gameweek': h['event'], 'score': true_gw_score})
    
    gw_scores_df = pd.DataFrame(gw_scores_list)
    gw_scores_wide = gw_scores_df.pivot(index='manager_id', columns='gameweek', values='score').fillna(0).astype(int)
    gw_scores_wide.columns = [f"GW{col}" for col in gw_scores_wide.columns]
        
    classic_standings_df = pd.DataFrame(classic_league_data['standings']['results'])[['rank', 'entry_name', 'player_name', 'total', 'entry']]
    classic_standings_df.rename(columns={'rank': 'Standings', 'entry_name': 'Team', 'player_name': 'Manager', 'total': 'Total', 'entry': 'manager_id'}, inplace=True)
    classic_standings_df = classic_standings_df.merge(gw_scores_wide, on='manager_id', how='left').drop(columns=['manager_id'])
    worksheets_to_write["classic_league_standings"] = classic_standings_df
    
    h2h_standings_results = h2h_league_data.get('standings',{}).get('results',[])
    h2h_standings_df = pd.DataFrame(h2h_standings_results)[['rank', 'entry_name', 'player_name', 'total', 'points_for', 'entry']]
    h2h_standings_df.rename(columns={'rank': 'Standings', 'entry_name': 'Team', 'player_name': 'Manager', 'total': 'Total H2H Point', 'points_for': 'Total FPL Point', 'entry': 'manager_id'}, inplace=True)
    h2h_standings_df = h2h_standings_df.merge(gw_scores_wide, on='manager_id', how='left').drop(columns=['manager_id'])
    worksheets_to_write["h2h_league_standings"] = h2h_standings_df
    
    # --- FPL Challenge Weekly Manager (as per your definitive guide) ---
    print("Processing FPL Challenge awards...")
    fpl_challenge_log = []
    
    # Step 1: Get all managers from the Challenge league
    challenge_league_data = get_json_from_url(FPL_CHALLENGE_LEAGUE_URL)
    if challenge_league_data and 'standings' in challenge_league_data:
        challenge_manager_df = pd.DataFrame(challenge_league_data['standings']['results'])[['entry', 'player_name', 'entry_name']].rename(
            columns={'entry': 'manager_id', 'player_name': 'manager_name', 'entry_name': 'team_name'}
        )

        # Step 2: Get the gameweek history for each manager
        print("Pre-fetching FPL Challenge manager histories...")
        challenge_manager_histories = {
            row['manager_id']: get_json_from_url(f"https://fplchallenge.premierleague.com/api/entry/{row['manager_id']}/history/") 
            for _, row in challenge_manager_df.iterrows()
        }

        # Step 3: Combine and calculate the weekly standings
        for gw in range(1, last_finished_gw + 1):
            gw_scores = []
            for manager_id, history in challenge_manager_histories.items():
                if history and 'current' in history:
                    # Find the specific gameweek's score from the history list
                    gw_history_data = next((item for item in history['current'] if item['event'] == gw), None)
                    if gw_history_data:
                        gw_scores.append({'manager_id': manager_id, 'score': gw_history_data['points']})
            
            if gw_scores:
                gw_scores_df = pd.DataFrame(gw_scores)
                # Find the top score for the week
                max_score = gw_scores_df['score'].max()
                # Find all managers who achieved that score (handles ties)
                winners = gw_scores_df[gw_scores_df['score'] == max_score]
                winners = winners.merge(challenge_manager_df, on='manager_id')

                # Aggregate winners in case of a tie
                winner_teams = ', '.join(winners['team_name'])
                winner_managers = ', '.join(winners['manager_name'])
                
                fpl_challenge_log.append({
                    'Gameweek': gw, 'Team': winner_teams, 'Manager': winner_managers, 'Score': max_score
                })

        if fpl_challenge_log:
            worksheets_to_write["fpl_challenge_weekly_log"] = pd.DataFrame(fpl_challenge_log)

    # --- Classic Weekly Manager (as per your definitive guide) ---        
    gw_scores_no_chips = []
    for m_id, hist in manager_histories.items():
        if not hist: continue
        chip_weeks = {c['event'] for c in hist.get('chips', [])}
        for gw_data in hist.get('current', []):
            if gw_data['event'] not in chip_weeks:
               # --- THIS IS THE CORRECTED LINE ---
                gw_scores_no_chips.append({'gameweek': gw_data['event'], 'manager_id': m_id, 'score': gw_data['points']})
    weekly_winners_log_df = pd.DataFrame(gw_scores_no_chips)
    if not weekly_winners_log_df.empty:
        max_scores = weekly_winners_log_df.groupby('gameweek')['score'].max().reset_index()
        weekly_winners = pd.merge(weekly_winners_log_df, max_scores, on=['gameweek', 'score'])
        weekly_winners = weekly_winners.merge(manager_df, on='manager_id')
        weekly_winners_final_df = weekly_winners.groupby('gameweek').agg(Team=('team_name', lambda x: ', '.join(x)), Manager=('manager_name', lambda x: ', '.join(x)), Score=('score', 'first')).reset_index()
        # --- THIS IS THE FIX ---
        weekly_winners_final_df.rename(columns={'gameweek': 'Gameweek'}, inplace=True)
        worksheets_to_write["weekly_manager_log"] = weekly_winners_final_df    
    # --- Corrected Monthly Score Calculation (with transfer costs) ---
    all_gw_scores_list = []
    for manager_id, history in manager_histories.items():
        if history and 'current' in history:
            for gw_data in history['current']:
                # The corrected calculation: points - event_transfers_cost
                true_gw_score = gw_data.get('points', 0) - gw_data.get('event_transfers_cost', 0)
                all_gw_scores_list.append({'gameweek': gw_data['event'], 'manager_id': manager_id, 'score': true_gw_score})
    
    all_gw_scores_df = pd.DataFrame(all_gw_scores_list)
    all_gw_scores_df['month'] = all_gw_scores_df['gameweek'].map(gw_month_map)
    
    if not all_gw_scores_df.empty:
        classic_monthly_scores = all_gw_scores_df.groupby(['month', 'manager_id'])['score'].sum().reset_index()
        for month_name in classic_monthly_scores['month'].unique():
            month_df = classic_monthly_scores[classic_monthly_scores['month'] == month_name].copy()
            gws_in_month = all_gw_scores_df[all_gw_scores_df['month'] == month_name]['gameweek'].unique()
            
            # Merge to get manager names and team names
            month_df = month_df.merge(manager_df, on='manager_id')
            
            # Pivot GW scores for this month
            gw_scores_month = all_gw_scores_df[all_gw_scores_df['gameweek'].isin(gws_in_month)]
            gw_scores_pivot = gw_scores_month.pivot(index='manager_id', columns='gameweek', values='score').fillna(0).astype(int)
            gw_scores_pivot.columns = [f"GW{col}" for col in gw_scores_pivot.columns]
            
            # Combine everything
            final_month_df = month_df.merge(gw_scores_pivot, on='manager_id')
            final_month_df.rename(columns={'score': 'Total Monthly Points', 'team_name': 'Team', 'manager_name': 'Manager'}, inplace=True)
            final_month_df['Standings'] = final_month_df['Total Monthly Points'].rank(method='min', ascending=False).astype(int)
            final_month_df.sort_values(by='Standings', inplace=True)
            
            gw_cols = sorted([f"GW{gw}" for gw in gws_in_month])
            column_order = ['Standings', 'Team', 'Manager', 'Total Monthly Points'] + gw_cols
            worksheets_to_write[f"classic_monthly_{month_name}"] = final_month_df[column_order]

    # --- H2H Monthly Manager (as per screenshot) ---
    h2h_matches_df = pd.DataFrame(h2h_matches_data.get('results', []))
    if not h2h_matches_df.empty:
        # Define the official FPL monthly gameweek ranges
        FPL_MONTH_MAP = {
            "August": list(range(1, 4)), "September": list(range(4, 7)),
            "October": list(range(7, 10)), "November": list(range(10, 14)),
            "December": list(range(14, 20)), "January": list(range(20, 25)),
            "February": list(range(25, 29)), "March": list(range(29, 32)),
            "April": list(range(32, 35)), "May": list(range(35, 39)),
        }
        
        # Create a historical log of H2H standings at the end of each GW
        h2h_history = []
        for gw in range(1, last_finished_gw + 1):
            # We must re-fetch H2H standings for each GW to get historical ranks
            gw_h2h_data = get_json_from_url(f"{FPL_API_URL}leagues-h2h/{H2H_LEAGUE_ID}/standings/?page_standings=1&event={gw}")
            if gw_h2h_data:
                for team in gw_h2h_data.get('standings',{}).get('results',[]):
                    h2h_history.append({'gameweek': gw, 'manager_id': team['entry'], 'total_h2h_points': team['total']})
        h2h_history_df = pd.DataFrame(h2h_history)

        for month_name, gws_in_month in FPL_MONTH_MAP.items():
            if not gws_in_month or gws_in_month[0] > last_finished_gw:
                continue

            last_gw_of_month = max([gw for gw in gws_in_month if gw <= last_finished_gw])
            last_gw_of_prev_month = min(gws_in_month) - 1

            h2h_totals_end_of_month = h2h_history_df[h2h_history_df['gameweek'] == last_gw_of_month][['manager_id', 'total_h2h_points']]
            
            if last_gw_of_prev_month > 0:
                h2h_totals_start_of_month = h2h_history_df[h2h_history_df['gameweek'] == last_gw_of_prev_month][['manager_id', 'total_h2h_points']]
                monthly_summary = h2h_totals_end_of_month.merge(h2h_totals_start_of_month, on='manager_id', suffixes=('_end', '_start'))
                monthly_summary['Total_Head_to_Head_FPL_Point'] = monthly_summary['total_h2h_points_end'] - monthly_summary['total_h2h_points_start']
            else:
                monthly_summary = h2h_totals_end_of_month.rename(columns={'total_h2h_points': 'Total_Head_to_Head_FPL_Point'})
            
            # Deconstruct matches into individual results for FPL Points and Point Difference
            monthly_matches = h2h_matches_df[h2h_matches_df['event'].isin(gws_in_month)]
            results_list = []
            for _, row in monthly_matches.iterrows():
                results_list.append({'manager_id': row['entry_1_entry'], 'gameweek': row['event'], 'points': row['entry_1_points'], 'opponent_score': row['entry_2_points']})
                results_list.append({'manager_id': row['entry_2_entry'], 'gameweek': row['event'], 'points': row['entry_2_points'], 'opponent_score': row['entry_1_points']})
            monthly_results_df = pd.DataFrame(results_list)

            point_diff_calc = monthly_results_df.groupby('manager_id')[['points', 'opponent_score']].sum()
            fpl_points_summary = point_diff_calc.reset_index()
            fpl_points_summary['FPL_Point_Difference'] = fpl_points_summary['points'] - fpl_points_summary['opponent_score']
            
            final_month_df = monthly_summary.merge(fpl_points_summary[['manager_id', 'FPL_Point_Difference']], on='manager_id')

            # Pivot GW scores to create 'vs' columns
            gw_scores_pivot = monthly_results_df.pivot(index='manager_id', columns='gameweek', values='points').fillna(0).astype(int)
            opponent_scores_pivot = monthly_results_df.pivot(index='manager_id', columns='gameweek', values='opponent_score').fillna(0).astype(int)
            
            # First, combine the scores into the 'vs' format using integer column names
            for gw_col_num in gws_in_month:
                # Check if the integer column (e.g., 1, 2, 3) exists in both tables
                if gw_col_num in gw_scores_pivot.columns and gw_col_num in opponent_scores_pivot.columns:
                    # Modify the gw_scores_pivot table in place
                    gw_scores_pivot[gw_col_num] = gw_scores_pivot[gw_col_num].astype(str) + " vs " + opponent_scores_pivot[gw_col_num].astype(str)

            # Second, AFTER the loop is finished, rename the columns to the desired "GW1", "GW2" format
            gw_scores_pivot.columns = [f"GW{col}" for col in gw_scores_pivot.columns]
                        
            final_month_df = manager_df.merge(final_month_df, on='manager_id', how='left').fillna(0)
            final_month_df = final_month_df.merge(gw_scores_pivot, on='manager_id', how='left').fillna('0 vs 0')
            
            final_month_df.sort_values(by=['Total_Head_to_Head_FPL_Point', 'FPL_Point_Difference'], ascending=[False, False], inplace=True)
            final_month_df['Standings'] = final_month_df['Total_Head_to_Head_FPL_Point'].rank(method='min', ascending=False).astype(int)
            
            final_month_df.rename(columns={'team_name': 'Team', 'manager_name': 'Manager'}, inplace=True)
            
            gw_cols = sorted([f"GW{gw}" for gw in gws_in_month])
            column_order = ['Standings', 'Team', 'Manager', 'Total_Head_to_Head_FPL_Point', 'FPL_Point_Difference'] + [col for col in gw_cols if col in final_month_df.columns]
            worksheets_to_write[f"h2h_monthly_{month_name}"] = final_month_df[column_order]                                                                            
    # --- League Cup Winner ---
    # This award is only processed if the season has progressed far enough for the cup to be relevant.
    if last_finished_gw >= 34:
        cup_data = get_json_from_url(CUP_STATUS_URL)
        cup_winner_name = "To Be Determined"
        cup_winner_team = "---"

        # Check if the cup data exists and is marked as finished
        if cup_data and cup_data.get('cup', {}).get('status') == 'finished':
            all_cup_matches = cup_data.get('cup', {}).get('matches', [])
            
            # The final match of the cup always takes place in Gameweek 38
            final_match = next((match for match in all_cup_matches if match['event'] == 38), None)
            
            if final_match:
                winner_id = final_match.get('winner')
                if winner_id:
                    # Look up the winner's details from the main manager dataframe
                    winner_details = manager_df[manager_df['manager_id'] == winner_id]
                    if not winner_details.empty:
                        cup_winner_name = winner_details.iloc[0]['manager_name']
                        cup_winner_team = winner_details.iloc[0]['team_name']

        # Create a DataFrame to store the result
        cup_df = pd.DataFrame([
            {"Winner": cup_winner_name, "Team": cup_winner_team}
        ])
        worksheets_to_write["cup_winner"] = cup_df

    # --- Update the Time Machine for the next run ---
    print("Updating the '_time_machine_ranks' sheet...")
    
    classic_ranks_now = {s['entry']: s['rank'] for s in classic_league_data.get('standings', {}).get('results', [])}
    h2h_ranks_now = {s['entry']: s['rank'] for s in h2h_league_data.get('standings', {}).get('results', [])}
    
    # Remove any old data for the current gameweek to prevent duplicates
    time_machine_df = time_machine_df[time_machine_df['gameweek'] != last_finished_gw]
    
    # Create new rows for the current gameweek's final ranks
    new_ranks_list = []
    for _, manager in manager_df.iterrows():
        manager_id = manager['manager_id']
        new_ranks_list.append({
            'gameweek': last_finished_gw,
            'manager_id': manager_id,
            'manager_name': manager['manager_name'],
            'classic_rank': classic_ranks_now.get(manager_id, 999),
            'h2h_rank': h2h_ranks_now.get(manager_id, 999)
        })
    
    new_ranks_df = pd.DataFrame(new_ranks_list)
    
    # Combine old and new data and save
    updated_time_machine_df = pd.concat([time_machine_df, new_ranks_df]).sort_values(by=['gameweek', 'classic_rank'])
    worksheets_to_write["_time_machine_ranks"] = updated_time_machine_df

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
        time.sleep(3)

    print("--- Pipeline finished successfully! ---")

if __name__ == "__main__":
    main()