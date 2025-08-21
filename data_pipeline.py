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
CREDENTIALS_FILE = ".streamlit/google_credentials.json"

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

def get_gameweek_to_month_map(fpl_data):
    gw_map = {}
    for gw_info in fpl_data['events']:
        deadline = datetime.fromisoformat(gw_info['deadline_time'].replace('Z', '+00:00'))
        gw_map[gw_info['id']] = deadline.strftime('%B')
    return gw_map

def main():
    print("--- Starting FPL Data Pipeline ---")
    gc = get_credentials()
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    print(f"Connected to Google Sheet: '{GOOGLE_SHEET_NAME}'")

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
    
    print("Pre-fetching manager histories, transfers, and player details...")
    manager_histories = {row['manager_id']: get_json_from_url(ENTRY_HISTORY_URL.format(TID=row['manager_id'])) for _, row in manager_df.iterrows()}
    manager_transfers = {row['manager_id']: get_json_from_url(ENTRY_TRANSFERS_URL.format(TID=row['manager_id'])) for _, row in manager_df.iterrows()}
    player_details_dict = {pid: get_json_from_url(ELEMENT_SUMMARY_URL.format(EID=pid)) for pid in elements_df['id']}

    long_format_data = {
        "golden_boot": [], "playmaker": [], "golden_glove": [], "best_gk": [], "best_def": [], "best_mid": [], "best_fwd": [], "best_vc": [],
        "transfer_king": [], "bench_king": [], "dream_team": [], "defensive_king": [], "shooting_stars": [], "best_underdog": [], "penalty_king": []
    }
    fpl_challenge_gw_scores = []
    
    print(f"Processing all gameweeks up to GW{last_finished_gw}...")
    for gw in range(1, last_finished_gw + 1):
        live_gw_data = get_json_from_url(LIVE_EVENT_URL.format(GW=gw))
        if not live_gw_data: print(f"Could not fetch live data for GW{gw}. Skipping."); continue
        
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

                transfers_in_gw = [t for t in manager_transfers.get(manager_id, []) if t['event'] == gw]
                points_in = sum(player_details_dict.get(t['element_in'], {}).get('history', [])[gw-1].get('total_points', 0) for t in transfers_in_gw)
                points_out = sum(player_details_dict.get(t['element_out'], {}).get('history', [])[gw-1].get('total_points', 0) for t in transfers_in_gw)
                cost = manager_histories.get(manager_id, {}).get('current', [])[gw-1].get('event_transfers_cost', 0) if manager_histories.get(manager_id) and len(manager_histories[manager_id].get('current', [])) >= gw else 0
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

                underdog_score = 0
                if gw > 1:
                    h2h_matches_df = pd.DataFrame(h2h_matches_data.get('results', []))
                    match = h2h_matches_df[((h2h_matches_df['event'] == gw) & (h2h_matches_df['entry_1_entry'] == manager_id)) | ((h2h_matches_df['event'] == gw) & (h2h_matches_df['entry_2_entry'] == manager_id))]
                    if not match.empty:
                        match = match.iloc[0]
                        opponent_id = None
                        if match['entry_1_entry'] == manager_id and match['entry_1_points'] > match['entry_2_points']: opponent_id = match['entry_2_entry']
                        elif match['entry_2_entry'] == manager_id and match['entry_2_points'] > match['entry_1_points']: opponent_id = match['entry_1_entry']
                        
                        if opponent_id:
                            opp_classic_rank, opp_h2h_rank = classic_ranks_prev.get(opponent_id, 999), h2h_ranks_prev.get(opponent_id, 999)
                            if (opp_classic_rank == 1 and opp_h2h_rank == 1): underdog_score = 3
                            elif (opp_classic_rank == 1 or opp_h2h_rank == 1): underdog_score = 2
                            elif (2 <= opp_classic_rank <= 4 or 2 <= opp_h2h_rank <= 4): underdog_score = 1
                long_format_data['best_underdog'].append({'gameweek': gw, 'manager_name': manager_name, 'score': underdog_score})

        challenge_data = get_json_from_url(FPL_CHALLENGE_LEAGUE_URL)
        if challenge_data and 'standings' in challenge_data:
            for manager_result in challenge_data['standings']['results']:
                fpl_challenge_gw_scores.append({'gameweek': gw, 'manager_name': manager_result['player_name'], 'team_name': manager_result['entry_name'], 'score': manager_result['event_total']})
        
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
        
        if award_name in ["transfer_king", "bench_king", "shooting_stars", "best_vc", "dream_team", "defensive_king", "best_underdog", "penalty_king", "golden_glove"]:
            total = wide_df.sum(axis=1)
        else:
            total = wide_df.get(f"GW{last_finished_gw}", pd.Series(0, index=wide_df.index))
        wide_df['Total'] = total
        
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
    gw_scores_list = []
    for m_id, hist in manager_histories.items():
        if hist and 'current' in hist:
            for h in hist['current']:
                gw_scores_list.append({'manager_id': m_id, 'gameweek': h['event'], 'score': h['points']})
    gw_scores_df = pd.DataFrame(gw_scores_list)
    gw_scores_wide = gw_scores_df.pivot(index='manager_id', columns='gameweek', values='score').fillna(0).astype(int)
    gw_scores_wide.columns = [f"GW{col}" for col in gw_scores_wide.columns]
    
    classic_standings_df = pd.DataFrame(classic_league_data['standings']['results'])[['rank', 'entry_name', 'player_name', 'total', 'entry']]
    classic_standings_df.rename(columns={'rank': 'Standings', 'entry_name': 'Team', 'player_name': 'Manager', 'total': 'Total', 'entry': 'manager_id'}, inplace=True)
    classic_standings_df = classic_standings_df.merge(gw_scores_wide, on='manager_id', how='left').drop(columns=['manager_id'])
    worksheets_to_write["classic_league_standings"] = classic_standings_df
    
    h2h_standings_results = h2h_league_data.get('standings',{}).get('results',[])
    h2h_standings_df = pd.DataFrame(h2h_standings_results)[['rank', 'entry_name', 'player_name', 'total', 'points_for', 'entry']]
    h2h_standings_df.rename(columns={'rank': 'Standings', 'entry_name': 'Team', 'player_name': 'Manager', 'total': 'Total Head-to-Head FPL Point', 'points_for': 'Total FPL Point', 'entry': 'manager_id'}, inplace=True)
    h2h_standings_df = h2h_standings_df.merge(gw_scores_wide, on='manager_id', how='left').drop(columns=['manager_id'])
    worksheets_to_write["h2h_league_standings"] = h2h_standings_df
    
    if fpl_challenge_gw_scores:
        challenge_df_long = pd.DataFrame(fpl_challenge_gw_scores)
        challenge_df_wide = challenge_df_long.pivot_table(index=['manager_name', 'team_name'], columns='gameweek', values='score').fillna(0).astype(int)
        challenge_df_wide.columns = [f"GW{col}" for col in challenge_df_wide.columns]
        challenge_df_wide['Total'] = challenge_df_wide.sum(axis=1)
        challenge_df_wide = challenge_df_wide.reset_index().sort_values(by='Total', ascending=False)
        challenge_df_wide['Standings'] = challenge_df_wide['Total'].rank(method='min', ascending=False).astype(int)
        challenge_df_wide.rename(columns={'team_name': 'Team', 'manager_name': 'Manager'}, inplace=True)
        worksheets_to_write["fpl_challenge_standings"] = challenge_df_wide

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
    all_gw_scores_list = []
    for manager_id, history in manager_histories.items():
        if history and 'current' in history:
            for gw_data in history['current']:
                all_gw_scores_list.append({'gameweek': gw_data['event'], 'manager_id': manager_id, 'score': gw_data['points']})
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

    h2h_matches_df = pd.DataFrame(h2h_matches_data.get('results', []))
    if not h2h_matches_df.empty:
        h2h_matches_df['month'] = h2h_matches_df['event'].map(gw_month_map)
        for month_name in h2h_matches_df['month'].unique():
            monthly_matches = h2h_matches_df[h2h_matches_df['month'] == month_name].copy()
            gws_in_month = monthly_matches['event'].unique()
            
            # Deconstruct matches into individual results
            results_list = []
            for _, row in monthly_matches.iterrows():
                # Result for player 1
                results_list.append({
                    'manager_id': row['entry_1_entry'],
                    'gameweek': row['event'],
                    'points': row['entry_1_points'],
                    'h2h_points': 3 if row['entry_1_points'] > row['entry_2_points'] else (1 if row['entry_1_points'] == row['entry_2_points'] else 0),
                    'opponent_score': row['entry_2_points']
                })
                # Result for player 2
                results_list.append({
                    'manager_id': row['entry_2_entry'],
                    'gameweek': row['event'],
                    'points': row['entry_2_points'],
                    'h2h_points': 3 if row['entry_2_points'] > row['entry_1_points'] else (1 if row['entry_2_points'] == row['entry_1_points'] else 0),
                    'opponent_score': row['entry_1_points']
                })
            monthly_results_df = pd.DataFrame(results_list)
            
            # Aggregate monthly totals
            monthly_summary = monthly_results_df.groupby('manager_id').agg(
                Total_Head_to_Head_FPL_Point=('h2h_points', 'sum'),
                Total_FPL_Points=('points', 'sum')
            ).reset_index()
            monthly_summary['FPL_Point_Difference'] = monthly_results_df.groupby('manager_id').apply(lambda x: (x['points'] - x['opponent_score']).sum()).values
            
            # Pivot GW scores
            gw_scores_pivot = monthly_results_df.pivot(index='manager_id', columns='gameweek', values='points').fillna(0).astype(int)
            gw_scores_pivot.columns = [f"GW{col}" for col in gw_scores_pivot.columns]
            opponent_scores_pivot = monthly_results_df.pivot(index='manager_id', columns='gameweek', values='opponent_score').fillna(0).astype(int)
            
            # Create the 'GW1', 'GW2', etc. columns with 'vs' format
            for gw_col_num in gws_in_month:
                gw_col_name = f"GW{gw_col_num}"
                gw_scores_pivot[gw_col_name] = gw_scores_pivot[gw_col_name].astype(str) + " vs " + opponent_scores_pivot[gw_col_num].astype(str)

            # Combine everything
            final_month_df = manager_df.merge(monthly_summary, on='manager_id', how='left').fillna(0)
            final_month_df = final_month_df.merge(gw_scores_pivot, on='manager_id', how='left').fillna('VS')
            
            final_month_df['Standings'] = final_month_df['Total_Head_to_Head_FPL_Point'].rank(method='min', ascending=False).astype(int)
            final_month_df.sort_values(by=['Standings', 'manager_name'], inplace=True)
            final_month_df.rename(columns={'team_name': 'Team', 'manager_name': 'Manager'}, inplace=True)
            
            gw_cols = sorted([f"GW{gw}" for gw in gws_in_month])
            column_order = ['Standings', 'Team', 'Manager', 'Total_Head_to_Head_FPL_Point', 'FPL_Point_Difference'] + gw_cols
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