"""
HALFLINE - CBB First Half + Full Game Analyzer
Backend server - fetches data from all 6 sources and runs AI decision logic
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import threading

load_dotenv()

app = Flask(__name__)
CORS(app)  # Allow frontend to call this server from anywhere

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

# -----------------------------------------------------------------
# SIMPLE IN-MEMORY CACHE
# Prevents hammering the data sources on every request
# Data refreshes every 30 minutes automatically
# -----------------------------------------------------------------
cache = {}
cache_lock = threading.Lock()

def get_cached(key, ttl_minutes=30):
    with cache_lock:
        if key in cache:
            data, timestamp = cache[key]
            if time.time() - timestamp < ttl_minutes * 60:
                return data
    return None

def set_cached(key, data):
    with cache_lock:
        cache[key] = (data, time.time())


# =================================================================
# DATA SOURCE 1: THE ODDS API (Live lines across all books)
# =================================================================
def fetch_odds(home_team, away_team):
    """Fetch live 1H and full game lines from The Odds API"""
    cache_key = f"odds_{home_team}_{away_team}"
    cached = get_cached(cache_key, ttl_minutes=5)
    if cached:
        return cached

    result = {
        "full_spread": "N/A",
        "full_total": "N/A",
        "half_spread": "N/A",
        "half_total": "N/A",
        "best_full_book": "N/A",
        "best_half_book": "N/A",
        "full_open": "N/A",
        "half_open": "N/A",
        "source": "odds_api"
    }

    if not ODDS_API_KEY:
        result["error"] = "No Odds API key set"
        return result

    try:
        # Full game lines
        url = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/"
        params = {
            "apiKey": ODDS_API_KEY,
            "regions": "us",
            "markets": "spreads,totals",
            "oddsFormat": "american",
            "bookmakers": "fanduel,draftkings,betmgm,caesars,pointsbet"
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            games = resp.json()
            for game in games:
                ht = game.get("home_team", "").lower()
                at = game.get("away_team", "").lower()
                if home_team.lower() in ht or away_team.lower() in at:
                    for bookmaker in game.get("bookmakers", []):
                        for market in bookmaker.get("markets", []):
                            if market["key"] == "spreads":
                                for outcome in market["outcomes"]:
                                    if home_team.lower() in outcome["name"].lower():
                                        result["full_spread"] = f"{outcome['point']:+.1f}"
                                        result["best_full_book"] = bookmaker["title"]
                            elif market["key"] == "totals":
                                result["full_total"] = str(market["outcomes"][0].get("point", "N/A"))
                    break
    except Exception as e:
        result["error"] = str(e)

    set_cached(cache_key, result)
    return result


# =================================================================
# DATA SOURCE 2: BART TORVIK (Advanced CBB stats + 1H splits)
# =================================================================
def fetch_torvik(team_name):
    """Scrape Bart Torvik for team efficiency and first-half data"""
    cache_key = f"torvik_{team_name}"
    cached = get_cached(cache_key, ttl_minutes=60)
    if cached:
        return cached

    result = {
        "adj_off": None,
        "adj_def": None,
        "adj_tempo": None,
        "barthag": None,
        "team": team_name,
        "source": "barttorvik"
    }

    try:
        # Torvik's main ratings table
        url = "https://barttorvik.com/trank.php"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) > 5:
                    team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    if team_name.lower() in team_cell.lower():
                        try:
                            result["adj_off"] = float(cells[2].get_text(strip=True))
                            result["adj_def"] = float(cells[3].get_text(strip=True))
                            result["adj_tempo"] = float(cells[4].get_text(strip=True))
                            result["barthag"] = float(cells[5].get_text(strip=True))
                        except (ValueError, IndexError):
                            pass
                        break
    except Exception as e:
        result["error"] = str(e)

    set_cached(cache_key, result)
    return result


# =================================================================
# DATA SOURCE 3: HASLAMETRICS (Team fingerprint + efficiency)
# =================================================================
def fetch_haslametrics():
    """Scrape Haslametrics for team ratings and fingerprints"""
    cache_key = "haslametrics_all"
    cached = get_cached(cache_key, ttl_minutes=120)
    if cached:
        return cached

    result = {"teams": {}, "source": "haslametrics"}

    try:
        url = "https://haslametrics.com/ratings.php"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) > 10:
                    try:
                        team_name = cells[1].get_text(strip=True)
                        if team_name:
                            result["teams"][team_name.lower()] = {
                                "rank": cells[0].get_text(strip=True),
                                "off_eff": cells[2].get_text(strip=True),
                                "def_eff": cells[19].get_text(strip=True) if len(cells) > 19 else "N/A",
                                "pace": cells[37].get_text(strip=True) if len(cells) > 37 else "N/A",
                                "momentum": cells[41].get_text(strip=True) if len(cells) > 41 else "N/A",
                            }
                    except (IndexError, ValueError):
                        pass
    except Exception as e:
        result["error"] = str(e)

    set_cached(cache_key, result)
    return result


def get_haslametrics_team(team_name):
    """Get a specific team's data from Haslametrics"""
    all_data = fetch_haslametrics()
    teams = all_data.get("teams", {})
    # Fuzzy match team name
    team_lower = team_name.lower()
    for key, val in teams.items():
        if team_lower in key or key in team_lower:
            return val
    return {"off_eff": "N/A", "def_eff": "N/A", "pace": "N/A", "momentum": "N/A"}


# =================================================================
# DATA SOURCE 4: ACTION NETWORK (Line movement + public %%)
# =================================================================
def fetch_action_network(home_team, away_team):
    """Fetch line movement and public betting data from Action Network"""
    cache_key = f"action_{home_team}_{away_team}"
    cached = get_cached(cache_key, ttl_minutes=10)
    if cached:
        return cached

    result = {
        "public_pct_home": None,
        "public_pct_away": None,
        "line_move": None,
        "sharp_indicator": "MIXED",
        "steam_move": False,
        "source": "action_network"
    }

    try:
        # Action Network public API endpoint for NCAAB
        today = datetime.now().strftime("%Y%m%d")
        url = f"https://api.actionnetwork.com/web/v1/scoreboard/ncaab?period=game&bookIds=15,30,76,75&date={today}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            games = data.get("games", [])
            for game in games:
                teams = game.get("teams", [])
                team_names = [t.get("full_name", "").lower() for t in teams]
                if any(home_team.lower() in tn for tn in team_names) or \
                   any(away_team.lower() in tn for tn in team_names):
                    # Pull betting splits if available
                    odds = game.get("odds", [])
                    if odds:
                        o = odds[0]
                        result["public_pct_home"] = o.get("home_spread_pct")
                        result["public_pct_away"] = o.get("away_spread_pct")
                        open_spread = o.get("open_spread")
                        current_spread = o.get("spread")
                        if open_spread and current_spread:
                            move = round(current_spread - open_spread, 1)
                            result["line_move"] = f"{move:+.1f}"
                            result["open_spread"] = str(open_spread)
                    break
    except Exception as e:
        result["error"] = str(e)

    # Determine sharp indicator based on public % vs line movement
    if result["public_pct_home"] and result["line_move"]:
        pub = result["public_pct_home"]
        move = float(result["line_move"])
        if pub > 60 and move < 0:
            result["sharp_indicator"] = "REVERSE LINE — SHARP"
        elif pub > 60 and move > 0:
            result["sharp_indicator"] = "PUBLIC SIDE"
        elif pub < 40 and move < 0:
            result["sharp_indicator"] = "SHARP MONEY"
        else:
            result["sharp_indicator"] = "MIXED"

    set_cached(cache_key, result)
    return result


# =================================================================
# DATA SOURCE 5: TEAMRANKINGS (1H ATS records + scoring splits)
# =================================================================
def fetch_teamrankings(team_name):
    """Scrape TeamRankings for first-half stats"""
    cache_key = f"teamrankings_{team_name}"
    cached = get_cached(cache_key, ttl_minutes=120)
    if cached:
        return cached

    result = {
        "first_half_ppg": None,
        "first_half_pa": None,
        "ats_pct": None,
        "source": "teamrankings"
    }

    try:
        # TeamRankings 1H points per game
        url = "https://www.teamrankings.com/ncaa-basketball/stat/1st-half-points-per-game"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/"
        }
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    name = cells[1].get_text(strip=True)
                    if team_name.lower() in name.lower():
                        try:
                            result["first_half_ppg"] = float(cells[2].get_text(strip=True))
                        except ValueError:
                            pass
                        break
    except Exception as e:
        result["error"] = str(e)

    set_cached(cache_key, result)
    return result


# =================================================================
# DATA SOURCE 6: COVERS.COM (Historical ATS trends)
# =================================================================
def fetch_covers(home_team, away_team):
    """Scrape Covers.com for historical ATS trends"""
    cache_key = f"covers_{home_team}_{away_team}"
    cached = get_cached(cache_key, ttl_minutes=120)
    if cached:
        return cached

    result = {
        "home_ats_record": "N/A",
        "away_ats_record": "N/A",
        "home_ou_record": "N/A",
        "away_ou_record": "N/A",
        "source": "covers"
    }

    try:
        url = f"https://www.covers.com/sport/basketball/ncaab/teams/main/{home_team.lower().replace(' ', '-')}/2024-2025"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        }
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            # Look for ATS record in page
            ats_elements = soup.find_all(text=lambda t: "ATS" in str(t))
            if ats_elements:
                result["home_ats_record"] = str(ats_elements[0]).strip()
    except Exception as e:
        result["error"] = str(e)

    set_cached(cache_key, result)
    return result


# =================================================================
# AI DECISION ENGINE
# Synthesizes all data into PLAY / PASS / MONITOR + reasoning
# =================================================================
def run_decision_engine(home, away, data):
    """
    Core decision logic — scores each signal and produces a recommendation.
    Returns verdict, confidence (0-100), and reasoning for both 1H and full game.
    """

    half_score = 50  # Start neutral
    full_score = 50

    signals = []
    full_signals = []

    # --- EFFICIENCY SIGNALS ---
    torvik_home = data.get("torvik_home", {})
    torvik_away = data.get("torvik_away", {})

    home_off = torvik_home.get("adj_off") or 105.0
    away_off = torvik_away.get("adj_off") or 105.0
    home_def = torvik_home.get("adj_def") or 100.0
    away_def = torvik_away.get("adj_def") or 100.0
    home_tempo = torvik_home.get("adj_tempo") or 70.0
    away_tempo = torvik_away.get("adj_tempo") or 70.0

    off_edge = home_off - away_off
    def_edge = away_def - home_def  # positive = home defense better
    combined_tempo = (home_tempo + away_tempo) / 2

    if off_edge > 5:
        half_score += 8
        full_score += 10
        signals.append(f"{home.upper()} has a significant offensive efficiency edge (+{off_edge:.1f})")
        full_signals.append(f"{home.upper()} offensive edge is strong throughout the game")
    elif off_edge < -5:
        half_score -= 6
        full_score -= 8
        signals.append(f"{away.upper()} has the offensive efficiency advantage (+{abs(off_edge):.1f})")

    if combined_tempo > 73:
        half_score += 5
        full_score += 5
        signals.append(f"Fast combined pace ({combined_tempo:.1f}) favors the OVER")
    elif combined_tempo < 67:
        half_score -= 5
        full_score -= 5
        signals.append(f"Slow pace ({combined_tempo:.1f}) — this could be a low-scoring grind")

    # --- HASLAMETRICS MOMENTUM SIGNAL ---
    hasla_home = data.get("hasla_home", {})
    hasla_away = data.get("hasla_away", {})
    home_mom = hasla_home.get("momentum", "")
    away_mom = hasla_away.get("momentum", "")
    try:
        if float(home_mom) > 5:
            half_score += 4
            full_score += 6
            signals.append(f"{home.upper()} carrying strong positive momentum")
        elif float(away_mom) > 5:
            half_score += 3
            full_score += 4
    except (ValueError, TypeError):
        pass

    # --- MARKET SIGNALS ---
    action = data.get("action", {})
    sharp = action.get("sharp_indicator", "MIXED")
    line_move = action.get("line_move")
    pub_pct = action.get("public_pct_home")

    if sharp == "REVERSE LINE — SHARP":
        half_score += 10
        full_score += 8
        signals.append("Reverse line movement detected — sharp money is active")
    elif sharp == "SHARP MONEY":
        half_score += 7
        full_score += 6
        signals.append("Sharp money indicator confirmed on this side")
    elif sharp == "PUBLIC SIDE":
        half_score -= 5
        full_score -= 4
        signals.append("Heavy public action — fade risk present")

    if line_move:
        try:
            move_val = float(line_move)
            if abs(move_val) >= 1.5:
                half_score += 6
                signals.append(f"Significant line movement of {line_move} pts since open")
        except ValueError:
            pass

    # --- FIRST HALF SPECIFIC: TEAMRANKINGS SCORING ---
    tr_home = data.get("tr_home", {})
    tr_away = data.get("tr_away", {})
    home_1h = tr_home.get("first_half_ppg")
    away_1h = tr_away.get("first_half_ppg")

    half_total_posted = data.get("half_total_posted")
    full_total_posted = data.get("full_total_posted")

    if home_1h and away_1h:
        projected_1h = home_1h + away_1h
        if half_total_posted:
            try:
                diff = projected_1h - float(half_total_posted)
                if diff > 3:
                    half_score += 8
                    signals.append(f"Projected 1H total ({projected_1h:.1f}) is {diff:.1f} pts OVER posted line")
                elif diff < -3:
                    half_score -= 8
                    signals.append(f"Projected 1H total ({projected_1h:.1f}) is {abs(diff):.1f} pts UNDER posted line")
            except (ValueError, TypeError):
                pass

    # --- SITUATIONAL FLAGS ---
    flags = data.get("flags", {})
    if flags.get("injury"):
        half_score -= 8
        full_score -= 10
        signals.append("KEY INJURY FLAG — significantly reduces confidence")
    if flags.get("b2b"):
        half_score -= 4
        full_score -= 6
        signals.append("Back-to-back game — fatigue factor in second half")
        full_signals.append("Back-to-back significantly affects full game performance")
    if flags.get("rivalry"):
        half_score += 3
        full_score += 2
        signals.append("Rivalry game — expect high intensity from tip-off")
    if flags.get("slow_starter"):
        half_score -= 6
        signals.append("Team flagged as slow starter — 1H value is reduced")

    # --- CONVERT SCORE TO VERDICT ---
    def score_to_verdict(score):
        if score >= 62:
            return "PLAY"
        elif score <= 42:
            return "PASS"
        else:
            return "MONITOR"

    half_verdict = score_to_verdict(half_score)
    full_verdict = score_to_verdict(full_score)
    half_conf = max(35, min(88, half_score))
    full_conf = max(35, min(88, full_score))

    # --- BUILD REASONING TEXT ---
    def build_reasoning(verdict, conf, sigs, half=True):
        period = "first half" if half else "full game"
        if not sigs:
            sigs = [f"Insufficient data for strong {period} signal — use manual judgment"]

        if verdict == "PLAY":
            intro = f"Model shows {conf}% confidence for this {period} bet. "
        elif verdict == "PASS":
            intro = f"Model recommends skipping this {period} at {100-conf}% confidence. "
        else:
            intro = f"Mixed signals on this {period} — monitor for updates. "

        return intro + " ".join(sigs[:3]) + "."

    half_reasoning = build_reasoning(half_verdict, half_conf, signals, half=True)
    full_reasoning = build_reasoning(full_verdict, full_conf, full_signals + signals, half=False)

    # Detect conflict and parlay
    is_conflict = (half_verdict != full_verdict and
                   "MONITOR" not in [half_verdict, full_verdict])
    is_parlay = (half_verdict == "PLAY" and full_verdict == "PLAY" and
                 half_conf >= 65 and full_conf >= 65)

    return {
        "half_verdict": half_verdict,
        "full_verdict": full_verdict,
        "half_confidence": half_conf,
        "full_confidence": full_conf,
        "half_reasoning": half_reasoning,
        "full_reasoning": full_reasoning,
        "is_conflict": is_conflict,
        "is_parlay": is_parlay,
        "signals": signals[:5],
        "half_score_raw": half_score,
        "full_score_raw": full_score,
        "combined_tempo": round(combined_tempo, 1),
        "off_edge": round(off_edge, 1),
        "projected_half_total": round((home_1h or 33) + (away_1h or 33), 1),
        "projected_full_total": round(((home_1h or 33) + (away_1h or 33)) * 2.05, 1),
    }


# =================================================================
# MAIN API ENDPOINT
# POST /analyze  — called by your frontend
# =================================================================
@app.route("/analyze", methods=["POST"])
def analyze():
    """
    Main endpoint. Accepts game details, fetches all data, returns full analysis.
    """
    body = request.get_json()
    home = body.get("home", "")
    away = body.get("away", "")
    half_spread = body.get("half_spread", "")
    half_total = body.get("half_total", "")
    full_spread = body.get("full_spread", "")
    full_total = body.get("full_total", "")
    flags = body.get("flags", {})

    if not home or not away:
        return jsonify({"error": "Home and away team names are required"}), 400

    # --- FETCH ALL DATA SOURCES IN PARALLEL ---
    results = {}

    def fetch_all():
        results["odds"] = fetch_odds(home, away)
        results["torvik_home"] = fetch_torvik(home)
        results["torvik_away"] = fetch_torvik(away)
        results["hasla_home"] = get_haslametrics_team(home)
        results["hasla_away"] = get_haslametrics_team(away)
        results["action"] = fetch_action_network(home, away)
        results["tr_home"] = fetch_teamrankings(home)
        results["tr_away"] = fetch_teamrankings(away)
        results["covers"] = fetch_covers(home, away)

    thread = threading.Thread(target=fetch_all)
    thread.start()
    thread.join(timeout=20)  # Max 20 seconds to fetch everything

    # --- PACKAGE DATA FOR DECISION ENGINE ---
    engine_data = {
        **results,
        "half_total_posted": half_total,
        "full_total_posted": full_total,
        "flags": flags
    }

    # --- RUN DECISION ENGINE ---
    decision = run_decision_engine(home, away, engine_data)

    # --- BUILD FULL RESPONSE ---
    torvik_home = results.get("torvik_home", {})
    torvik_away = results.get("torvik_away", {})
    hasla_home = results.get("hasla_home", {})
    hasla_away = results.get("hasla_away", {})
    action = results.get("action", {})
    odds = results.get("odds", {})
    tr_home = results.get("tr_home", {})
    tr_away = results.get("tr_away", {})
    covers = results.get("covers", {})

    response = {
        # Verdicts
        "half_verdict": decision["half_verdict"],
        "full_verdict": decision["full_verdict"],
        "half_confidence": decision["half_confidence"],
        "full_confidence": decision["full_confidence"],
        "half_reasoning": decision["half_reasoning"],
        "full_reasoning": decision["full_reasoning"],
        "is_conflict": decision["is_conflict"],
        "is_parlay": decision["is_parlay"],
        "signals": decision["signals"],

        # Projections
        "projected_half_total": decision["projected_half_total"],
        "projected_full_total": decision["projected_full_total"],

        # Efficiency data
        "home_adj_off": torvik_home.get("adj_off", "N/A"),
        "away_adj_off": torvik_away.get("adj_off", "N/A"),
        "home_adj_def": torvik_home.get("adj_def", "N/A"),
        "away_adj_def": torvik_away.get("adj_def", "N/A"),
        "home_tempo": torvik_home.get("adj_tempo", "N/A"),
        "away_tempo": torvik_away.get("adj_tempo", "N/A"),
        "combined_tempo": decision["combined_tempo"],
        "off_edge": decision["off_edge"],

        # Haslametrics
        "home_momentum": hasla_home.get("momentum", "N/A"),
        "away_momentum": hasla_away.get("momentum", "N/A"),
        "home_off_eff_hasla": hasla_home.get("off_eff", "N/A"),
        "away_off_eff_hasla": hasla_away.get("off_eff", "N/A"),

        # First half scoring
        "home_1h_ppg": tr_home.get("first_half_ppg", "N/A"),
        "away_1h_ppg": tr_away.get("first_half_ppg", "N/A"),

        # Market data
        "sharp_indicator": action.get("sharp_indicator", "N/A"),
        "line_move": action.get("line_move", "N/A"),
        "public_pct_home": action.get("public_pct_home", "N/A"),
        "public_pct_away": action.get("public_pct_away", "N/A"),
        "open_spread": action.get("open_spread", full_spread),
        "current_spread": full_spread,
        "best_full_book": odds.get("best_full_book", "N/A"),
        "best_half_book": odds.get("best_half_book", "N/A"),
        "live_full_spread": odds.get("full_spread", full_spread),
        "live_full_total": odds.get("full_total", full_total),

        # Covers ATS
        "home_ats_record": covers.get("home_ats_record", "N/A"),
        "away_ats_record": covers.get("away_ats_record", "N/A"),

        # Signal summary for display
        "sig_line": f"MOVE {action.get('line_move', 'N/A')}",
        "sig_sharp": action.get("sharp_indicator", "MIXED"),
        "sig_pace": "FAST" if decision["combined_tempo"] > 72 else "SLOW",
        "sig_momentum": hasla_home.get("momentum", "N/A"),

        # Meta
        "home": home,
        "away": away,
        "timestamp": datetime.now().isoformat(),
        "data_sources_status": {
            "torvik": "ok" if torvik_home.get("adj_off") else "partial",
            "haslametrics": "ok" if hasla_home.get("off_eff") != "N/A" else "partial",
            "teamrankings": "ok" if tr_home.get("first_half_ppg") else "partial",
            "action_network": "ok" if action.get("sharp_indicator") != "N/A" else "partial",
            "odds_api": "ok" if odds.get("full_spread") != "N/A" else "no_key",
            "covers": "ok" if covers.get("home_ats_record") != "N/A" else "partial",
        }
    }

    return jsonify(response)


# =================================================================
# HEALTH CHECK ENDPOINT
# =================================================================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "cache_entries": len(cache),
        "odds_api_configured": bool(ODDS_API_KEY)
    })


@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "HALFLINE API is running. POST to /analyze to get started."})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
