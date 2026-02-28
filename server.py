"""
HALFLINE - CBB First Half + Full Game Analyzer
Haslametrics-first architecture — FT%, 3P%, FG%, rebounding, momentum
all come from Haslametrics. Torvik and others used to verify/supplement.
"""

import os
import time
import requests
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import threading

load_dotenv()

app = Flask(__name__)
CORS(app)

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

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
# PRIMARY SOURCE: HASLAMETRICS
# Columns (offense, cols 2-19):
#   2=Off Eff | 3=FTAR | 4=FT% | 5=FGAR | 6=FG% | 7=3PAR | 8=3P%
#   9=MRAR | 10=MR% | 11=NPAR | 12=NP% | 13=PPSt | 14=PPSC
#   15=SCC% | 16=%3PA | 17=%MRA | 18=%NPA | 19=Prox
# Defense (cols 20-37): same structure
# Fingerprint (cols ~51+): Pace | Mom | MomO | MomD | Con | ConR | SOS
# =================================================================
def fetch_haslametrics_all():
    cache_key = "haslametrics_full"
    cached = get_cached(cache_key, ttl_minutes=120)
    if cached:
        return cached

    result = {"teams": {}, "status": "ok"}

    try:
        url = "https://haslametrics.com/ratings.php"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://haslametrics.com/"
        }
        resp = requests.get(url, headers=headers, timeout=20)

        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 40:
                    continue
                try:
                    team_name = cells[1].get_text(strip=True)
                    if not team_name or team_name.isdigit():
                        continue

                    def sf(idx, default=None):
                        try:
                            v = cells[idx].get_text(strip=True)
                            return float(v) if v else default
                        except (ValueError, IndexError):
                            return default

                    result["teams"][team_name.lower()] = {
                        "name": team_name,
                        # OFFENSE
                        "off_eff":     sf(2),
                        "off_ftar":    sf(3),
                        "off_ft_pct":  sf(4),
                        "off_fgar":    sf(5),
                        "off_fg_pct":  sf(6),
                        "off_3par":    sf(7),
                        "off_3p_pct":  sf(8),
                        "off_mr_pct":  sf(10),
                        "off_np_pct":  sf(12),
                        "off_paint_rate": sf(18),
                        "off_prox":    sf(19),
                        # DEFENSE (what they allow)
                        "def_eff":     sf(20),
                        "def_ftar":    sf(21),
                        "def_ft_pct":  sf(22),
                        "def_fgar":    sf(23),
                        "def_fg_pct":  sf(24),
                        "def_3par":    sf(25),
                        "def_3p_pct":  sf(26),
                        "def_mr_pct":  sf(28),
                        "def_np_pct":  sf(30),
                        "def_paint_rate": sf(36),
                        "def_prox":    sf(37),
                        # FINGERPRINT
                        "pace":        sf(51),
                        "momentum":    sf(52),
                        "momentum_off": sf(53),
                        "momentum_def": sf(54),
                        "consistency": sf(55),
                        "sos":         sf(57),
                        "rank":        cells[0].get_text(strip=True),
                    }
                except Exception:
                    continue
        else:
            result["status"] = f"http_{resp.status_code}"
    except Exception as e:
        result["status"] = f"error:{str(e)}"

    set_cached(cache_key, result)
    return result


def get_hasla_team(team_name):
    all_data = fetch_haslametrics_all()
    teams = all_data.get("teams", {})
    tl = team_name.lower().strip()
    if tl in teams:
        return teams[tl]
    for key, val in teams.items():
        if tl in key or key in tl:
            return val
    words = [w for w in tl.split() if len(w) > 3]
    for key, val in teams.items():
        if any(w in key for w in words):
            return val
    return {}


def fetch_torvik(team_name):
    cache_key = f"torvik_{team_name}"
    cached = get_cached(cache_key, ttl_minutes=60)
    if cached:
        return cached
    result = {"adj_off": None, "adj_def": None, "adj_tempo": None}
    try:
        resp = requests.get("https://barttorvik.com/trank.php",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) > 5:
                    name = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    if team_name.lower() in name.lower() or name.lower() in team_name.lower():
                        try:
                            result["adj_off"] = float(cells[2].get_text(strip=True))
                            result["adj_def"] = float(cells[3].get_text(strip=True))
                            result["adj_tempo"] = float(cells[4].get_text(strip=True))
                        except (ValueError, IndexError):
                            pass
                        break
    except Exception as e:
        result["error"] = str(e)
    set_cached(cache_key, result)
    return result


def fetch_teamrankings(team_name):
    cache_key = f"tr_{team_name}"
    cached = get_cached(cache_key, ttl_minutes=120)
    if cached:
        return cached
    result = {"first_half_ppg": None}
    try:
        resp = requests.get(
            "https://www.teamrankings.com/ncaa-basketball/stat/1st-half-points-per-game",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                     "Referer": "https://www.google.com/"}, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 3 and team_name.lower() in cells[1].get_text(strip=True).lower():
                    try:
                        result["first_half_ppg"] = float(cells[2].get_text(strip=True))
                    except ValueError:
                        pass
                    break
    except Exception as e:
        result["error"] = str(e)
    set_cached(cache_key, result)
    return result


def fetch_action_network(home_team, away_team):
    cache_key = f"action_{home_team}_{away_team}"
    cached = get_cached(cache_key, ttl_minutes=10)
    if cached:
        return cached
    result = {"public_pct_home": None, "public_pct_away": None,
              "line_move": None, "open_spread": None, "sharp_indicator": "N/A"}
    try:
        today = datetime.now().strftime("%Y%m%d")
        resp = requests.get(
            f"https://api.actionnetwork.com/web/v1/scoreboard/ncaab?period=game&bookIds=15,30,76,75&date={today}",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}, timeout=10)
        if resp.status_code == 200:
            for game in resp.json().get("games", []):
                tnames = [t.get("full_name", "").lower() for t in game.get("teams", [])]
                if any(home_team.lower() in tn for tn in tnames) or any(away_team.lower() in tn for tn in tnames):
                    odds = game.get("odds", [])
                    if odds:
                        o = odds[0]
                        result["public_pct_home"] = o.get("home_spread_pct")
                        result["public_pct_away"] = o.get("away_spread_pct")
                        op = o.get("open_spread")
                        cu = o.get("spread")
                        if op is not None and cu is not None:
                            result["open_spread"] = str(op)
                            result["line_move"] = f"{round(cu - op, 1):+.1f}"
                    break
        pub = result.get("public_pct_home")
        mv = result.get("line_move")
        if pub is not None and mv is not None:
            try:
                m = float(mv)
                if pub > 60 and m < 0:
                    result["sharp_indicator"] = "REVERSE LINE — SHARP"
                elif pub > 60 and m > 0:
                    result["sharp_indicator"] = "PUBLIC SIDE"
                elif pub < 40 and m < 0:
                    result["sharp_indicator"] = "SHARP MONEY"
                else:
                    result["sharp_indicator"] = "MIXED"
            except (ValueError, TypeError):
                pass
    except Exception as e:
        result["error"] = str(e)
    set_cached(cache_key, result)
    return result


def fetch_odds(home_team, away_team):
    cache_key = f"odds_{home_team}_{away_team}"
    cached = get_cached(cache_key, ttl_minutes=5)
    if cached:
        return cached
    result = {"full_spread": "N/A", "full_total": "N/A", "best_full_book": "N/A"}
    if not ODDS_API_KEY:
        result["status"] = "no_key"
        return result
    try:
        resp = requests.get("https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "spreads,totals", "oddsFormat": "american",
                    "bookmakers": "fanduel,draftkings,betmgm,caesars"}, timeout=10)
        if resp.status_code == 200:
            for game in resp.json():
                ht = game.get("home_team", "").lower()
                at = game.get("away_team", "").lower()
                if home_team.lower() in ht or away_team.lower() in at:
                    for bm in game.get("bookmakers", []):
                        for market in bm.get("markets", []):
                            if market["key"] == "spreads":
                                for o in market["outcomes"]:
                                    if home_team.lower() in o["name"].lower():
                                        result["full_spread"] = f"{o['point']:+.1f}"
                                        result["best_full_book"] = bm["title"]
                            elif market["key"] == "totals":
                                result["full_total"] = str(market["outcomes"][0].get("point", "N/A"))
                    break
    except Exception as e:
        result["error"] = str(e)
    set_cached(cache_key, result)
    return result


# =================================================================
# HASLAMETRICS-FIRST DECISION ENGINE
# Weights: Haslametrics 70% | Torvik verify 20% | Market 10%
# =================================================================
def run_decision_engine(home, away, data):
    hh = data.get("hasla_home", {})
    ha = data.get("hasla_away", {})
    th = data.get("torvik_home", {})
    ta = data.get("torvik_away", {})
    action = data.get("action", {})
    flags = data.get("flags", {})
    tr_home = data.get("tr_home", {})
    tr_away = data.get("tr_away", {})

    half_score = 50
    full_score = 50
    signals = []
    hasla_ok = bool(hh.get("off_fg_pct") or hh.get("off_eff"))

    if hasla_ok:
        # --- FG% MATCHUP ---
        home_fg_edge = (hh.get("off_fg_pct") or 0) - (ha.get("def_fg_pct") or 0)
        away_fg_edge = (ha.get("off_fg_pct") or 0) - (hh.get("def_fg_pct") or 0)
        if home_fg_edge > 3:
            half_score += 9; full_score += 11
            signals.append(f"{home.upper()} FG% edge: shoots {hh.get('off_fg_pct'):.1f}% vs {away.upper()} defense allowing {ha.get('def_fg_pct'):.1f}%")
        elif away_fg_edge > 3:
            half_score -= 7; full_score -= 9
            signals.append(f"{away.upper()} FG% edge: shoots {ha.get('off_fg_pct'):.1f}% vs {home.upper()} defense allowing {hh.get('def_fg_pct'):.1f}%")

        # --- 3P% MATCHUP ---
        home_3p_edge = (hh.get("off_3p_pct") or 0) - (ha.get("def_3p_pct") or 0)
        away_3p_edge = (ha.get("off_3p_pct") or 0) - (hh.get("def_3p_pct") or 0)
        combined_3p = home_3p_edge + away_3p_edge
        if combined_3p > 4:
            half_score += 7; full_score += 8
            signals.append(f"Both teams shooting above defensive 3P% averages — elevated scoring from beyond the arc")
        elif combined_3p < -4:
            half_score -= 6; full_score -= 7
            signals.append(f"Both defenses suppressing 3P% effectively — expect fewer points from distance")

        # --- FT% + FREE THROW RATE ---
        home_ft_contrib = (hh.get("off_ftar") or 0) * ((hh.get("off_ft_pct") or 70) / 100)
        away_ft_contrib = (ha.get("off_ftar") or 0) * ((ha.get("off_ft_pct") or 70) / 100)
        combined_ft = home_ft_contrib + away_ft_contrib
        if combined_ft > 30:
            half_score += 5; full_score += 7
            signals.append(f"High free throw volume + FT% efficiency — both teams generating extra scoring at the line")
        elif combined_ft < 15:
            half_score -= 4; full_score -= 5
            signals.append(f"Low free throw rate on both sides — fewer free points available")

        # --- REBOUNDING (offensive paint rate vs defensive paint control) ---
        home_reb_edge = (hh.get("off_paint_rate") or 0) - (ha.get("def_paint_rate") or 0)
        away_reb_edge = (ha.get("off_paint_rate") or 0) - (hh.get("def_paint_rate") or 0)
        if home_reb_edge > 5:
            half_score += 6; full_score += 8
            signals.append(f"{home.upper()} offensive rebounding advantage — extra possessions and second-chance points")
        elif away_reb_edge > 5:
            half_score += 4; full_score += 6
            signals.append(f"{away.upper()} winning the paint rebounding battle — extra possessions")

        # --- MOMENTUM (weighted heavily for 1H) ---
        home_mom = hh.get("momentum") or 0
        away_mom = ha.get("momentum") or 0
        home_mom_off = hh.get("momentum_off") or 0
        away_mom_off = ha.get("momentum_off") or 0
        home_mom_def = hh.get("momentum_def") or 0
        away_mom_def = ha.get("momentum_def") or 0
        mom_diff = home_mom - away_mom

        if mom_diff > 8:
            half_score += 10; full_score += 7
            signals.append(f"{home.upper()} momentum: +{mom_diff:.1f} advantage — hot team, expect a strong start")
        elif mom_diff > 4:
            half_score += 6; full_score += 4
            signals.append(f"{home.upper()} has meaningful momentum edge (+{mom_diff:.1f})")
        elif mom_diff < -8:
            half_score -= 10; full_score -= 7
            signals.append(f"{away.upper()} momentum: +{abs(mom_diff):.1f} advantage — dangerous road team right now")
        elif mom_diff < -4:
            half_score -= 6; full_score -= 4
            signals.append(f"{away.upper()} has momentum edge (+{abs(mom_diff):.1f}) — could start fast")

        if home_mom_off > 6 or away_mom_off > 6:
            half_score += 5
            signals.append(f"High offensive momentum — elevated 1H scoring burst likely")

        if home_mom_def > 6 and away_mom_def > 6:
            half_score -= 4; full_score -= 5
            signals.append(f"Both teams in strong defensive momentum — defensive battle expected")

        # --- SHOT PROXIMITY (paint vs perimeter tendencies) ---
        home_prox = hh.get("off_prox") or 0
        away_prox = ha.get("off_prox") or 0
        if home_prox > 0 and away_prox > 0:
            if home_prox < 12 and away_prox < 12:
                half_score += 4; full_score += 5
                signals.append(f"Both teams attack the paint — high-percentage offense favors scoring")
            elif home_prox > 16 and away_prox > 16:
                half_score -= 3; full_score -= 4
                signals.append(f"Both teams relying on perimeter — lower-efficiency offensive night ahead")

    # --- TORVIK CROSS-CHECK ---
    torvik_ok = bool(th.get("adj_off"))
    combined_tempo = 70.0
    if torvik_ok:
        eff_gap = ((th.get("adj_off") or 105) - (th.get("adj_def") or 100)) - \
                  ((ta.get("adj_off") or 105) - (ta.get("adj_def") or 100))
        home_tempo = th.get("adj_tempo") or 70
        away_tempo = ta.get("adj_tempo") or 70
        combined_tempo = (home_tempo + away_tempo) / 2

        if eff_gap > 10:
            half_score += 4; full_score += 6
            signals.append(f"Torvik cross-check confirms {home.upper()} net efficiency advantage")
        elif eff_gap < -10:
            half_score -= 3; full_score -= 5

        if combined_tempo > 73:
            half_score += 5; full_score += 6
            signals.append(f"Torvik confirms fast pace ({combined_tempo:.1f} pos/g) — favors the OVER")
        elif combined_tempo < 67:
            half_score -= 5; full_score -= 6
            signals.append(f"Slow pace ({combined_tempo:.1f} pos/g) — grind expected")

    # --- MARKET SIGNALS ---
    sharp = action.get("sharp_indicator", "N/A")
    line_move = action.get("line_move")
    if sharp == "REVERSE LINE — SHARP":
        half_score += 7; full_score += 6
        signals.append("Reverse line movement — sharp money is active on this game")
    elif sharp == "SHARP MONEY":
        half_score += 5; full_score += 4
        signals.append("Sharp money indicator confirmed")
    elif sharp == "PUBLIC SIDE":
        half_score -= 4; full_score -= 3
        signals.append("Heavy public action — potential fade situation")
    if line_move:
        try:
            if abs(float(line_move)) >= 1.5:
                half_score += 5
                signals.append(f"Significant line move of {line_move} pts since open")
        except ValueError:
            pass

    # --- FLAGS ---
    if flags.get("injury"):
        half_score -= 10; full_score -= 12
        signals.append("KEY INJURY — confidence significantly reduced, verify lineup before betting")
    if flags.get("b2b"):
        half_score -= 3; full_score -= 7
        signals.append("Back-to-back game — fatigue affects second half more than first")
    if flags.get("rivalry"):
        half_score += 3; full_score += 2
        signals.append("Rivalry game — high intensity from opening tip")
    if flags.get("slow_starter"):
        half_score -= 7
        signals.append("Slow starter flag — 1H value significantly reduced")

    # --- PROJECTED TOTALS ---
    h1 = tr_home.get("first_half_ppg")
    a1 = tr_away.get("first_half_ppg")
    if h1 and a1:
        proj_half = round(h1 + a1, 1)
    elif hh.get("off_eff") and ha.get("off_eff"):
        avg_eff = ((hh.get("off_eff") or 105) + (ha.get("off_eff") or 105)) / 2
        proj_half = round(avg_eff * 0.65, 1)
    else:
        proj_half = 67.0
    proj_full = round(proj_half * 2.08, 1)

    half_posted = data.get("half_total_posted")
    if half_posted:
        try:
            diff = proj_half - float(half_posted)
            if diff > 3:
                half_score += 7
                signals.append(f"Projected 1H total ({proj_half}) is {diff:.1f} pts over posted line — OVER value")
            elif diff < -3:
                half_score -= 7
                signals.append(f"Projected 1H total ({proj_half}) is {abs(diff):.1f} pts under posted line — UNDER value")
        except (ValueError, TypeError):
            pass

    def to_verdict(s):
        return "PLAY" if s >= 62 else "PASS" if s <= 42 else "MONITOR"

    half_verdict = to_verdict(half_score)
    full_verdict = to_verdict(full_score)
    half_conf = max(35, min(88, half_score))
    full_conf = max(35, min(88, full_score))
    data_note = "Haslametrics primary." if hasla_ok else "Haslametrics partial — using available data."

    def reasoning(verdict, conf, period, sigs):
        top = sigs[:3]
        base = f"{conf}% confidence. {data_note}"
        if not top:
            return f"Insufficient data for {period} signal. {data_note}"
        if verdict == "PLAY":
            return f"{base} Key factors: {' | '.join(top)}"
        elif verdict == "PASS":
            return f"Skip this {period}. {base} Concerns: {' | '.join(top)}"
        return f"Mixed {period} signals — monitor before tip. {base} {' | '.join(top[:2])}"

    return {
        "half_verdict": half_verdict,
        "full_verdict": full_verdict,
        "half_confidence": half_conf,
        "full_confidence": full_conf,
        "half_reasoning": reasoning(half_verdict, half_conf, "1st half", signals),
        "full_reasoning": reasoning(full_verdict, full_conf, "full game", signals),
        "is_conflict": half_verdict != full_verdict and "MONITOR" not in [half_verdict, full_verdict],
        "is_parlay": half_verdict == "PLAY" and full_verdict == "PLAY" and half_conf >= 65 and full_conf >= 65,
        "signals": signals[:6],
        "projected_half_total": proj_half,
        "projected_full_total": proj_full,
        "combined_tempo": round(combined_tempo, 1),
        "hasla_available": hasla_ok,
    }


@app.route("/analyze", methods=["POST"])
def analyze():
    body = request.get_json()
    home = body.get("home", "").strip()
    away = body.get("away", "").strip()
    if not home or not away:
        return jsonify({"error": "Home and away team names required"}), 400

    results = {}
    def fetch_all():
        results["hasla_home"] = get_hasla_team(home)
        results["hasla_away"] = get_hasla_team(away)
        results["torvik_home"] = fetch_torvik(home)
        results["torvik_away"] = fetch_torvik(away)
        results["action"] = fetch_action_network(home, away)
        results["odds"] = fetch_odds(home, away)
        results["tr_home"] = fetch_teamrankings(home)
        results["tr_away"] = fetch_teamrankings(away)

    t = threading.Thread(target=fetch_all)
    t.start()
    t.join(timeout=22)

    engine_data = {**results,
        "half_total_posted": body.get("half_total", ""),
        "full_total_posted": body.get("full_total", ""),
        "flags": body.get("flags", {})}

    d = run_decision_engine(home, away, engine_data)
    hh = results.get("hasla_home", {})
    ha = results.get("hasla_away", {})
    th = results.get("torvik_home", {})
    ta = results.get("torvik_away", {})
    action = results.get("action", {})
    odds = results.get("odds", {})
    tr_home = results.get("tr_home", {})
    tr_away = results.get("tr_away", {})

    def f(v, suf="", dec=1):
        if v is None: return "N/A"
        try: return f"{float(v):.{dec}f}{suf}"
        except: return str(v)

    return jsonify({
        # Verdicts
        "half_verdict": d["half_verdict"],
        "full_verdict": d["full_verdict"],
        "half_confidence": d["half_confidence"],
        "full_confidence": d["full_confidence"],
        "half_reasoning": d["half_reasoning"],
        "full_reasoning": d["full_reasoning"],
        "is_conflict": d["is_conflict"],
        "is_parlay": d["is_parlay"],
        "signals": d["signals"],
        "projected_half_total": d["projected_half_total"],
        "projected_full_total": d["projected_full_total"],

        # HASLAMETRICS — PRIMARY
        "home_off_fg_pct": f(hh.get("off_fg_pct"), "%"),
        "away_off_fg_pct": f(ha.get("off_fg_pct"), "%"),
        "home_def_fg_pct": f(hh.get("def_fg_pct"), "%"),
        "away_def_fg_pct": f(ha.get("def_fg_pct"), "%"),
        "home_off_3p_pct": f(hh.get("off_3p_pct"), "%"),
        "away_off_3p_pct": f(ha.get("off_3p_pct"), "%"),
        "home_def_3p_pct": f(hh.get("def_3p_pct"), "%"),
        "away_def_3p_pct": f(ha.get("def_3p_pct"), "%"),
        "home_off_ft_pct": f(hh.get("off_ft_pct"), "%"),
        "away_off_ft_pct": f(ha.get("off_ft_pct"), "%"),
        "home_def_ft_pct": f(hh.get("def_ft_pct"), "%"),
        "away_def_ft_pct": f(ha.get("def_ft_pct"), "%"),
        "home_off_ftar":   f(hh.get("off_ftar")),
        "away_off_ftar":   f(ha.get("off_ftar")),
        "home_off_3par":   f(hh.get("off_3par")),
        "away_off_3par":   f(ha.get("off_3par")),
        "home_off_paint":  f(hh.get("off_paint_rate"), "%"),
        "away_off_paint":  f(ha.get("off_paint_rate"), "%"),
        "home_def_paint":  f(hh.get("def_paint_rate"), "%"),
        "away_def_paint":  f(ha.get("def_paint_rate"), "%"),
        "home_off_prox":   f(hh.get("off_prox")),
        "away_off_prox":   f(ha.get("off_prox")),
        "home_momentum":     f(hh.get("momentum")),
        "away_momentum":     f(ha.get("momentum")),
        "home_momentum_off": f(hh.get("momentum_off")),
        "away_momentum_off": f(ha.get("momentum_off")),
        "home_momentum_def": f(hh.get("momentum_def")),
        "away_momentum_def": f(ha.get("momentum_def")),
        "home_off_eff": f(hh.get("off_eff")),
        "away_off_eff": f(ha.get("off_eff")),
        "home_def_eff": f(hh.get("def_eff")),
        "away_def_eff": f(ha.get("def_eff")),
        "home_pace": f(hh.get("pace")),
        "away_pace": f(ha.get("pace")),
        "home_sos":  f(hh.get("sos")),
        "away_sos":  f(ha.get("sos")),
        "home_rank": hh.get("rank", "N/A"),
        "away_rank": ha.get("rank", "N/A"),

        # TORVIK — CROSS-CHECK
        "home_adj_off": f(th.get("adj_off")),
        "away_adj_off": f(ta.get("adj_off")),
        "home_adj_def": f(th.get("adj_def")),
        "away_adj_def": f(ta.get("adj_def")),
        "home_tempo":   f(th.get("adj_tempo")),
        "away_tempo":   f(ta.get("adj_tempo")),
        "combined_tempo": f(d["combined_tempo"]),

        # MARKET
        "sharp_indicator": action.get("sharp_indicator", "N/A"),
        "line_move": action.get("line_move", "N/A"),
        "open_spread": action.get("open_spread", body.get("full_spread", "")),
        "current_spread": body.get("full_spread", ""),
        "public_pct_home": action.get("public_pct_home", "N/A"),
        "public_pct_away": action.get("public_pct_away", "N/A"),
        "best_full_book": odds.get("best_full_book", "N/A"),
        "live_full_total": odds.get("full_total", body.get("full_total", "")),

        # TEAMRANKINGS — 1H supplement
        "home_1h_ppg": f(tr_home.get("first_half_ppg")),
        "away_1h_ppg": f(tr_away.get("first_half_ppg")),

        "home": home,
        "away": away,
        "timestamp": datetime.now().isoformat(),
        "data_sources_status": {
            "haslametrics": "ok" if hh.get("off_fg_pct") else "partial",
            "torvik": "ok" if th.get("adj_off") else "partial",
            "teamrankings": "ok" if tr_home.get("first_half_ppg") else "partial",
            "action_network": "ok" if action.get("sharp_indicator") not in ["N/A", None] else "partial",
            "odds_api": "ok" if odds.get("full_spread") != "N/A" else ("no_key" if not ODDS_API_KEY else "partial"),
            "covers": "partial",
        }
    })


@app.route("/debug/haslametrics", methods=["GET"])
def debug_haslametrics():
    """
    Scrapes Haslametrics and returns the first 3 data rows with every
    cell index + value so we can confirm exact column positions.
    Visit: https://your-railway-url.railway.app/debug/haslametrics
    """
    try:
        url = "https://haslametrics.com/ratings.php"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://haslametrics.com/"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            return jsonify({"error": f"HTTP {resp.status_code}", "url": url})

        soup = BeautifulSoup(resp.text, "html.parser")

        # Grab header rows
        headers_found = []
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) > 10:
                headers_found.append([c.get_text(strip=True) for c in cells])
            if len(headers_found) >= 3:
                break

        # Grab first 3 data rows (rows with enough td cells to be team rows)
        data_rows = []
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 20:
                indexed = {str(i): cells[i].get_text(strip=True) for i in range(min(70, len(cells)))}
                data_rows.append(indexed)
            if len(data_rows) >= 3:
                break

        return jsonify({
            "status": "ok",
            "http_status": resp.status_code,
            "header_rows": headers_found,
            "first_3_data_rows": data_rows,
            "total_rows_found": len(soup.find_all("tr")),
        })

    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat(),
                    "cache_entries": len(cache), "odds_api_configured": bool(ODDS_API_KEY)})

@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "HALFLINE API — Haslametrics-first. POST to /analyze."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
