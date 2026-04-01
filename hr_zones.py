"""
Heart rate zone calculator for Optimal Fitness Tracker.
Uses 220-age for max HR, Karvonen formula if resting HR is known.
"""
from datetime import date


ZONES = [
    {"number": 1, "name": "Warmup",    "pct_low": 50, "pct_high": 60, "color": "#64B5F6", "label": "Easy — recovery pace"},
    {"number": 2, "name": "Fat Burn",  "pct_low": 60, "pct_high": 70, "color": "#42A5F5", "label": "Comfortable — can hold a conversation"},
    {"number": 3, "name": "Aerobic",   "pct_low": 70, "pct_high": 80, "color": "#66BB6A", "label": "Moderate — breathing harder"},
    {"number": 4, "name": "Threshold", "pct_low": 80, "pct_high": 90, "color": "#FFA726", "label": "Hard — difficult to talk"},
    {"number": 5, "name": "Max",       "pct_low": 90, "pct_high": 100, "color": "#EF5350", "label": "All out — can't sustain long"},
]

GOAL_TARGET_ZONES = {
    "fat_loss":       {"zones": [2, 3], "label": "Zones 2–3 (60–75%)", "why": "Fat burning and aerobic base — this is where your body uses fat as primary fuel"},
    "build_strength": {"zones": [3, 4], "label": "Zones 3–4 (70–85%)", "why": "Aerobic support for strength work — keeps intensity high without burning out"},
    "improve_fitness":{"zones": [3, 4], "label": "Zones 3–4 (70–85%)", "why": "Aerobic and threshold work builds cardiovascular fitness fastest"},
    "general_health": {"zones": [2, 3], "label": "Zones 2–3 (60–75%)", "why": "Sustainable effort that builds a strong aerobic base over time"},
    "performance":    {"zones": [4, 5], "label": "Zones 4–5 (85–95%)", "why": "High intensity builds speed, power and VO2 max for peak performance"},
}


def calc_age(dob_str: str) -> int:
    """Calculate age from ISO date string."""
    try:
        dob = date.fromisoformat(dob_str)
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except Exception:
        return None


def calc_max_hr(age: int, max_hr_manual: int = None) -> int:
    if max_hr_manual:
        return max_hr_manual
    return 220 - age


def calc_zones(max_hr: int, resting_hr: int = None) -> list:
    """Return zone list with bpm boundaries filled in."""
    zones = []
    for z in ZONES:
        if resting_hr:
            # Karvonen (Heart Rate Reserve) — more personalised
            hrr = max_hr - resting_hr
            bpm_low  = round((hrr * z["pct_low"]  / 100) + resting_hr)
            bpm_high = round((hrr * z["pct_high"] / 100) + resting_hr)
        else:
            bpm_low  = round(max_hr * z["pct_low"]  / 100)
            bpm_high = round(max_hr * z["pct_high"] / 100)
        zones.append({**z, "bpm_low": bpm_low, "bpm_high": bpm_high})
    return zones


def get_zone_for_hr(hr: int, zones: list) -> dict:
    """Return which zone a given HR falls into."""
    for z in reversed(zones):
        if hr >= z["bpm_low"]:
            return z
    return zones[0]


def get_hr_pct_of_max(hr: int, max_hr: int) -> int:
    return round((hr / max_hr) * 100)


def build_hr_profile(profile: dict) -> dict:
    """Given a client_profiles row, return full HR zone data."""
    if not profile or not profile.get("dob"):
        return None

    age = calc_age(profile["dob"])
    if not age:
        return None

    max_hr = calc_max_hr(age, profile.get("max_hr_manual"))
    resting_hr = profile.get("resting_hr")
    zones = calc_zones(max_hr, resting_hr)
    goal = profile.get("goal")
    target = GOAL_TARGET_ZONES.get(goal)

    return {
        "age": age,
        "max_hr": max_hr,
        "resting_hr": resting_hr,
        "zones": zones,
        "target": target,
        "method": "Karvonen (Heart Rate Reserve)" if resting_hr else "Age-based (220 − age)",
    }
