"""
sync_sessions.py — Auto-syncs group sessions from the Brain into sessions.json.

Run this whenever you add a new session to the Brain:
  python3 apps/workout-tracker/sync_sessions.py

It reads all .md files in:
  programming/groups/burn/
  programming/groups/strong/
  programming/groups/hybrid/

And extracts:
  - Session name (from the first H1 heading)
  - Competition description (from "Competition:" or "Challenge:" line)
  - Metric (Cals / Reps / Weight / Distance / Time)
  - Unit (cals / reps / kg / metres / seconds)

Then writes the result to apps/workout-tracker/static/sessions.json
"""

import os
import json
import re

BRAIN_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SESSIONS_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "sessions.json")

TYPE_DIRS = {
    "BURN":   os.path.join(BRAIN_ROOT, "programming", "groups", "burn"),
    "STRONG": os.path.join(BRAIN_ROOT, "programming", "groups", "strong"),
    "HYBRID": os.path.join(BRAIN_ROOT, "programming", "groups", "hybrid"),
}

# Files to skip
SKIP_FILES = {"configs", "scripts", "__pycache__"}

METRIC_MAP = {
    "cals":    ("Cals",     "cals"),
    "cal":     ("Cals",     "cals"),
    "calories":("Cals",     "cals"),
    "reps":    ("Reps",     "reps"),
    "rep":     ("Reps",     "reps"),
    "kg":      ("Weight",   "kg"),
    "weight":  ("Weight",   "kg"),
    "metres":  ("Distance", "metres"),
    "meters":  ("Distance", "metres"),
    "distance":("Distance", "metres"),
    "seconds": ("Time",     "seconds"),
    "time":    ("Time",     "seconds"),
    "mins":    ("Time",     "seconds"),
}


def parse_session_md(filepath: str):
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    # Name — first H1, take part before " — " if present
    name_match = re.search(r'^#\s+(.+)', content, re.MULTILINE)
    if not name_match:
        return None
    raw_name = name_match.group(1).strip()
    # Strip type suffix like "— BURN SESSION" or "— HYBRID SESSION FORMAT"
    name = re.split(r'\s+[—–-]+\s+', raw_name)[0].strip()
    # Title case
    name = name.title()

    # Competition / Challenge line
    comp_match = re.search(
        r'(?:competition|challenge|comp metric)[:\s*]+(.+)',
        content, re.IGNORECASE | re.MULTILINE
    )
    competition = comp_match.group(1).strip() if comp_match else "Best effort"
    # Strip markdown bold/italic
    competition = re.sub(r'\*+', '', competition).strip()

    # Detect metric from competition text or nearby lines
    metric = "Cals"
    unit = "cals"
    text_lower = competition.lower()
    for keyword, (m, u) in METRIC_MAP.items():
        if keyword in text_lower:
            metric, unit = m, u
            break

    return {"name": name, "competition": competition, "metric": metric, "unit": unit}


def sync():
    result = {}
    total = 0

    for stype, dirpath in TYPE_DIRS.items():
        sessions = []
        if not os.path.isdir(dirpath):
            print(f"  ⚠️  Directory not found: {dirpath}")
            result[stype] = []
            continue

        for fname in sorted(os.listdir(dirpath)):
            if fname.startswith(".") or fname in SKIP_FILES:
                continue
            if not fname.endswith(".md"):
                continue

            fpath = os.path.join(dirpath, fname)
            parsed = parse_session_md(fpath)
            if parsed:
                sessions.append(parsed)
                print(f"  ✅ {stype} — {parsed['name']}")
                total += 1
            else:
                print(f"  ⚠️  Skipped (no H1): {fname}")

        result[stype] = sessions

    with open(SESSIONS_JSON, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n✅ Done — {total} sessions written to sessions.json")


if __name__ == "__main__":
    print("🔄 Syncing sessions from Brain → sessions.json...\n")
    sync()
