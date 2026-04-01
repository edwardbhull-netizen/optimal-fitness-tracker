import os
import re
import shutil
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, Request, Form, Response, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

import database as db

# ── VAPID / Push config ───────────────────────────────────────────────────────
VAPID_PUBLIC_KEY = os.environ.get(
    "VAPID_PUBLIC_KEY",
    "BNToRJn9Xg3sMphjjIXc0V6B1m0v2p9yl698r_QPXMv3VdjN3d5EWB4g4oCxkvvGqZ09ONzCvEK9LfwzZZm4MXI"
)
VAPID_PRIVATE_PEM = os.environ.get(
    "VAPID_PRIVATE_PEM",
    "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgi0TS6UQGQ793CeQT\nswOIBtOi7HSL9/uFK0uV0dCxVcuhRANCAATU6ESZ/V4N7DKYY4yF3NFegdZtL9qf\ncpevfK/0D1zL91XYzd3eRFgeIOKAsZL7xqmdPTjcwrxCvS38M2WZuDFy\n-----END PRIVATE KEY-----\n"
)
VAPID_CLAIMS = {"sub": "mailto:ed@optimalfitnesssolihull.co.uk"}

# ── App setup ─────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(__file__)

app = FastAPI(title="Optimal Fitness Tracker")
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
templates.env.filters["urlencode"] = lambda s: url_quote(str(s), safe="")

from urllib.parse import quote as url_quote

SECRET_KEY = os.environ.get("SECRET_KEY", "of-solihull-tracker-secret-2026")
serializer = URLSafeTimedSerializer(SECRET_KEY)

SESSION_COOKIE = "of_session"
SESSION_MAX_AGE = 60 * 60 * 24 * 7  # 7 days


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    db.init_db()


# ── Session helpers ───────────────────────────────────────────────────────────

def create_session_cookie(data: dict) -> str:
    return serializer.dumps(data)


def read_session_cookie(request: Request) -> Optional[dict]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    try:
        return serializer.loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def require_client(request: Request):
    session = read_session_cookie(request)
    if not session or "client_id" not in session:
        return None
    return session


def require_coach(request: Request):
    session = read_session_cookie(request)
    if not session or not session.get("is_coach"):
        return None
    return session


# ── Programme parser ──────────────────────────────────────────────────────────

def parse_programme(md_path: str) -> dict:
    """
    Parse a programme .md file and return a dict like:
    {
        "DAY A": [{"label": "A", "exercise": "SSB Squat", "sets": "5", "reps": "5", "rest": "3 min", "notes": "..."}, ...],
        "DAY B": [...],
        ...
    }
    """
    if not md_path or not os.path.exists(md_path):
        return {}

    with open(md_path, "r", encoding="utf-8") as f:
        content = f.read()

    days = {}
    current_day = None
    current_exercises = []

    # Split into lines
    lines = content.split("\n")

    for line in lines:
        # Match day headers like: ## DAY A — LOWER BODY or ## Day A or ## DAY A
        day_match = re.match(r"^#{1,3}\s+(DAY\s+[A-Z])", line, re.IGNORECASE)
        if day_match:
            if current_day and current_exercises:
                days[current_day] = current_exercises
            current_day = day_match.group(1).upper()
            current_exercises = []
            continue

        # Match table rows (skip header and separator rows)
        if current_day and line.strip().startswith("|"):
            # Skip separator rows like |---|---|
            if re.match(r"^\|[\s\-\|]+\|$", line.strip()):
                continue
            # Skip header rows
            if re.search(r"\|\s*label\s*\|", line, re.IGNORECASE):
                continue

            parts = [p.strip() for p in line.strip().strip("|").split("|")]
            if len(parts) >= 3:
                # Clean bold markdown from label
                label = re.sub(r"\*+", "", parts[0]).strip()
                exercise = parts[1].strip() if len(parts) > 1 else ""
                sets = parts[2].strip() if len(parts) > 2 else ""
                reps = parts[3].strip() if len(parts) > 3 else ""
                rest = parts[4].strip() if len(parts) > 4 else ""
                notes = parts[5].strip() if len(parts) > 5 else ""

                if exercise and exercise.lower() not in ("exercise", ""):
                    current_exercises.append({
                        "label": label,
                        "exercise": exercise,
                        "sets": sets,
                        "reps": reps,
                        "rest": rest,
                        "notes": notes,
                    })

    # Catch last day
    if current_day and current_exercises:
        days[current_day] = current_exercises

    return days


# ── Routes ────────────────────────────────────────────────────────────────────

# ── iOS API helpers ───────────────────────────────────────────────────────────

def require_token(request: Request, token_override: str = None):
    """Authenticate API requests via Bearer token or form token field."""
    if token_override:
        return db.get_client_by_token(token_override)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return db.get_client_by_token(auth[7:])
    return None


# ── iOS API routes ─────────────────────────────────────────────────────────────

@app.post("/api/auth")
async def api_auth(
    name: str = Form(...),
    pin: str = Form(...),
):
    client = db.get_client_by_name_pin(name, pin)
    if not client:
        return JSONResponse({"error": "Invalid name or PIN"}, status_code=401)
    token = db.get_or_create_client_token(client["id"])
    effort_points = db.get_effort_points_this_month(client["id"])
    return JSONResponse({
        "token": token,
        "client_id": client["id"],
        "name": client["name"],
        "max_hr": client.get("max_hr") or 190,
        "effort_points_this_month": effort_points,
    })


@app.get("/api/profile")
async def api_profile(request: Request):
    client = require_token(request)
    if not client:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    effort_points = db.get_effort_points_this_month(client["id"])
    streak = db.get_session_streak(client["id"])
    return JSONResponse({
        "id": client["id"],
        "name": client["name"],
        "max_hr": client.get("max_hr") or 190,
        "effort_points_this_month": effort_points,
        "streak": streak,
    })


@app.post("/api/profile/max-hr")
async def api_update_max_hr(
    request: Request,
    max_hr: int = Form(...),
):
    client = require_token(request)
    if not client:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    db.update_client_max_hr(client["id"], max_hr)
    return JSONResponse({"ok": True})


@app.post("/api/steps")
async def api_log_steps(
    request: Request,
    steps: int = Form(...),
    date: str = Form(default=""),
    source: str = Form(default="Apple Watch"),
    token: str = Form(default=""),
):
    client = require_token(request, token_override=token or None)
    if not client:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    from datetime import date as dt
    log_date = date if date else dt.today().strftime("%Y-%m-%d")
    db.log_steps(client["id"], log_date, steps, source)
    return JSONResponse({"ok": True, "steps": steps, "date": log_date})


@app.post("/api/workout")
async def api_log_workout(request: Request):
    client = require_token(request)
    if not client:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()
    session_id = db.log_workout_from_ios(
        client_id=client["id"],
        effort_points=body.get("effort_points", 0),
        avg_hr=body.get("avg_hr", 0),
        max_hr=body.get("max_hr", 0),
        calories=body.get("calories", 0),
        duration_seconds=body.get("duration_seconds", 0),
        z1_mins=body.get("z1_mins", 0),
        z2_mins=body.get("z2_mins", 0),
        z3_mins=body.get("z3_mins", 0),
        z4_mins=body.get("z4_mins", 0),
        z5_mins=body.get("z5_mins", 0),
        workout_date=body.get("date"),
    )
    return JSONResponse({"ok": True, "session_id": session_id})


@app.post("/api/workout/sync")
async def api_sync_workout(
    request: Request,
    token: str = Form(...),
    calories: int = Form(default=0),
    avg_hr: int = Form(default=0),
    max_hr: int = Form(default=0),
    duration_minutes: float = Form(default=0),
    source: str = Form(default="Apple Watch"),
    date: str = Form(default=""),
):
    """Form-based workout sync endpoint for iOS Shortcuts."""
    client = require_token(request, token_override=token)
    if not client:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    from datetime import date as dt
    workout_date = date if date else dt.today().strftime("%Y-%m-%d")
    duration_seconds = int(duration_minutes * 60)
    session_id = db.log_workout_from_ios(
        client_id=client["id"],
        effort_points=0,
        avg_hr=avg_hr,
        max_hr=max_hr,
        calories=calories,
        duration_seconds=duration_seconds,
        z1_mins=0, z2_mins=0, z3_mins=0, z4_mins=0, z5_mins=0,
        workout_date=workout_date,
    )
    db.update_session_device_calories(session_id, calories, source)
    return JSONResponse({
        "ok": True,
        "calories": calories,
        "avg_hr": avg_hr,
        "max_hr": max_hr,
        "duration_minutes": duration_minutes,
        "date": workout_date,
    })


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    # If already logged in, redirect
    session = read_session_cookie(request)
    if session:
        if session.get("is_coach"):
            return RedirectResponse("/coach", status_code=302)
        if session.get("client_id"):
            return RedirectResponse("/client/home", status_code=302)
    clients = db.get_all_clients()
    return templates.TemplateResponse("login.html", {"request": request, "clients": clients})


@app.post("/login")
async def login(
    request: Request,
    response: Response,
    login_type: str = Form(...),
    name: str = Form(default=""),
    pin: str = Form(...),
):
    if login_type == "coach":
        coach = db.get_coach_by_pin(pin)
        if not coach:
            clients = db.get_all_clients()
            return templates.TemplateResponse(
                "login.html",
                {"request": request, "clients": clients, "error": "Incorrect coach PIN."},
                status_code=401,
            )
        token = create_session_cookie({"is_coach": True, "coach_id": coach["id"], "coach_name": coach["name"]})
        resp = RedirectResponse("/coach", status_code=302)
        resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_MAX_AGE, httponly=True, samesite="lax")
        return resp

    else:
        client = db.get_client_by_name_pin(name, pin)
        if not client:
            clients = db.get_all_clients()
            return templates.TemplateResponse(
                "login.html",
                {"request": request, "clients": clients, "error": "Name or PIN incorrect. Try again."},
                status_code=401,
            )
        token = create_session_cookie({"client_id": client["id"], "client_name": client["name"]})
        resp = RedirectResponse("/client/home", status_code=302)
        resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_MAX_AGE, httponly=True, samesite="lax")
        return resp


@app.post("/logout")
async def logout():
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie(SESSION_COOKIE)
    return resp


# ── Client routes ─────────────────────────────────────────────────────────────

@app.get("/client/home", response_class=HTMLResponse)
async def client_home(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    client = db.get_client_by_id(session["client_id"])
    if not client:
        return RedirectResponse("/", status_code=302)

    last_session = db.get_last_session(client["id"])
    programme = parse_programme(client.get("programme_path"))
    days = list(programme.keys()) if programme else ["DAY A", "DAY B", "DAY C"]
    streak = db.get_session_streak(client["id"])
    coach_note = db.get_coach_note(client["id"])

    import json
    from datetime import date, timedelta
    week_start = db.get_week_start()
    schedule_rows = db.get_weekly_schedule(week_start)
    sessions_path = os.path.join(BASE_DIR, "static", "sessions.json")
    with open(sessions_path) as f:
        sessions_data = json.load(f)
    sessions_lookup = {}
    for stype, slist in sessions_data.items():
        for s in slist:
            sessions_lookup[s["name"]] = {**s, "type": stype}
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    schedule_dict = {row["day_of_week"]: row for row in schedule_rows}
    week_monday = date.fromisoformat(week_start)
    day_offsets = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5}
    today_name = date.today().strftime("%A")
    weekly_schedule = []
    for d in days_order:
        row = schedule_dict.get(d, {"day_of_week": d, "session_name": None, "session_type": None})
        day_date = week_monday + timedelta(days=day_offsets[d])
        weekly_schedule.append({
            "day": d,
            "day_short": d[:3].upper(),
            "date": day_date.strftime("%-d %b"),
            "session_name": row.get("session_name"),
            "session_type": row.get("session_type"),
            "is_today": d == today_name,
        })

    return templates.TemplateResponse("client_home.html", {
        "request": request,
        "client": client,
        "last_session": last_session,
        "days": days,
        "streak": streak,
        "coach_note": coach_note,
        "weekly_schedule": weekly_schedule,
        "today_name": today_name,
        "has_avatar": os.path.exists(os.path.join(AVATARS_DIR, f"{client['id']}.jpg")),
    })


@app.get("/client/session/{day}", response_class=HTMLResponse)
async def session_page(request: Request, day: str):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    client = db.get_client_by_id(session["client_id"])
    if not client:
        return RedirectResponse("/", status_code=302)

    day_key = day.upper().replace("-", " ")  # e.g. "day-a" → "DAY A"
    programme = parse_programme(client.get("programme_path"))
    exercises = programme.get(day_key, [])

    # Check if there's an active (incomplete) session for today with this day
    today = datetime.now().strftime("%Y-%m-%d")
    conn = db.get_db()
    active = conn.execute(
        "SELECT * FROM sessions WHERE client_id = ? AND date = ? AND programme_day = ? AND completed_at IS NULL ORDER BY id DESC LIMIT 1",
        (client["id"], today, day_key)
    ).fetchone()
    conn.close()

    active_session = dict(active) if active else None

    return templates.TemplateResponse("session.html", {
        "request": request,
        "client": client,
        "day": day_key,
        "exercises": exercises,
        "active_session": active_session,
    })


@app.post("/client/session/start")
async def start_session(
    request: Request,
    programme_day: str = Form(...),
):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    session_id = db.create_session(session["client_id"], programme_day.upper())
    return JSONResponse({"session_id": session_id})


@app.post("/client/session/{session_id}/complete")
async def complete_session(
    request: Request,
    session_id: int,
    notes: str = Form(default=""),
    device_calories: Optional[int] = Form(default=None),
    device_source: str = Form(default=""),
    avg_hr: Optional[int] = Form(default=None),
    max_hr: Optional[int] = Form(default=None),
):
    s = require_client(request)
    if not s:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    sess = db.get_session_by_id(session_id)
    if not sess or sess["client_id"] != s["client_id"]:
        return JSONResponse({"error": "Not found"}, status_code=404)

    db.complete_session(session_id, notes)
    if device_calories:
        db.update_session_device_calories(session_id, device_calories, device_source or "Manual")
    if avg_hr:
        db.update_session_hr(session_id, avg_hr, max_hr)
    return JSONResponse({"ok": True})


@app.post("/client/log-set")
async def log_set_route(
    request: Request,
    session_id: int = Form(...),
    exercise_name: str = Form(...),
    label: str = Form(default=""),
    set_number: int = Form(default=1),
    reps: Optional[int] = Form(default=None),
    weight_kg: Optional[float] = Form(default=None),
    duration_seconds: Optional[int] = Form(default=None),
    calories: Optional[int] = Form(default=None),
    rpe: Optional[int] = Form(default=None),
    notes: str = Form(default=""),
):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    # Verify session belongs to client
    sess = db.get_session_by_id(session_id)
    if not sess or sess["client_id"] != session["client_id"]:
        return JSONResponse({"error": "Not found"}, status_code=404)

    # Check for PR before logging (so we compare against previous bests, not including this set)
    is_pr = db.get_pr_check(session["client_id"], exercise_name, weight_kg, reps)

    log_id = db.log_set(
        session_id=session_id,
        exercise_name=exercise_name,
        label=label,
        set_number=set_number,
        reps=reps,
        weight_kg=weight_kg,
        duration_seconds=duration_seconds,
        calories=calories,
        rpe=rpe,
        notes=notes,
    )
    return JSONResponse({"log_id": log_id, "ok": True, "is_pr": is_pr})


@app.delete("/client/log/{log_id}")
async def delete_log(request: Request, log_id: int):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    db.delete_log(log_id)
    return JSONResponse({"ok": True})


@app.get("/client/exercise/{exercise_name}/history")
async def exercise_history(request: Request, exercise_name: str):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    last_sets = db.get_last_session_exercise(session["client_id"], exercise_name)
    pb = db.get_exercise_pb(session["client_id"], exercise_name)

    return JSONResponse({
        "last_sets": last_sets,
        "pb": pb,
    })


@app.get("/client/session-logs/{session_id}")
async def get_session_logs(request: Request, session_id: int):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    sess = db.get_session_by_id(session_id)
    if not sess or sess["client_id"] != session["client_id"]:
        return JSONResponse({"error": "Not found"}, status_code=404)

    logs = db.get_session_logs(session_id)
    return JSONResponse({"logs": logs})


@app.get("/client/history", response_class=HTMLResponse)
async def client_history(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    client = db.get_client_by_id(session["client_id"])
    all_group = db.get_all_group_sessions_for_client(session["client_id"])

    # Split by type
    burn_sessions = [s for s in all_group if (s.get("session_type") or "").upper() == "BURN"]
    strong_sessions = [s for s in all_group if (s.get("session_type") or "").upper() == "STRONG"]
    hybrid_sessions = [s for s in all_group if (s.get("session_type") or "").upper() == "HYBRID"]
    other_sessions = [s for s in all_group if (s.get("session_type") or "").upper() not in ("BURN", "STRONG", "HYBRID")]

    return templates.TemplateResponse("history.html", {
        "request": request,
        "client": client,
        "burn_sessions": burn_sessions,
        "strong_sessions": strong_sessions,
        "hybrid_sessions": hybrid_sessions,
        "other_sessions": other_sessions,
        "total": len(all_group),
    })


# ── Coach routes ──────────────────────────────────────────────────────────────

@app.get("/coach", response_class=HTMLResponse)
async def coach_dashboard(request: Request):
    session = require_coach(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    clients = db.get_all_clients_with_stats()
    for c in clients:
        c["sessions_this_week"] = len(db.get_sessions_this_week(c["id"]))
        c["recent_pbs"] = db.get_recent_pbs(c["id"], limit=3)
        note = db.get_coach_note(c["id"])
        c["coach_note"] = note["note"] if note else ""

    coaches = db.get_all_coaches()

    return templates.TemplateResponse("coach.html", {
        "request": request,
        "coach_name": session.get("coach_name", "Coach"),
        "clients": clients,
        "coaches": coaches,
    })


@app.get("/coach/client/{client_id}", response_class=HTMLResponse)
async def coach_client_view(request: Request, client_id: int):
    session = require_coach(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    client = db.get_client_by_id(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    all_group = db.get_all_group_sessions_for_client(client_id)
    burn_sessions = [s for s in all_group if (s.get("session_type") or "").upper() == "BURN"]
    strong_sessions = [s for s in all_group if (s.get("session_type") or "").upper() == "STRONG"]
    hybrid_sessions = [s for s in all_group if (s.get("session_type") or "").upper() == "HYBRID"]
    other_sessions = [s for s in all_group if (s.get("session_type") or "").upper() not in ("BURN", "STRONG", "HYBRID")]
    coach_note = db.get_coach_note(client_id)

    return templates.TemplateResponse("history.html", {
        "request": request,
        "client": client,
        "burn_sessions": burn_sessions,
        "strong_sessions": strong_sessions,
        "hybrid_sessions": hybrid_sessions,
        "other_sessions": other_sessions,
        "total": len(all_group),
        "is_coach_view": True,
        "coach_note": coach_note,
        "coach_id": session.get("coach_id"),
    })


@app.get("/group-sessions", response_class=HTMLResponse)
async def group_sessions_page(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    import json
    sessions_path = os.path.join(BASE_DIR, "static", "sessions.json")
    with open(sessions_path) as f:
        sessions_data = json.load(f)
    return templates.TemplateResponse(request, "group_sessions.html", {
        "client": session,
        "sessions": sessions_data
    })


@app.get("/group-session/{session_name}", response_class=HTMLResponse)
async def group_session_detail(request: Request, session_name: str):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    import json
    from urllib.parse import unquote
    session_name = unquote(session_name)
    sessions_path = os.path.join(BASE_DIR, "static", "sessions.json")
    with open(sessions_path) as f:
        sessions_data = json.load(f)
    # Find the session
    found = None
    session_type = None
    for stype, slist in sessions_data.items():
        for s in slist:
            if s["name"] == session_name:
                found = s
                session_type = stype
                break
    if not found:
        return RedirectResponse("/group-sessions", status_code=302)

    client_id = session["client_id"]
    history = db.get_group_session_history(client_id, session_name)
    pb = db.get_group_session_pb(client_id, session_name, found["unit"])

    return templates.TemplateResponse(request, "group_session_detail.html", {
        "client": session,
        "session_info": found,
        "session_type": session_type,
        "history": history,
        "pb": pb
    })


@app.post("/group-session/{session_name}/log")
async def log_group_session(
    request: Request,
    session_name: str,
    result: float = Form(...),
    notes: str = Form(default=""),
    device_calories: Optional[int] = Form(default=None),
    device_source: str = Form(default=""),
):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    from urllib.parse import unquote
    import json
    session_name = unquote(session_name)
    sessions_path = os.path.join(BASE_DIR, "static", "sessions.json")
    with open(sessions_path) as f:
        sessions_data = json.load(f)
    found = None
    session_type = None
    for stype, slist in sessions_data.items():
        for s in slist:
            if s["name"] == session_name:
                found = s
                session_type = stype
                break
    if not found:
        return JSONResponse({"error": "Session not found"}, status_code=404)

    client_id = session["client_id"]
    session_id = db.log_group_session(client_id, session_name, session_type,
                                      found["competition"], result, found["unit"], notes)
    if device_calories:
        db.update_session_device_calories(session_id, device_calories, device_source or "Manual")
    from urllib.parse import quote
    return RedirectResponse(f"/group-session/{quote(session_name)}", status_code=302)


@app.get("/schedule", response_class=HTMLResponse)
async def schedule_page(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    import json
    week_start = db.get_week_start()
    schedule = db.get_weekly_schedule(week_start)
    sessions_path = os.path.join(BASE_DIR, "static", "sessions.json")
    with open(sessions_path) as f:
        sessions_data = json.load(f)
    sessions_lookup = {}
    for stype, slist in sessions_data.items():
        for s in slist:
            sessions_lookup[s["name"]] = {**s, "type": stype}
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    schedule_dict = {row["day_of_week"]: row for row in schedule}
    from datetime import date, timedelta
    week_monday = date.fromisoformat(week_start)
    day_offsets = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5}
    enriched = []
    for day in days_order:
        row = schedule_dict.get(day, {"day_of_week": day, "session_name": None, "session_type": None})
        session_info = sessions_lookup.get(row.get("session_name"), {}) if row.get("session_name") else {}
        day_date = week_monday + timedelta(days=day_offsets[day])
        enriched.append({
            "day": day,
            "date": day_date.strftime("%-d %b"),
            "session_name": row.get("session_name"),
            "session_type": row.get("session_type"),
            "competition": session_info.get("competition"),
            "metric": session_info.get("metric"),
            "unit": session_info.get("unit"),
        })
    today_name = date.today().strftime("%A")
    return templates.TemplateResponse(request, "schedule.html", {
        "client": session,
        "schedule": enriched,
        "week_start": week_start,
        "today_name": today_name,
    })


@app.get("/coach/schedule", response_class=HTMLResponse)
async def coach_schedule_page(request: Request):
    session = require_coach(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    import json
    week_start = db.get_week_start()
    schedule = db.get_weekly_schedule(week_start)
    sessions_path = os.path.join(BASE_DIR, "static", "sessions.json")
    with open(sessions_path) as f:
        sessions_data = json.load(f)
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    schedule_dict = {row["day_of_week"]: row for row in schedule}
    sessions_lookup = {}
    for stype, slist in sessions_data.items():
        for s in slist:
            sessions_lookup[s["name"]] = {**s, "type": stype}
    current = []
    for day in days_order:
        row = schedule_dict.get(day, {"day_of_week": day, "session_name": None, "session_type": None})
        current.append({"day": day, "session_name": row.get("session_name"), "session_type": row.get("session_type")})
    return templates.TemplateResponse(request, "coach_schedule.html", {
        "coach": session,
        "schedule": current,
        "sessions_data": sessions_data,
        "sessions_lookup": sessions_lookup,
        "week_start": week_start,
    })


@app.post("/coach/schedule")
async def save_coach_schedule(request: Request):
    session = require_coach(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    import json
    form = await request.form()
    week_start = db.get_week_start()
    sessions_path = os.path.join(BASE_DIR, "static", "sessions.json")
    with open(sessions_path) as f:
        sessions_data = json.load(f)
    sessions_lookup = {}
    for stype, slist in sessions_data.items():
        for s in slist:
            sessions_lookup[s["name"]] = stype
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    for day in days:
        session_name = form.get(day, "").strip()
        if session_name and session_name in sessions_lookup:
            db.set_weekly_schedule(week_start, day, session_name, sessions_lookup[session_name])
        elif session_name == "":
            db.set_weekly_schedule(week_start, day, None, None)
    return RedirectResponse("/coach/schedule", status_code=302)


@app.get("/client/activity", response_class=HTMLResponse)
async def activity_page(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    client = db.get_client_by_id(session["client_id"])
    today_steps = db.get_today_steps(session["client_id"])
    cal_history = db.get_session_calories_history(session["client_id"], limit=8)
    cal_pb = db.get_session_calories_pb(session["client_id"])

    return templates.TemplateResponse("activity.html", {
        "request": request,
        "client": client,
        "today_steps": today_steps,
        "cal_history": cal_history,
        "cal_pb": cal_pb,
    })


@app.get("/client/connect", response_class=HTMLResponse)
async def connect_watch_page(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    client_id = session["client_id"]
    token = db.get_or_create_client_token(client_id)
    base_url = "https://optimal-fitness-tracker-production.up.railway.app"
    return templates.TemplateResponse(request, "connect_watch.html", {
        "request": request,
        "client_name": session["client_name"],
        "token": token,
        "base_url": base_url,
    })


@app.get("/client/pbs", response_class=HTMLResponse)
async def pbs_page(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    client_id = session["client_id"]
    pbs = db.get_all_pbs(client_id)
    return templates.TemplateResponse(request, "pbs.html", {
        "request": request,
        "client_name": session["client_name"],
        "pbs": pbs,
    })


@app.post("/client/pbs/set")
async def set_pb(
    request: Request,
    exercise_name: str = Form(...),
    weight_kg: Optional[float] = Form(default=None),
    reps: Optional[int] = Form(default=None),
    distance_m: Optional[float] = Form(default=None),
    time_seconds: Optional[int] = Form(default=None),
    notes: str = Form(default=""),
    date_set: str = Form(default=""),
):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    client_id = session["client_id"]
    db.set_manual_pb(client_id, exercise_name, weight_kg, reps, distance_m, time_seconds, notes, date_set or None)
    return RedirectResponse("/client/pbs", status_code=302)


@app.post("/client/pbs/delete/{pb_id}")
async def delete_pb(request: Request, pb_id: int):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    db.delete_manual_pb(session["client_id"], pb_id)
    return RedirectResponse("/client/pbs", status_code=302)


@app.post("/client/steps/log")
async def log_steps(
    request: Request,
    steps: int = Form(...),
    source: str = Form(default="Manual"),
    date: str = Form(default=""),
):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    from datetime import date as dt
    log_date = date if date else dt.today().strftime("%Y-%m-%d")
    db.log_steps(session["client_id"], log_date, steps, source)
    return JSONResponse({"ok": True})


@app.get("/client/steps/data")
async def steps_data(request: Request, view: str = "daily"):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    client_id = session["client_id"]
    if view == "weekly":
        data = db.get_steps_weekly(client_id, weeks=8)
        labels = [d["label"] for d in data]
        values = [d["total"] for d in data]
    elif view == "monthly":
        data = db.get_steps_monthly(client_id, months=6)
        labels = [d["label"] for d in data]
        values = [d["total"] for d in data]
    else:
        data = db.get_steps_daily(client_id, days=14)
        from datetime import datetime
        labels = [datetime.strptime(d["date"], "%Y-%m-%d").strftime("%-d %b") for d in data]
        values = [d["steps"] for d in data]

    return JSONResponse({"labels": labels, "values": values})


@app.get("/client/progress", response_class=HTMLResponse)
async def progress_page(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    client = db.get_client_by_id(session["client_id"])
    cid = session["client_id"]

    streak = db.get_session_streak(cid)
    weekly_counts = db.get_weekly_session_counts(cid, weeks=8)
    pbs = db.get_recent_pbs(cid, limit=6)
    latest_body = db.get_latest_body(cid)
    body_history = db.get_body_history(cid, limit=12)
    recovery_history = db.get_recovery_history(cid, limit=5)
    total_sessions = len(db.get_client_sessions(cid, limit=1000))

    # Sessions this week
    from datetime import date, timedelta
    today = date.today()
    monday = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
    sunday = (today + timedelta(days=(6 - today.weekday()))).strftime("%Y-%m-%d")
    conn = db.get_db()
    week_sessions = conn.execute(
        "SELECT COUNT(*) as cnt FROM sessions WHERE client_id=? AND date>=? AND date<=? AND completed_at IS NOT NULL",
        (cid, monday, sunday)
    ).fetchone()["cnt"]
    conn.close()

    return templates.TemplateResponse("progress.html", {
        "request": request,
        "client": client,
        "streak": streak,
        "weekly_counts": weekly_counts,
        "pbs": pbs,
        "latest_body": latest_body,
        "body_history": body_history,
        "recovery_history": recovery_history,
        "total_sessions": total_sessions,
        "week_sessions": week_sessions,
    })


@app.get("/client/recovery/{session_id}", response_class=HTMLResponse)
async def recovery_page(request: Request, session_id: int):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    sess = db.get_session_by_id(session_id)
    if not sess or sess["client_id"] != session["client_id"]:
        return RedirectResponse("/client/home", status_code=302)

    # Already logged recovery for this session?
    existing = db.get_recovery_for_session(session_id)

    return templates.TemplateResponse("recovery.html", {
        "request": request,
        "client": db.get_client_by_id(session["client_id"]),
        "session": sess,
        "session_id": session_id,
        "existing": existing,
    })


@app.post("/client/recovery/{session_id}")
async def log_recovery(
    request: Request,
    session_id: int,
    energy: int = Form(...),
    soreness: int = Form(...),
    sleep: int = Form(...),
    notes: str = Form(default=""),
):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    from datetime import date
    today = date.today().strftime("%Y-%m-%d")
    db.log_recovery(session["client_id"], session_id, today, energy, soreness, sleep, notes)
    return RedirectResponse("/client/home", status_code=302)


@app.post("/client/body/log")
async def log_body(
    request: Request,
    weight_kg: Optional[float] = Form(default=None),
    waist_cm: Optional[float] = Form(default=None),
    energy: Optional[int] = Form(default=None),
    notes: str = Form(default=""),
):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    from datetime import date
    today = date.today().strftime("%Y-%m-%d")
    db.log_body(session["client_id"], today, weight_kg, waist_cm, energy, notes)
    return JSONResponse({"ok": True})


@app.get("/client/body/data")
async def body_data(request: Request):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    history = db.get_body_history(session["client_id"], limit=12)
    return JSONResponse({
        "labels": [h["date"] for h in history],
        "weights": [h["weight_kg"] for h in history],
    })


@app.post("/coach/client/{client_id}/note")
async def save_coach_note(
    request: Request,
    client_id: int,
    note: str = Form(...),
):
    session = require_coach(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    db.set_coach_note(client_id, session["coach_id"], note)
    return RedirectResponse(f"/coach/client/{client_id}", status_code=302)


@app.post("/coach/delete-client/{client_id}")
async def delete_client(request: Request, client_id: int):
    session = require_coach(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    db.delete_client(client_id)
    return RedirectResponse("/coach", status_code=302)


@app.post("/coach/add-client")
async def add_client(
    request: Request,
    name: str = Form(...),
    pin: str = Form(...),
    programme_path: str = Form(default=""),
):
    session = require_coach(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    client_id = db.add_client(name, pin, programme_path or None)
    return JSONResponse({"client_id": client_id, "ok": True})


# ── Profile routes ────────────────────────────────────────────────────────────

AVATARS_DIR = os.path.join(BASE_DIR, "data", "avatars")
os.makedirs(AVATARS_DIR, exist_ok=True)


@app.get("/client/avatar/{client_id}")
async def serve_avatar(client_id: int):
    path = os.path.join(AVATARS_DIR, f"{client_id}.jpg")
    if os.path.exists(path):
        from fastapi.responses import FileResponse
        return FileResponse(path, media_type="image/jpeg")
    return Response(status_code=404)


@app.post("/client/avatar")
async def upload_avatar(request: Request):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    form = await request.form()
    photo = form.get("photo")
    if not photo:
        return JSONResponse({"error": "No photo"}, status_code=400)
    try:
        from PIL import Image
        import io
        data = await photo.read()
        img = Image.open(io.BytesIO(data)).convert("RGB")
        w, h = img.size
        side = min(w, h)
        left = (w - side) // 2
        top = (h - side) // 2
        img = img.crop((left, top, left + side, top + side))
        img = img.resize((300, 300), Image.LANCZOS)
        out_path = os.path.join(AVATARS_DIR, f"{session['client_id']}.jpg")
        img.save(out_path, "JPEG", quality=85)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/client/profile", response_class=HTMLResponse)
async def client_profile_page(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    import hr_zones as hrz
    client = db.get_client_by_id(session["client_id"])
    profile = db.get_client_profile(session["client_id"])
    hr_data = hrz.build_hr_profile(profile) if profile else None
    has_avatar = os.path.exists(os.path.join(AVATARS_DIR, f"{session['client_id']}.jpg"))
    return templates.TemplateResponse("profile.html", {
        "request": request,
        "client": client,
        "profile": profile,
        "hr_data": hr_data,
        "goals": [
            ("fat_loss",        "Lose weight / Burn fat"),
            ("build_strength",  "Build strength"),
            ("improve_fitness", "Improve fitness & cardio"),
            ("general_health",  "General health & wellbeing"),
            ("performance",     "Performance / Competition"),
        ],
        "fitness_levels": ["Beginner", "Intermediate", "Advanced", "Athlete"],
        "freq_options": ["1–2x per week", "3–4x per week", "5+ per week"],
        "has_avatar": has_avatar,
    })


@app.get("/client/hr-zones", response_class=HTMLResponse)
async def client_hr_zones(request: Request):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    import hr_zones as hrz
    client = db.get_client_by_id(session["client_id"])
    profile = db.get_client_profile(session["client_id"])
    hr_data = hrz.build_hr_profile(profile) if profile else None
    return templates.TemplateResponse("hr_zones.html", {
        "request": request,
        "client": client,
        "hr_data": hr_data,
    })


@app.post("/client/profile")
async def save_profile(
    request: Request,
    dob: str = Form(default=""),
    sex: str = Form(default=""),
    height_cm: str = Form(default=""),
    weight_kg: str = Form(default=""),
    resting_hr: str = Form(default=""),
    max_hr_manual: str = Form(default=""),
    goal: str = Form(default=""),
    fitness_level: str = Form(default=""),
    training_freq: str = Form(default=""),
    injuries: str = Form(default=""),
):
    session = require_client(request)
    if not session:
        return RedirectResponse("/", status_code=302)
    db.save_client_profile(session["client_id"], {
        "dob": dob or None,
        "sex": sex or None,
        "height_cm": float(height_cm) if height_cm else None,
        "weight_kg": float(weight_kg) if weight_kg else None,
        "resting_hr": int(resting_hr) if resting_hr else None,
        "max_hr_manual": int(max_hr_manual) if max_hr_manual else None,
        "goal": goal or None,
        "fitness_level": fitness_level or None,
        "training_freq": training_freq or None,
        "injuries": injuries or None,
    })
    return RedirectResponse("/client/profile", status_code=302)


# ── Push notification endpoints ───────────────────────────────────────────────

@app.get("/api/push/vapid-public-key")
async def get_vapid_public_key():
    return JSONResponse({"publicKey": VAPID_PUBLIC_KEY})


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    import json
    body = await request.json()
    db.save_push_subscription(session["client_id"], json.dumps(body))
    return JSONResponse({"ok": True})


@app.post("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    session = require_client(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    db.delete_push_subscription(session["client_id"])
    return JSONResponse({"ok": True})


@app.post("/coach/push/send")
async def coach_send_push(
    request: Request,
    title: str = Form(default="Optimal Fitness"),
    body: str = Form(...),
    url: str = Form(default="/client/home"),
):
    session = require_coach(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    import json
    from pywebpush import webpush, WebPushException

    subscriptions = db.get_all_push_subscriptions()
    sent = 0
    failed = 0
    for sub in subscriptions:
        try:
            sub_info = json.loads(sub["subscription_json"])
            webpush(
                subscription_info=sub_info,
                data=json.dumps({"title": title, "body": body, "url": url}),
                vapid_private_key=VAPID_PRIVATE_PEM,
                vapid_claims=VAPID_CLAIMS,
            )
            sent += 1
        except WebPushException as e:
            failed += 1
            if e.response and e.response.status_code == 410:
                db.delete_push_subscription(sub["client_id"])

    return JSONResponse({"ok": True, "sent": sent, "failed": failed})
