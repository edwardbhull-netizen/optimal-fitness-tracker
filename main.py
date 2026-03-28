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

    return templates.TemplateResponse("client_home.html", {
        "request": request,
        "client": client,
        "last_session": last_session,
        "days": days,
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
    return JSONResponse({"log_id": log_id, "ok": True})


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
    sessions = db.get_client_sessions(session["client_id"], limit=30)
    pbs = db.get_recent_pbs(session["client_id"])

    # Attach logs to each session for display
    for s in sessions:
        s["logs"] = db.get_session_logs(s["id"])

    return templates.TemplateResponse("history.html", {
        "request": request,
        "client": client,
        "sessions": sessions,
        "pbs": pbs,
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

    return templates.TemplateResponse("coach.html", {
        "request": request,
        "coach_name": session.get("coach_name", "Coach"),
        "clients": clients,
    })


@app.get("/coach/client/{client_id}", response_class=HTMLResponse)
async def coach_client_view(request: Request, client_id: int):
    session = require_coach(request)
    if not session:
        return RedirectResponse("/", status_code=302)

    client = db.get_client_by_id(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    sessions = db.get_client_sessions(client_id, limit=20)
    for s in sessions:
        s["logs"] = db.get_session_logs(s["id"])

    pbs = db.get_recent_pbs(client_id, limit=10)

    return templates.TemplateResponse("history.html", {
        "request": request,
        "client": client,
        "sessions": sessions,
        "pbs": pbs,
        "is_coach_view": True,
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
    current = []
    for day in days_order:
        row = schedule_dict.get(day, {"day_of_week": day, "session_name": None, "session_type": None})
        current.append({"day": day, "session_name": row.get("session_name"), "session_type": row.get("session_type")})
    return templates.TemplateResponse(request, "coach_schedule.html", {
        "coach": session,
        "schedule": current,
        "sessions_data": sessions_data,
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
