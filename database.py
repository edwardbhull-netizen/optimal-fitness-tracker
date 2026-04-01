import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "tracker.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            pin TEXT NOT NULL,
            programme_path TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS coaches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            pin TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            programme_day TEXT,
            completed_at TEXT,
            notes TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS exercise_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            exercise_name TEXT NOT NULL,
            label TEXT,
            set_number INTEGER,
            reps INTEGER,
            weight_kg REAL,
            duration_seconds INTEGER,
            calories INTEGER,
            rpe INTEGER,
            notes TEXT,
            logged_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        );

        CREATE TABLE IF NOT EXISTS weekly_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start TEXT NOT NULL,
            day_of_week TEXT NOT NULL,
            session_name TEXT,
            session_type TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS daily_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            steps INTEGER NOT NULL,
            source TEXT DEFAULT 'Manual',
            logged_at TEXT DEFAULT (datetime('now')),
            UNIQUE(client_id, date),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS recovery_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            session_id INTEGER,
            date TEXT NOT NULL,
            energy INTEGER,
            soreness INTEGER,
            sleep INTEGER,
            notes TEXT,
            logged_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS body_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            weight_kg REAL,
            waist_cm REAL,
            energy INTEGER,
            notes TEXT,
            logged_at TEXT DEFAULT (datetime('now')),
            UNIQUE(client_id, date),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS coach_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL UNIQUE,
            coach_id INTEGER,
            note TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS personal_bests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            exercise_name TEXT NOT NULL,
            weight_kg REAL,
            reps INTEGER,
            distance_m REAL,
            time_seconds INTEGER,
            notes TEXT,
            date_set TEXT DEFAULT (date('now')),
            logged_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            subscription_json TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(client_id)
        );

        CREATE TABLE IF NOT EXISTS client_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            dob TEXT,
            sex TEXT,
            height_cm REAL,
            weight_kg REAL,
            resting_hr INTEGER,
            max_hr_manual INTEGER,
            goal TEXT,
            fitness_level TEXT,
            training_freq TEXT,
            injuries TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(client_id)
        );
    """)

    # Seed coaches if not already present
    default_coaches = [
        ("Ed Hull", "1234"),
        ("Dan Truepenny", "1234"),
        ("Vicky Ratcliffe", "1234"),
        ("Sarah", "1234"),
        ("Jody", "1234"),
    ]
    for name, pin in default_coaches:
        exists = c.execute("SELECT id FROM coaches WHERE LOWER(name) = LOWER(?)", (name,)).fetchone()
        if not exists:
            c.execute("INSERT INTO coaches (name, pin) VALUES (?, ?)", (name, pin))

    # Seed Ed as a test client if not already present
    test_client = c.execute("SELECT id FROM clients WHERE LOWER(name) = 'ed hull'").fetchone()
    if not test_client:
        c.execute("INSERT INTO clients (name, pin, programme_path) VALUES (?, ?, ?)",
                  ("Ed Hull", "1234", None))

    # Migrate: add columns to sessions if not present
    for col, coltype in [
        ("device_calories", "INTEGER"),
        ("device_source", "TEXT"),
        ("avg_hr", "INTEGER"),
        ("max_hr", "INTEGER"),
        ("effort_points", "INTEGER"),
        ("z1_mins", "REAL"),
        ("z2_mins", "REAL"),
        ("z3_mins", "REAL"),
        ("z4_mins", "REAL"),
        ("z5_mins", "REAL"),
        ("session_type", "TEXT"),
        ("rpe", "INTEGER"),
    ]:
        try:
            c.execute(f"ALTER TABLE sessions ADD COLUMN {col} {coltype}")
        except Exception:
            pass

    # Migrate: add columns to clients if not present
    for col, coltype in [
        ("api_token", "TEXT"),
        ("max_hr", "INTEGER"),
    ]:
        try:
            c.execute(f"ALTER TABLE clients ADD COLUMN {col} {coltype}")
        except Exception:
            pass

    # Generate tokens for any clients that don't have one
    import secrets
    clients_without_tokens = c.execute("SELECT id FROM clients WHERE api_token IS NULL").fetchall()
    for row in clients_without_tokens:
        token = secrets.token_urlsafe(32)
        c.execute("UPDATE clients SET api_token = ? WHERE id = ?", (token, row["id"]))


    conn.commit()
    conn.close()

    seed_current_week_hybrid()


def get_week_start(date_str=None):
    """Get the Monday of the current (or given) week as YYYY-MM-DD."""
    from datetime import date, timedelta
    d = date.today() if not date_str else datetime.strptime(date_str, "%Y-%m-%d").date()
    monday = d - timedelta(days=d.weekday())
    return monday.strftime("%Y-%m-%d")


def get_weekly_schedule(week_start=None):
    """Get the schedule for a given week (defaults to current week)."""
    if not week_start:
        week_start = get_week_start()
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM weekly_schedule WHERE week_start = ? ORDER BY id",
        (week_start,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_weekly_schedule(week_start, day_of_week, session_name, session_type):
    """Set or update the session for a specific day."""
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM weekly_schedule WHERE week_start = ? AND day_of_week = ?",
        (week_start, day_of_week)
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE weekly_schedule SET session_name=?, session_type=?, updated_at=datetime('now') WHERE week_start=? AND day_of_week=?",
            (session_name, session_type, week_start, day_of_week)
        )
    else:
        conn.execute(
            "INSERT INTO weekly_schedule (week_start, day_of_week, session_name, session_type) VALUES (?,?,?,?)",
            (week_start, day_of_week, session_name, session_type)
        )
    conn.commit()
    conn.close()


def seed_current_week_hybrid():
    """Seed the current week with example sessions. Only runs if no schedule exists for this week."""
    week_start = get_week_start()
    conn = get_db()
    existing = conn.execute("SELECT id FROM weekly_schedule WHERE week_start = ? LIMIT 1", (week_start,)).fetchone()
    conn.close()
    if existing:
        return

    schedule = [
        ("Monday",    "9-9-9",        "BURN"),
        ("Tuesday",   "The Builder",   "STRONG"),
        ("Wednesday", "Triple Hit",    "HYBRID"),
        ("Thursday",  "The Ladder",    "BURN"),
        ("Friday",    "Heavy Metal",   "STRONG"),
        ("Saturday",  "Halo",         "HYBRID"),
    ]
    for day, session, stype in schedule:
        set_weekly_schedule(week_start, day, session, stype)


def get_all_clients():
    conn = get_db()
    rows = conn.execute("SELECT id, name FROM clients ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_client_by_name_pin(name: str, pin: str):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM clients WHERE LOWER(name) = LOWER(?) AND pin = ?",
        (name, pin)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_client_by_id(client_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_coach_by_pin(pin: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM coaches WHERE pin = ?", (pin,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_last_session(client_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM sessions WHERE client_id = ? ORDER BY date DESC, id DESC LIMIT 1",
        (client_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_client_sessions(client_id: int, limit: int = 50):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM sessions WHERE client_id = ? ORDER BY date DESC, id DESC LIMIT ?",
        (client_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_session_by_id(session_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_session(client_id: int, programme_day: str, notes: str = ""):
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    c = conn.cursor()
    c.execute(
        "INSERT INTO sessions (client_id, date, programme_day, notes) VALUES (?, ?, ?, ?)",
        (client_id, today, programme_day, notes)
    )
    session_id = c.lastrowid
    conn.commit()
    conn.close()
    return session_id


def complete_session(session_id: int, notes: str = ""):
    conn = get_db()
    conn.execute(
        "UPDATE sessions SET completed_at = datetime('now'), notes = ? WHERE id = ?",
        (notes, session_id)
    )
    conn.commit()
    conn.close()


def get_session_logs(session_id: int):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM exercise_logs WHERE session_id = ? ORDER BY logged_at",
        (session_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def log_set(session_id: int, exercise_name: str, label: str, set_number: int,
            reps: int = None, weight_kg: float = None, duration_seconds: int = None,
            calories: int = None, rpe: int = None, notes: str = ""):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """INSERT INTO exercise_logs
           (session_id, exercise_name, label, set_number, reps, weight_kg,
            duration_seconds, calories, rpe, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, exercise_name, label, set_number, reps, weight_kg,
         duration_seconds, calories, rpe, notes)
    )
    log_id = c.lastrowid
    conn.commit()
    conn.close()
    return log_id


def get_exercise_history(client_id: int, exercise_name: str, limit: int = 10):
    """Get previous logs for a specific exercise across all sessions for this client."""
    conn = get_db()
    rows = conn.execute(
        """SELECT el.*, s.date, s.programme_day
           FROM exercise_logs el
           JOIN sessions s ON el.session_id = s.id
           WHERE s.client_id = ? AND LOWER(el.exercise_name) = LOWER(?)
           ORDER BY s.date DESC, el.set_number ASC
           LIMIT ?""",
        (client_id, exercise_name, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_exercise_pb(client_id: int, exercise_name: str):
    """Get personal best weight and reps for an exercise."""
    conn = get_db()
    row = conn.execute(
        """SELECT MAX(weight_kg) as best_weight, MAX(reps) as best_reps
           FROM exercise_logs el
           JOIN sessions s ON el.session_id = s.id
           WHERE s.client_id = ? AND LOWER(el.exercise_name) = LOWER(?)""",
        (client_id, exercise_name)
    ).fetchone()
    conn.close()
    return dict(row) if row else {"best_weight": None, "best_reps": None}


def get_last_session_exercise(client_id: int, exercise_name: str):
    """Get all sets from the most recent session this exercise was logged."""
    conn = get_db()
    # Find the most recent session containing this exercise
    row = conn.execute(
        """SELECT s.id, s.date FROM exercise_logs el
           JOIN sessions s ON el.session_id = s.id
           WHERE s.client_id = ? AND LOWER(el.exercise_name) = LOWER(?)
           ORDER BY s.date DESC, s.id DESC LIMIT 1""",
        (client_id, exercise_name)
    ).fetchone()
    if not row:
        conn.close()
        return []
    last_session_id = row["id"]
    rows = conn.execute(
        """SELECT * FROM exercise_logs
           WHERE session_id = ? AND LOWER(exercise_name) = LOWER(?)
           ORDER BY set_number""",
        (last_session_id, exercise_name)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_clients_with_stats():
    """For coach dashboard — all clients with last session info."""
    conn = get_db()
    rows = conn.execute(
        """SELECT c.id, c.name, c.programme_path,
                  MAX(s.date) as last_session_date,
                  COUNT(s.id) as total_sessions
           FROM clients c
           LEFT JOIN sessions s ON s.client_id = c.id
           GROUP BY c.id
           ORDER BY c.name"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_sessions_this_week(client_id: int):
    conn = get_db()
    rows = conn.execute(
        """SELECT * FROM sessions
           WHERE client_id = ?
           AND date >= date('now', 'weekday 0', '-7 days')
           ORDER BY date DESC""",
        (client_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_manual_pb(client_id: int, exercise_name: str, weight_kg=None, reps=None,
                   distance_m=None, time_seconds=None, notes="", date_set=None):
    conn = get_db()
    conn.execute(
        """INSERT INTO personal_bests
           (client_id, exercise_name, weight_kg, reps, distance_m, time_seconds, notes, date_set)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (client_id, exercise_name.strip(), weight_kg, reps, distance_m, time_seconds,
         notes, date_set or datetime.now().strftime("%Y-%m-%d"))
    )
    conn.commit()
    conn.close()


def get_all_pbs(client_id: int):
    """Get all PBs — combining manual entries and auto-tracked from exercise logs."""
    conn = get_db()
    # Manual PBs
    manual = conn.execute(
        """SELECT exercise_name, weight_kg, reps, distance_m, time_seconds, notes, date_set, 'manual' as source
           FROM personal_bests WHERE client_id = ? ORDER BY date_set DESC""",
        (client_id,)
    ).fetchall()
    # Auto PBs from exercise logs
    auto = conn.execute(
        """SELECT el.exercise_name, MAX(el.weight_kg) as weight_kg, MAX(el.reps) as reps,
                  NULL as distance_m, NULL as time_seconds, '' as notes,
                  MAX(s.date) as date_set, 'auto' as source
           FROM exercise_logs el
           JOIN sessions s ON el.session_id = s.id
           WHERE s.client_id = ? AND el.weight_kg IS NOT NULL
           GROUP BY el.exercise_name""",
        (client_id,)
    ).fetchall()
    conn.close()
    # Merge — manual overrides auto for same exercise
    manual_names = {r["exercise_name"].lower() for r in manual}
    combined = [dict(r) for r in manual]
    for r in auto:
        if r["exercise_name"].lower() not in manual_names:
            combined.append(dict(r))
    combined.sort(key=lambda x: x["exercise_name"].lower())
    return combined


def delete_manual_pb(client_id: int, pb_id: int):
    conn = get_db()
    conn.execute("DELETE FROM personal_bests WHERE id = ? AND client_id = ?", (pb_id, client_id))
    conn.commit()
    conn.close()


def get_recent_pbs(client_id: int, limit: int = 5):
    """Get recently set personal bests (by weight) for a client."""
    conn = get_db()
    rows = conn.execute(
        """SELECT el.exercise_name, MAX(el.weight_kg) as best_weight, MAX(el.reps) as best_reps,
                  s.date
           FROM exercise_logs el
           JOIN sessions s ON el.session_id = s.id
           WHERE s.client_id = ? AND el.weight_kg IS NOT NULL
           GROUP BY el.exercise_name
           ORDER BY s.date DESC
           LIMIT ?""",
        (client_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_client(name: str, pin: str, programme_path: str = None):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO clients (name, pin, programme_path) VALUES (?, ?, ?)",
        (name, pin, programme_path)
    )
    client_id = c.lastrowid
    conn.commit()
    conn.close()
    return client_id


def update_session_device_calories(session_id: int, device_calories: int, device_source: str):
    conn = get_db()
    conn.execute(
        "UPDATE sessions SET device_calories = ?, device_source = ? WHERE id = ?",
        (device_calories, device_source, session_id)
    )
    conn.commit()
    conn.close()


def log_steps(client_id: int, date: str, steps: int, source: str = "Manual"):
    conn = get_db()
    conn.execute(
        """INSERT INTO daily_steps (client_id, date, steps, source)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(client_id, date) DO UPDATE SET steps=excluded.steps, source=excluded.source, logged_at=datetime('now')""",
        (client_id, date, steps, source)
    )
    conn.commit()
    conn.close()


def get_today_steps(client_id: int):
    from datetime import date
    today = date.today().strftime("%Y-%m-%d")
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM daily_steps WHERE client_id = ? AND date = ?",
        (client_id, today)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_steps_daily(client_id: int, days: int = 7):
    """Last N days of steps, filling gaps with 0."""
    from datetime import date, timedelta
    conn = get_db()
    rows = conn.execute(
        "SELECT date, steps FROM daily_steps WHERE client_id = ? AND date >= date('now', ?) ORDER BY date ASC",
        (client_id, f"-{days-1} days")
    ).fetchall()
    conn.close()
    data = {r["date"]: r["steps"] for r in rows}
    result = []
    for i in range(days - 1, -1, -1):
        d = (date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
        result.append({"date": d, "steps": data.get(d, 0)})
    return result


def get_steps_weekly(client_id: int, weeks: int = 8):
    """Aggregate steps by week (Mon–Sun) for last N weeks."""
    from datetime import date, timedelta
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    conn = get_db()
    result = []
    for i in range(weeks - 1, -1, -1):
        week_mon = monday - timedelta(weeks=i)
        week_sun = week_mon + timedelta(days=6)
        row = conn.execute(
            """SELECT COALESCE(SUM(steps), 0) as total
               FROM daily_steps
               WHERE client_id = ? AND date >= ? AND date <= ?""",
            (client_id, week_mon.strftime("%Y-%m-%d"), week_sun.strftime("%Y-%m-%d"))
        ).fetchone()
        result.append({
            "label": week_mon.strftime("%-d %b"),
            "total": row["total"] if row else 0
        })
    conn.close()
    return result


def get_steps_monthly(client_id: int, months: int = 6):
    """Aggregate steps by calendar month for last N months."""
    from datetime import date
    import calendar
    today = date.today()
    conn = get_db()
    result = []
    for i in range(months - 1, -1, -1):
        # Calculate year/month going back i months
        month = today.month - i
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        month_start = f"{year}-{month:02d}-01"
        last_day = calendar.monthrange(year, month)[1]
        month_end = f"{year}-{month:02d}-{last_day:02d}"
        row = conn.execute(
            """SELECT COALESCE(SUM(steps), 0) as total
               FROM daily_steps
               WHERE client_id = ? AND date >= ? AND date <= ?""",
            (client_id, month_start, month_end)
        ).fetchone()
        import calendar as cal
        result.append({
            "label": f"{cal.month_abbr[month]} {year}",
            "total": row["total"] if row else 0
        })
    conn.close()
    return result


def get_session_calories_history(client_id: int, limit: int = 10):
    conn = get_db()
    rows = conn.execute(
        """SELECT date, programme_day, device_calories, device_source
           FROM sessions
           WHERE client_id = ? AND device_calories IS NOT NULL
           ORDER BY date DESC, id DESC
           LIMIT ?""",
        (client_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_session_calories_pb(client_id: int):
    conn = get_db()
    row = conn.execute(
        """SELECT MAX(device_calories) as pb_cals, date as pb_date
           FROM sessions
           WHERE client_id = ? AND device_calories IS NOT NULL""",
        (client_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else {"pb_cals": None, "pb_date": None}


def delete_log(log_id: int):
    conn = get_db()
    conn.execute("DELETE FROM exercise_logs WHERE id = ?", (log_id,))
    conn.commit()
    conn.close()


def log_group_session(client_id: int, session_name: str, session_type: str,
                      competition_metric: str, result_value: float, unit: str, notes: str = ""):
    conn = get_db()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    # Create a session record
    c.execute(
        "INSERT INTO sessions (client_id, date, programme_day, session_type, notes) VALUES (?, ?, ?, ?, ?)",
        (client_id, today, session_name, session_type, notes)
    )
    session_id = c.lastrowid
    # Log the competition result as an exercise log
    c.execute(
        """INSERT INTO exercise_logs (session_id, exercise_name, label, set_number,
           reps, weight_kg, duration_seconds, calories, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, competition_metric, "COMPETITION", 1,
         int(result_value) if unit == "reps" else None,
         result_value if unit == "kg" else None,
         int(result_value) if unit == "seconds" else None,
         int(result_value) if unit == "cals" else None,
         f"Unit: {unit} | Session: {session_name}" + (f" | {notes}" if notes else ""))
    )
    conn.commit()
    conn.close()
    return session_id


def get_all_group_sessions_for_client(client_id: int, limit: int = 60):
    """Get all group sessions for a client, with type and competition result."""
    conn = get_db()
    rows = conn.execute(
        """SELECT s.id, s.date, s.programme_day as session_name, s.session_type,
                  s.device_calories, s.avg_hr, s.max_hr,
                  el.exercise_name as competition_metric,
                  el.reps, el.weight_kg, el.calories as comp_cals, el.notes as el_notes
           FROM sessions s
           LEFT JOIN exercise_logs el ON el.session_id = s.id AND el.label = 'COMPETITION'
           WHERE s.client_id = ? AND s.programme_day IS NOT NULL
           ORDER BY s.date DESC
           LIMIT ?""",
        (client_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_group_session_history(client_id: int, session_name: str, limit: int = 10):
    """Get previous results for a specific group session."""
    conn = get_db()
    rows = conn.execute(
        """SELECT s.date, el.exercise_name, el.reps, el.weight_kg,
                  el.duration_seconds, el.calories, el.notes, s.notes as session_notes
           FROM sessions s
           JOIN exercise_logs el ON el.session_id = s.id
           WHERE s.client_id = ? AND s.programme_day = ? AND el.label = 'COMPETITION'
           ORDER BY s.date DESC
           LIMIT ?""",
        (client_id, session_name, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_client_by_token(token: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM clients WHERE api_token = ?", (token,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_or_create_client_token(client_id: int) -> str:
    import secrets
    conn = get_db()
    row = conn.execute("SELECT api_token FROM clients WHERE id = ?", (client_id,)).fetchone()
    if row and row["api_token"]:
        conn.close()
        return row["api_token"]
    token = secrets.token_urlsafe(32)
    conn.execute("UPDATE clients SET api_token = ? WHERE id = ?", (token, client_id))
    conn.commit()
    conn.close()
    return token


def update_client_max_hr(client_id: int, max_hr: int):
    conn = get_db()
    conn.execute("UPDATE clients SET max_hr = ? WHERE id = ?", (max_hr, client_id))
    conn.commit()
    conn.close()


def get_effort_points_this_month(client_id: int) -> int:
    from datetime import date
    today = date.today()
    month_start = f"{today.year}-{today.month:02d}-01"
    conn = get_db()
    row = conn.execute(
        """SELECT COALESCE(SUM(effort_points), 0) as total
           FROM sessions
           WHERE client_id = ? AND date >= ? AND effort_points IS NOT NULL""",
        (client_id, month_start)
    ).fetchone()
    conn.close()
    return row["total"] if row else 0


def log_workout_from_ios(client_id: int, effort_points: int, avg_hr: int, max_hr: int,
                         calories: int, duration_seconds: int,
                         z1_mins: float, z2_mins: float, z3_mins: float,
                         z4_mins: float, z5_mins: float,
                         workout_date: str = None) -> int:
    from datetime import date as dt
    today = workout_date or dt.today().strftime("%Y-%m-%d")
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """INSERT INTO sessions (client_id, date, programme_day, completed_at,
           avg_hr, max_hr, device_calories, effort_points,
           z1_mins, z2_mins, z3_mins, z4_mins, z5_mins)
           VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (client_id, today, "iOS Workout", avg_hr, max_hr, calories,
         effort_points, z1_mins, z2_mins, z3_mins, z4_mins, z5_mins)
    )
    session_id = c.lastrowid
    conn.commit()
    conn.close()
    return session_id


def update_session_hr(session_id: int, avg_hr: int, max_hr: int):
    conn = get_db()
    conn.execute(
        "UPDATE sessions SET avg_hr = ?, max_hr = ? WHERE id = ?",
        (avg_hr, max_hr, session_id)
    )
    conn.commit()
    conn.close()


def log_recovery(client_id: int, session_id: int, date: str,
                 energy: int, soreness: int, sleep: int, notes: str = ""):
    conn = get_db()
    conn.execute(
        """INSERT INTO recovery_logs (client_id, session_id, date, energy, soreness, sleep, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (client_id, session_id, date, energy, soreness, sleep, notes)
    )
    conn.commit()
    conn.close()


def get_recovery_history(client_id: int, limit: int = 10):
    conn = get_db()
    rows = conn.execute(
        """SELECT r.*, s.programme_day FROM recovery_logs r
           LEFT JOIN sessions s ON s.id = r.session_id
           WHERE r.client_id = ? ORDER BY r.date DESC, r.id DESC LIMIT ?""",
        (client_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recovery_for_session(session_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM recovery_logs WHERE session_id = ? ORDER BY id DESC LIMIT 1",
        (session_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def log_body(client_id: int, date: str, weight_kg: float = None,
             waist_cm: float = None, energy: int = None, notes: str = ""):
    conn = get_db()
    conn.execute(
        """INSERT INTO body_logs (client_id, date, weight_kg, waist_cm, energy, notes)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(client_id, date) DO UPDATE SET
             weight_kg=excluded.weight_kg,
             waist_cm=excluded.waist_cm,
             energy=excluded.energy,
             notes=excluded.notes,
             logged_at=datetime('now')""",
        (client_id, date, weight_kg, waist_cm, energy, notes)
    )
    conn.commit()
    conn.close()


def get_latest_body(client_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM body_logs WHERE client_id = ? ORDER BY date DESC LIMIT 1",
        (client_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_body_history(client_id: int, limit: int = 12):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM body_logs WHERE client_id = ? ORDER BY date ASC LIMIT ?",
        (client_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_coach_note(client_id: int, coach_id: int, note: str):
    conn = get_db()
    conn.execute(
        """INSERT INTO coach_notes (client_id, coach_id, note)
           VALUES (?, ?, ?)
           ON CONFLICT(client_id) DO UPDATE SET
             note=excluded.note,
             coach_id=excluded.coach_id,
             updated_at=datetime('now')""",
        (client_id, coach_id, note)
    )
    conn.commit()
    conn.close()


def get_coach_note(client_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM coach_notes WHERE client_id = ?",
        (client_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_session_streak(client_id: int) -> int:
    """Consecutive weeks with at least 1 completed session (current week given grace)."""
    from datetime import date, timedelta
    conn = get_db()
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    streak = 0
    for i in range(52):
        week_mon = monday - timedelta(weeks=i)
        week_sun = week_mon + timedelta(days=6)
        row = conn.execute(
            """SELECT COUNT(*) as cnt FROM sessions
               WHERE client_id = ? AND date >= ? AND date <= ? AND completed_at IS NOT NULL""",
            (client_id, week_mon.strftime("%Y-%m-%d"), week_sun.strftime("%Y-%m-%d"))
        ).fetchone()
        has_session = row and row["cnt"] > 0
        if has_session:
            streak += 1
        elif streak == 0 and i == 0:
            continue  # grace: current week not yet done
        else:
            break
    conn.close()
    return streak


def get_weekly_session_counts(client_id: int, weeks: int = 8) -> list:
    """Sessions completed per week for last N weeks."""
    from datetime import date, timedelta
    conn = get_db()
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    result = []
    for i in range(weeks - 1, -1, -1):
        week_mon = monday - timedelta(weeks=i)
        week_sun = week_mon + timedelta(days=6)
        row = conn.execute(
            """SELECT COUNT(*) as cnt FROM sessions
               WHERE client_id = ? AND date >= ? AND date <= ? AND completed_at IS NOT NULL""",
            (client_id, week_mon.strftime("%Y-%m-%d"), week_sun.strftime("%Y-%m-%d"))
        ).fetchone()
        result.append({
            "label": week_mon.strftime("%-d %b"),
            "count": row["cnt"] if row else 0
        })
    conn.close()
    return result


def get_pr_check(client_id: int, exercise_name: str, weight_kg: float = None, reps: int = None):
    """Returns True if the given values beat the existing personal best."""
    conn = get_db()
    row = conn.execute(
        """SELECT MAX(el.weight_kg) as best_weight, MAX(el.reps) as best_reps
           FROM exercise_logs el
           JOIN sessions s ON el.session_id = s.id
           WHERE s.client_id = ? AND LOWER(el.exercise_name) = LOWER(?)""",
        (client_id, exercise_name)
    ).fetchone()
    conn.close()
    if not row:
        return False
    if weight_kg and row["best_weight"] and weight_kg > row["best_weight"]:
        return True
    if reps and row["best_reps"] and reps > row["best_reps"] and not weight_kg:
        return True
    return False


def get_group_session_pb(client_id: int, session_name: str, unit: str):
    """Get personal best for a group session competition metric."""
    conn = get_db()
    if unit == "cals":
        field = "el.calories"
    elif unit == "kg":
        field = "el.weight_kg"
    elif unit == "reps":
        field = "el.reps"
    elif unit == "seconds":
        field = "el.duration_seconds"
    else:
        field = "el.calories"

    row = conn.execute(
        f"""SELECT MAX({field}) as pb_value, s.date as pb_date
            FROM sessions s
            JOIN exercise_logs el ON el.session_id = s.id
            WHERE s.client_id = ? AND s.programme_day = ? AND el.label = 'COMPETITION'""",
        (client_id, session_name)
    ).fetchone()
    conn.close()
    return dict(row) if row else {"pb_value": None, "pb_date": None}


def save_push_subscription(client_id: int, subscription_json: str):
    conn = get_db()
    conn.execute(
        "INSERT INTO push_subscriptions (client_id, subscription_json) VALUES (?, ?) "
        "ON CONFLICT(client_id) DO UPDATE SET subscription_json=excluded.subscription_json, created_at=datetime('now')",
        (client_id, subscription_json)
    )
    conn.commit()
    conn.close()


def get_all_push_subscriptions():
    conn = get_db()
    rows = conn.execute(
        "SELECT ps.client_id, ps.subscription_json, c.name FROM push_subscriptions ps "
        "JOIN clients c ON c.id = ps.client_id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_push_subscription(client_id: int):
    conn = get_db()
    conn.execute("DELETE FROM push_subscriptions WHERE client_id = ?", (client_id,))
    conn.commit()
    conn.close()


def save_client_profile(client_id: int, data: dict):
    conn = get_db()
    conn.execute("""
        INSERT INTO client_profiles (
            client_id, dob, sex, height_cm, weight_kg, resting_hr, max_hr_manual,
            goal, fitness_level, training_freq, injuries
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(client_id) DO UPDATE SET
            dob=excluded.dob, sex=excluded.sex, height_cm=excluded.height_cm,
            weight_kg=excluded.weight_kg, resting_hr=excluded.resting_hr,
            max_hr_manual=excluded.max_hr_manual, goal=excluded.goal,
            fitness_level=excluded.fitness_level, training_freq=excluded.training_freq,
            injuries=excluded.injuries, updated_at=datetime('now')
    """, (
        client_id,
        data.get("dob"), data.get("sex"), data.get("height_cm"), data.get("weight_kg"),
        data.get("resting_hr"), data.get("max_hr_manual"), data.get("goal"),
        data.get("fitness_level"), data.get("training_freq"), data.get("injuries"),
    ))
    conn.commit()
    conn.close()


def get_client_profile(client_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM client_profiles WHERE client_id = ?", (client_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_coaches():
    conn = get_db()
    rows = conn.execute("SELECT id, name FROM coaches ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_client(client_id: int):
    conn = get_db()
    conn.execute("DELETE FROM exercise_logs WHERE session_id IN (SELECT id FROM sessions WHERE client_id = ?)", (client_id,))
    conn.execute("DELETE FROM sessions WHERE client_id = ?", (client_id,))
    conn.execute("DELETE FROM daily_steps WHERE client_id = ?", (client_id,))
    conn.execute("DELETE FROM body_logs WHERE client_id = ?", (client_id,))
    conn.execute("DELETE FROM recovery_logs WHERE session_id IN (SELECT id FROM sessions WHERE client_id = ?)", (client_id,))
    conn.execute("DELETE FROM coach_notes WHERE client_id = ?", (client_id,))
    conn.execute("DELETE FROM personal_bests WHERE client_id = ?", (client_id,))
    conn.execute("DELETE FROM push_subscriptions WHERE client_id = ?", (client_id,))
    conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
    conn.commit()
    conn.close()
