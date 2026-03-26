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

    conn.commit()
    conn.close()


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


def delete_log(log_id: int):
    conn = get_db()
    conn.execute("DELETE FROM exercise_logs WHERE id = ?", (log_id,))
    conn.commit()
    conn.close()
