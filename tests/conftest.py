import os
import sqlite3

import pytest

# The app resolves templates/static relative to the working directory
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)

from fastapi.testclient import TestClient  # noqa: E402

import app.database as database  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Points the app at a throwaway database in tmp_path/data/library.db."""
    folder = tmp_path / "data"
    monkeypatch.setattr(database, "DB_FOLDER", str(folder))
    monkeypatch.setattr(database, "DB_PATH", str(folder / "library.db"))
    monkeypatch.setattr(database, "BACKUP_DIR", str(folder / "backups"))
    return folder


@pytest.fixture
def client(data_dir):
    with TestClient(app) as c:  # runs init_db via the lifespan hook
        yield c


@pytest.fixture
def db(client):
    conn = database.get_db_connection()
    yield conn
    conn.close()


def add_book(conn, title="Book", author="Author", pages=300):
    """Inserts a book plus its library entry; returns the user_books id."""
    cur = conn.execute(
        "INSERT INTO books (google_id, title, author, total_pages) VALUES (?, ?, ?, ?)",
        (f"test_{title}", title, author, pages),
    )
    cur = conn.execute("INSERT INTO user_books (book_id) VALUES (?)", (cur.lastrowid,))
    conn.commit()
    return cur.lastrowid


def log_read(client, user_book_id, date, hours="", dnf=False, fmt="Physical", rating=""):
    data = {"date_finished": date, "hours": hours, "format_consumed": fmt, "session_rating": rating}
    if dnf:
        data["is_dnf"] = "true"
    return client.post(f"/book/{user_book_id}/add_log", data=data, follow_redirects=False)


def edit_log(client, log_id, date, hours="", dnf=False, fmt="Physical"):
    data = {"date_finished": date, "hours": hours, "format_consumed": fmt}
    if dnf:
        data["is_dnf"] = "true"
    return client.post(f"/log/{log_id}/edit", data=data, follow_redirects=False)


def read_status(conn, user_book_id):
    return conn.execute("SELECT read_status FROM user_books WHERE id = ?", (user_book_id,)).fetchone()[0]


def log_ids(conn, user_book_id):
    return [r[0] for r in conn.execute("SELECT id FROM reading_logs WHERE user_book_id = ? ORDER BY id", (user_book_id,))]
