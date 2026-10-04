from app.database import init_db

from .conftest import add_book, edit_log, log_ids, log_read, read_status


def test_new_book_is_unread(db):
    assert read_status(db, add_book(db)) == "Unread"


def test_only_dnf_readings_is_dnf(client, db):
    ub = add_book(db)
    log_read(client, ub, "2026-01-01", dnf=True)
    log_read(client, ub, "2026-02-01", dnf=True)
    assert read_status(db, ub) == "DNF"


def test_any_finished_reading_is_read_regardless_of_order(client, db):
    dnf_first = add_book(db, "A")
    log_read(client, dnf_first, "2026-01-01", dnf=True)
    log_read(client, dnf_first, "2026-02-01")

    finished_first = add_book(db, "B")
    log_read(client, finished_first, "2026-01-01")
    log_read(client, finished_first, "2026-02-01", dnf=True)

    assert read_status(db, dnf_first) == "Read"
    assert read_status(db, finished_first) == "Read"


def test_editing_dnf_flag_updates_status(client, db):
    ub = add_book(db)
    log_read(client, ub, "2026-01-01")
    (log_id,) = log_ids(db, ub)

    assert edit_log(client, log_id, "2026-01-01", dnf=True).status_code == 303
    assert read_status(db, ub) == "DNF"

    edit_log(client, log_id, "2026-01-01", dnf=False)
    assert read_status(db, ub) == "Read"


def test_deleting_readings_updates_status(client, db):
    ub = add_book(db)
    log_read(client, ub, "2026-01-01", dnf=True)
    log_read(client, ub, "2026-02-01")
    dnf_log, finished_log = log_ids(db, ub)

    client.post(f"/log/{finished_log}/delete", follow_redirects=False)
    assert read_status(db, ub) == "DNF"

    client.post(f"/log/{dnf_log}/delete", follow_redirects=False)
    assert read_status(db, ub) == "Unread"


def test_startup_repairs_stale_status(client, db):
    ub = add_book(db)
    log_read(client, ub, "2026-01-01", dnf=True)
    db.execute("UPDATE user_books SET read_status = 'Read'")
    db.commit()

    init_db()
    assert read_status(db, ub) == "DNF"


def test_hours_are_optional(client, db):
    ub = add_book(db)
    assert log_read(client, ub, "2026-01-01", hours="").status_code == 303
    (log_id,) = log_ids(db, ub)
    assert edit_log(client, log_id, "2026-01-01", hours="").status_code == 303
    assert db.execute("SELECT hours_read FROM reading_logs").fetchone()[0] == 0


def test_editing_missing_log_is_404(client):
    assert edit_log(client, 999, "2026-01-01").status_code == 404
