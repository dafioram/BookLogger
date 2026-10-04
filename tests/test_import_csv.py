import os
import subprocess
import sys

from app import database

from .conftest import ROOT, add_book


def run_import(data_dir, csv_text):
    """Runs import_csv.py against the test database (it expects ./data/library.db)."""
    work = data_dir.parent
    (work / "in.csv").write_text(csv_text)
    env = {**os.environ, "PYTHONPATH": ROOT}
    return subprocess.run([sys.executable, os.path.join(ROOT, "import_csv.py"), "in.csv"],
                          cwd=work, env=env, capture_output=True, text=True, check=True)


def test_import_dnf_column(db, data_dir):
    # Titles already in the DB, so no metadata lookup hits the network
    dnf_ub = add_book(db, "Gave Up")
    read_ub = add_book(db, "Loved It")
    run_import(data_dir, (
        "Title,Author,Date Finished,Service,DNF\n"
        "Gave Up,A,2025-03-01,Kindle,yes\n"
        "Loved It,A,2025-04-01,Audible,\n"
    ))

    conn = database.get_db_connection()
    status = dict(conn.execute("SELECT id, read_status FROM user_books").fetchall())
    assert status == {dnf_ub: "DNF", read_ub: "Read"}
    dnf_log = conn.execute("SELECT is_dnf, hours_read FROM reading_logs WHERE user_book_id = ?", (dnf_ub,)).fetchone()
    assert tuple(dnf_log) == (1, 0)  # no page-based hour estimate for a DNF
    conn.close()
