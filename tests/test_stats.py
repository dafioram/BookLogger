from .conftest import add_book, log_read


def test_yearly_stats_split_finished_and_dnf(client, db):
    ub = add_book(db, pages=300)
    log_read(client, ub, "2026-02-10", hours="3", dnf=True)
    log_read(client, ub, "2026-05-01", hours="5")
    log_read(client, ub, "2025-05-01", hours="9")  # other year, ignored

    ctx = client.get("/stats?year=2026").context
    assert ctx["data_books"][1] == 0 and ctx["data_books"][4] == 1
    assert ctx["data_dnf"][1] == 1 and sum(ctx["data_dnf"]) == 1
    # DNF time counts toward hours, but not toward pages read
    assert sum(ctx["data_hours"]) == 8
    assert ctx["data_pages"][1] == 0 and ctx["data_pages"][4] == 300


def test_yearly_reading_log_includes_dnf_sessions(client, db):
    ub = add_book(db, "Quit Early")
    log_read(client, ub, "2026-03-01", hours="2", dnf=True)

    resp = client.get("/stats?year=2026")
    (row,) = resp.context["logs"]
    assert row["title"] == "Quit Early" and row["is_dnf"] and row["book_id"] == ub
    assert "DNF" in resp.text


def test_dashboard_counts(client, db):
    ub = add_book(db)
    log_read(client, ub, "2026-01-01", hours="1.5", dnf=True)
    log_read(client, ub, "2026-02-01", hours="4")

    ctx = client.get("/").context
    assert ctx["stats"]["books_read"] == 1
    assert ctx["stats"]["books_dnf"] == 1
    assert ctx["stats"]["total_hours"] == 5.5
    newest, oldest = ctx["recent"]
    assert newest["date_finished"] == "2026-02-01" and oldest["is_dnf"]
    assert newest["book_id"] == ub
