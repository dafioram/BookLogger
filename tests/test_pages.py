import pytest

import app.main as main

from .conftest import add_book, log_ids, log_read


def test_pages_render(client, db):
    ub = add_book(db, author="Someone")
    log_read(client, ub, "2026-01-01", rating="4")
    (log_id,) = log_ids(db, ub)
    for url in ["/", "/stats", "/stats?year=2020", "/library", "/top_books", "/search",
                "/add_manual", "/author/Someone", f"/book/{ub}", f"/log/{log_id}/edit"]:
        assert client.get(url).status_code == 200, url


def test_missing_book_is_404(client):
    assert client.get("/book/999").status_code == 404


@pytest.fixture
def mismatched_ids(db):
    """A library entry whose user_books.id differs from its books.id."""
    db.execute("INSERT INTO books (google_id, title) VALUES ('not_in_library', 'Unowned')")
    db.commit()
    ub = add_book(db, "Owned")
    book_id = db.execute("SELECT book_id FROM user_books WHERE id = ?", (ub,)).fetchone()[0]
    assert ub != book_id
    return ub, book_id


def test_top_books_link_to_library_entry(client, db, mismatched_ids):
    ub, _ = mismatched_ids
    log_read(client, ub, "2026-01-01", rating="5")
    (book,) = client.get("/top_books").context["books"]
    assert book["id"] == ub


def test_cover_routes_use_library_entry_id(client, db, mismatched_ids, monkeypatch):
    ub, book_id = mismatched_ids
    queries = []

    async def fake_search(query, **kwargs):
        queries.append(query)
        return [{"cover": "https://img/a.jpg"}, {"cover": "https://img/a.jpg"}, {"cover": "/static/placeholder.png"}]

    monkeypatch.setattr(main, "search_aggregated", fake_search)

    resp = client.get(f"/book/{ub}/cover_options")
    assert queries == ["Owned Author"]
    assert resp.context["covers"] == ["https://img/a.jpg"]
    assert f"/book/{ub}/set_cover" in resp.text

    resp = client.post(f"/book/{ub}/set_cover", data={"new_cover_url": "https://img/new.jpg"}, follow_redirects=False)
    assert resp.headers["location"] == f"/book/{ub}"
    covers = dict(db.execute("SELECT id, cover_url FROM books").fetchall())
    assert covers[book_id] == "https://img/new.jpg"
    assert covers[book_id - 1] != "https://img/new.jpg"
