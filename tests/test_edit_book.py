from .conftest import add_book, log_read, read_status

FIELDS = {
    "title": "New Title", "author": "New Author", "subtitle": "A Subtitle",
    "series_name": "Saga", "series_index": "2.5", "publication_year": "1999",
    "total_pages": "412", "total_audio_minutes": "630", "publisher": "Pub",
    "language": "fr", "genres": "Fantasy", "isbn13": "9780000000001", "asin": "B00X",
    "goodreads_id": "123", "olid": "OL1W", "cover_url": "https://img/c.jpg",
    "summary": "Updated synopsis",
}


def book_row(conn, user_book_id):
    return dict(conn.execute(
        "SELECT b.* FROM user_books ub JOIN books b ON ub.book_id = b.id WHERE ub.id = ?",
        (user_book_id,)).fetchone())


def test_book_page_links_to_edit(client, db):
    ub = add_book(db)
    assert f'href="/book/{ub}/edit"' in client.get(f"/book/{ub}").text


def test_edit_page_is_prefilled(client, db):
    ub = add_book(db, "Dune", pages=896)
    html = client.get(f"/book/{ub}/edit").text
    assert 'value="Dune"' in html and 'value="896"' in html
    assert f'action="/book/{ub}/edit"' in html


def test_edit_updates_metadata(client, db):
    ub = add_book(db)
    resp = client.post(f"/book/{ub}/edit", data=FIELDS, follow_redirects=False)
    assert resp.status_code == 303 and resp.headers["location"] == f"/book/{ub}"

    row = book_row(db, ub)
    assert row["title"] == "New Title" and row["summary"] == "Updated synopsis"
    assert row["total_pages"] == 412 and row["total_audio_minutes"] == 630
    assert row["series_index"] == 2.5 and row["publication_year"] == "1999"
    assert row["isbn13"] == "9780000000001" and row["cover_url"] == "https://img/c.jpg"


def test_edit_blank_optional_fields_clear_them(client, db):
    ub = add_book(db, pages=300)
    client.post(f"/book/{ub}/edit", data={**FIELDS, "total_pages": "", "summary": "", "series_index": "", "cover_url": ""})
    row = book_row(db, ub)
    assert row["total_pages"] is None and row["summary"] is None and row["series_index"] is None
    assert row["cover_url"] == "/static/placeholder.png"  # matches the form's "leave blank" hint


def test_edit_keeps_readings_and_status(client, db):
    ub = add_book(db)
    log_read(client, ub, "2026-01-01", dnf=True)
    client.post(f"/book/{ub}/edit", data=FIELDS)
    assert read_status(db, ub) == "DNF"
    assert db.execute("SELECT COUNT(*) FROM reading_logs WHERE user_book_id = ?", (ub,)).fetchone()[0] == 1


def test_edit_missing_book_is_404(client):
    assert client.get("/book/999/edit").status_code == 404
    assert client.post("/book/999/edit", data=FIELDS).status_code == 404


def test_add_manual_uses_shared_fields(client, db):
    resp = client.post("/add_manual", data={
        "title": "Manual", "author": "Me", "publication_year": "2020", "total_pages": "250",
        "total_audio_minutes": "", "summary": "Hand entered", "format_owned": "Kindle", "status": "On Deck",
    }, follow_redirects=False)
    ub = int(resp.headers["location"].rsplit("/", 1)[1])
    row = book_row(db, ub)
    assert (row["title"], row["publication_year"], row["total_pages"], row["summary"]) == ("Manual", "2020", 250, "Hand entered")
    assert row["cover_url"] == "/static/placeholder.png"
    assert db.execute("SELECT shelf_status FROM user_books WHERE id = ?", (ub,)).fetchone()[0] == "On Deck"
