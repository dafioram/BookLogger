from fastapi import FastAPI, Request, Form, HTTPException, Body
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from starlette.exceptions import HTTPException as StarletteHTTPException
import httpx
import json
import math
import os
import uuid
import urllib.parse
from datetime import date
from typing import List

# --- LOCAL IMPORTS ---
from .database import init_db, get_db_connection, backup_database
from .metadata import search_aggregated
from .utils import (
    format_minutes, 
    format_runtime, 
    process_book_row, 
    recalculate_book_rating, 
    RELATION_MAP
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)
os.makedirs("app/static", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

templates.env.filters["format_minutes"] = format_minutes
templates.env.filters["format_runtime"] = format_runtime
templates.env.filters["urlencode"] = urllib.parse.quote_plus

@app.exception_handler(404)
async def custom_404_handler(request: Request, exc: StarletteHTTPException):
    return templates.TemplateResponse("404.html", {"request": request}, status_code=404)

# --- DASHBOARD ROUTE (Updated Ordering) ---
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    conn = get_db_connection()
    
    # 1. Lifetime Stats
    stats = conn.execute("""
        SELECT COUNT(DISTINCT l.id) as books_read, SUM(l.hours_read) as total_hours
        FROM reading_logs l WHERE l.is_dnf = 0
    """).fetchone()
    
    # 2. Total Library Count
    library_count = conn.execute("SELECT COUNT(*) FROM user_books").fetchone()[0]
    
    # 3. On Deck (List) - UPDATED TO USE ORDER COLUMN
    # We order by on_deck_order first. Nulls (new items) will naturally float to the top or bottom depending on DB,
    # so we add a secondary sort by ID to keep it stable.
    on_deck_rows = conn.execute("""
        SELECT ub.id, b.title, b.cover_url, b.cover_path
        FROM user_books ub 
        JOIN books b ON ub.book_id = b.id 
        WHERE ub.shelf_status = 'On Deck'
        ORDER BY ub.on_deck_order ASC, ub.id DESC
    """).fetchall()
    on_deck = [process_book_row(r) for r in on_deck_rows]
    
    on_deck_count = len(on_deck)
    
    # 4. Recent Logs
    recent = conn.execute("""
        SELECT b.title, l.date_finished, l.hours_read, l.is_dnf
        FROM reading_logs l
        JOIN user_books ub ON l.user_book_id = ub.id
        JOIN books b ON ub.book_id = b.id
        ORDER BY l.date_finished DESC
        LIMIT 10
    """).fetchall()
    
    conn.close()
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "stats": stats, 
        "total_books": library_count, 
        "on_deck": on_deck, 
        "on_deck_count": on_deck_count, 
        "recent": recent
    })

# --- NEW ENDPOINT: REORDER ON DECK ---
@app.post("/api/reorder_on_deck")
async def reorder_on_deck(ordered_ids: List[int] = Body(...)):
    """
    Receives a JSON list of user_book IDs in the desired order.
    Example: [5, 12, 3, 8]
    """
    conn = get_db_connection()
    
    # Update each book with its new index
    for index, user_book_id in enumerate(ordered_ids):
        conn.execute("UPDATE user_books SET on_deck_order = ? WHERE id = ?", (index, user_book_id))
        
    conn.commit()
    conn.close()
    return {"status": "success"}

# ... (Rest of your routes: /stats, /library, /search, etc. remain unchanged) ...
# Be sure to keep all the other routes from your previous main.py!
# I am omitting them here for brevity, but they should be included below.

@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request, year: int = None):
    # ... (Same as before) ...
    conn = get_db_connection()
    current_year = date.today().year
    selected_year = year if year else current_year
    
    years_rows = conn.execute("SELECT DISTINCT strftime('%Y', date_finished) as y FROM reading_logs WHERE date_finished IS NOT NULL ORDER BY y DESC").fetchall()
    available_years = [int(r['y']) for r in years_rows if r['y']]
    if current_year not in available_years: available_years.insert(0, current_year)
    
    monthly_query = """
        SELECT strftime('%m', l.date_finished) as month, COUNT(DISTINCT l.id) as books, SUM(l.hours_read) as hours, SUM(b.total_pages) as pages
        FROM reading_logs l JOIN user_books ub ON l.user_book_id = ub.id JOIN books b ON ub.book_id = b.id
        WHERE strftime('%Y', l.date_finished) = ? AND l.is_dnf = 0
        GROUP BY month ORDER BY month
    """
    rows = conn.execute(monthly_query, (str(selected_year),)).fetchall()
    labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    data_books = [0] * 12
    data_hours = [0] * 12
    data_pages = [0] * 12
    for r in rows:
        idx = int(r['month']) - 1
        data_books[idx] = r['books']
        data_hours[idx] = round(r['hours'], 1)
        data_pages[idx] = r['pages']

    format_data = conn.execute("SELECT format_consumed, COUNT(*) as count FROM reading_logs WHERE strftime('%Y', date_finished) = ? GROUP BY format_consumed", (str(selected_year),)).fetchall()
    format_labels = [row['format_consumed'] for row in format_data]
    format_counts = [row['count'] for row in format_data]

    logs_rows = conn.execute("""
        SELECT b.id as book_id, b.title, b.author, b.cover_url, b.cover_path, ub.effective_user_rating as user_rating, rl.date_finished, rl.format_consumed, rl.is_borrowed, rl.hours_read
        FROM reading_logs rl JOIN user_books ub ON rl.user_book_id = ub.id JOIN books b ON ub.book_id = b.id
        WHERE strftime('%Y', rl.date_finished) = ? ORDER BY rl.date_finished ASC
    """, (str(selected_year),)).fetchall()
    logs = []
    for row in logs_rows:
        r = dict(row)
        if r.get('cover_path'): r['cover_url'] = r['cover_path']
        logs.append(r)

    conn.close()
    return templates.TemplateResponse("stats.html", {"request": request, "selected_year": selected_year, "available_years": available_years, "labels": labels, "data_books": data_books, "data_hours": data_hours, "data_pages": data_pages, "format_labels": format_labels, "format_counts": format_counts, "logs": logs})

@app.get("/top_books", response_class=HTMLResponse)
async def top_books_page(request: Request):
    conn = get_db_connection()
    rows = conn.execute("SELECT b.id, b.title, b.author, b.cover_url, b.cover_path, ub.effective_user_rating FROM user_books ub JOIN books b ON ub.book_id = b.id WHERE ub.effective_user_rating IS NOT NULL ORDER BY ub.effective_user_rating DESC LIMIT 20").fetchall()
    conn.close()
    books = []
    for r in rows:
        book = dict(r)
        if book.get('cover_path'): book['cover_url'] = book['cover_path']
        if book['effective_user_rating']: book['effective_user_rating'] = round(book['effective_user_rating'], 1)
        books.append(book)
    return templates.TemplateResponse("top_books.html", {"request": request, "books": books})

@app.get("/library", response_class=HTMLResponse)
async def library(request: Request, q: str = "", sort: str = "title_asc", tag: str = None, filter_format: str = "all", page: int = 1):
    conn = get_db_connection()
    ITEMS_PER_PAGE = 24
    offset = (page - 1) * ITEMS_PER_PAGE
    all_tags = conn.execute("SELECT * FROM tags ORDER BY name ASC").fetchall()
    selected_tag_id = int(tag) if tag and tag.isdigit() else None
    base_query = " FROM user_books ub JOIN books b ON ub.book_id = b.id "
    params = []
    conditions = []
    if q:
        conditions.append("(b.title LIKE ? OR b.author LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])
    if selected_tag_id:
        base_query += " JOIN book_tags bt ON b.id = bt.book_id "
        conditions.append("bt.tag_id = ?")
        params.append(selected_tag_id)
    if filter_format == "owned": conditions.append("ub.is_owned = 1")
    elif filter_format == "physical": conditions.append("ub.formats_owned LIKE '%Physical%'")
    elif filter_format == "audio": conditions.append("(ub.formats_owned LIKE '%Audible%' OR ub.formats_owned LIKE '%Audiobook%')")
    elif filter_format == "digital": conditions.append("(ub.formats_owned LIKE '%Kindle%' OR ub.formats_owned LIKE '%eBook%')")
    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    total_books = conn.execute(f"SELECT COUNT(*) {base_query} {where_clause}", params).fetchone()[0]
    total_pages = math.ceil(total_books / ITEMS_PER_PAGE)
    order_clause = " ORDER BY b.title ASC"
    if sort == "date_desc": order_clause = " ORDER BY ub.date_added DESC"
    elif sort == "author_asc": order_clause = " ORDER BY b.author ASC"
    elif sort == "rating_desc": order_clause = " ORDER BY ub.effective_user_rating DESC"
    data_sql = f"SELECT ub.id, b.title, b.author, b.cover_url, b.cover_path, ub.read_status, ub.shelf_status, ub.formats_owned, ub.is_owned, ub.effective_user_rating {base_query} {where_clause} {order_clause} LIMIT ? OFFSET ?"
    data_params = params + [ITEMS_PER_PAGE, offset]
    books_rows = conn.execute(data_sql, data_params).fetchall()
    conn.close()
    books_data = [process_book_row(r) for r in books_rows]
    return templates.TemplateResponse("library.html", {"request": request, "books": books_data, "query": q, "sort": sort, "all_tags": all_tags, "selected_tag": selected_tag_id, "current_filter": filter_format, "current_page": page, "total_pages": total_pages, "total_books": total_books})

@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    return templates.TemplateResponse("search.html", {"request": request})

@app.post("/api/search")
async def search_api(request: Request, query: str = Form(...)):
    if not query: return ""
    results = await search_aggregated(query)
    return templates.TemplateResponse("partials/search_row.html", {"request": request, "results": results})

@app.post("/api/add_book")
async def add_book(google_id: str = Form(...), title: str = Form(...), author: str = Form(...), cover: str = Form(...), pages: int = Form(0), summary: str = Form(""), genres: str = Form(""), rating: float = Form(0.0), year: str = Form(""), isbn13: str = Form(None), olid: str = Form(None), content_score: int = Form(0)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM books WHERE google_id = ?", (google_id,))
    row = cursor.fetchone()
    if row:
        book_id = row['id']
        cursor.execute("UPDATE books SET publication_year = ?, genres = ?, average_rating = ?, summary = ?, isbn13 = ?, olid = ?, content_score = ? WHERE id = ?", (year, genres, rating, summary, isbn13, olid, content_score, book_id))
    else:
        cursor.execute("INSERT INTO books (google_id, isbn13, title, author, cover_url, total_pages, summary, genres, average_rating, publication_year, olid, content_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (google_id, isbn13, title, author, cover, pages, summary, genres, rating, year, olid, content_score))
        book_id = cursor.lastrowid
    cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "Already in Library"
    cursor.execute("INSERT INTO user_books (book_id) VALUES (?)", (book_id,))
    conn.commit()
    conn.close()
    return "✅ Added"

@app.get("/add_manual", response_class=HTMLResponse)
async def add_manual_page(request: Request):
    return templates.TemplateResponse("add_manual.html", {"request": request})

@app.post("/add_manual")
async def add_manual_post(title: str = Form(...), author: str = Form(...), subtitle: str = Form(""), year: str = Form(""), pages: int = Form(0), audio_minutes: int = Form(0), isbn13: str = Form(None), asin: str = Form(None), goodreads_id: str = Form(None), olid: str = Form(None), publisher: str = Form(""), series_name: str = Form(""), series_index: float = Form(None), language: str = Form("en"), genres: str = Form(""), summary: str = Form(""), cover_url: str = Form(None), format_owned: str = Form("Physical"), status: str = Form("Shelved")):
    conn = get_db_connection()
    cursor = conn.cursor()
    final_cover = cover_url.strip() if cover_url and cover_url.strip() else None
    existing_book = None
    if isbn13:
        cursor.execute("SELECT id, cover_url FROM books WHERE isbn13 = ?", (isbn13,))
        existing_book = cursor.fetchone()
    if not existing_book:
        cursor.execute("SELECT id, cover_url FROM books WHERE title = ? AND author = ?", (title, author))
        existing_book = cursor.fetchone()
    if existing_book:
        book_id = existing_book['id']
        current_db_cover = existing_book['cover_url']
        should_update_cover = final_cover and ("placeholder" in str(current_db_cover) or not current_db_cover)
        sql = "UPDATE books SET subtitle = ?, publisher = ?, publication_year = ?, total_pages = ?, total_audio_minutes = ?, summary = ?, series_name = ?, series_index = ?, goodreads_id = ?, asin = ?, olid = ?, genres = ?"
        params = [subtitle, publisher, year, pages, audio_minutes, summary, series_name, series_index, goodreads_id, asin, olid, genres]
        if should_update_cover:
            sql += ", cover_url = ?"
            params.append(final_cover)
        sql += " WHERE id = ?"
        params.append(book_id)
        cursor.execute(sql, tuple(params))
    else:
        unique_id = str(uuid.uuid4())
        custom_google_id = f"manual_{unique_id}"
        insert_cover = final_cover if final_cover else "/static/placeholder.png"
        cursor.execute("INSERT INTO books (google_id, isbn13, asin, olid, goodreads_id, title, subtitle, author, series_name, series_index, publisher, publication_year, language, genres, total_pages, total_audio_minutes, summary, cover_url, content_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (custom_google_id, isbn13, asin, olid, goodreads_id, title, subtitle, author, series_name, series_index, publisher, year, language, genres, pages, audio_minutes, summary, insert_cover, 100))
        book_id = cursor.lastrowid
    cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
    existing_inventory = cursor.fetchone()
    if existing_inventory:
        user_book_id = existing_inventory['id']
    else:
        formats = [format_owned]
        is_owned = True
        if format_owned in ["Libby Audiobook", "Libby eBook", "Libby Physical"]: is_owned = False
        cursor.execute("INSERT INTO user_books (book_id, shelf_status, formats_owned, is_owned) VALUES (?, ?, ?, ?)", (book_id, status, json.dumps(formats), is_owned))
        user_book_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.get("/book/{id}", response_class=HTMLResponse)
async def book_detail(request: Request, id: int):
    # ... (Keep existing implementation with recent changes) ...
    conn = get_db_connection()
    row = conn.execute("SELECT ub.*, b.* FROM user_books ub JOIN books b ON ub.book_id = b.id WHERE ub.id = ?", (id,)).fetchone()
    if not row: raise HTTPException(status_code=404, detail="Book not found")
    book = process_book_row(row)
    logs = conn.execute("SELECT * FROM reading_logs WHERE user_book_id = ? ORDER BY date_finished DESC", (id,)).fetchall()
    calculated_rating = book.get('effective_user_rating')
    if calculated_rating: calculated_rating = round(calculated_rating, 1)
    tags = conn.execute("SELECT t.* FROM tags t JOIN book_tags bt ON t.id = bt.tag_id WHERE bt.book_id = ? ORDER BY t.name", (book['book_id'],)).fetchall()
    outgoing = conn.execute("SELECT r.id as relation_id, r.relation_type, b.title, ub.id as related_inventory_id, b.cover_url, b.cover_path FROM book_relations r JOIN books b ON r.target_book_id = b.id LEFT JOIN user_books ub ON b.id = ub.book_id WHERE r.source_book_id = ?", (book['book_id'],)).fetchall()
    incoming = conn.execute("SELECT r.id as relation_id, r.relation_type, b.title, ub.id as related_inventory_id, b.cover_url, b.cover_path FROM book_relations r JOIN books b ON r.source_book_id = b.id LEFT JOIN user_books ub ON b.id = ub.book_id WHERE r.target_book_id = ?", (book['book_id'],)).fetchall()
    def prep_relation(r, is_incoming=False):
        d = dict(r)
        if d.get('cover_path'): d['cover_url'] = d['cover_path']
        raw_type = d['relation_type']
        labels = RELATION_MAP.get(raw_type, (raw_type, raw_type))
        if is_incoming:
            d['label'] = labels[1]
            d['direction_icon'] = "←"
        else:
            d['label'] = labels[0]
            d['direction_icon'] = "→"
        d['is_incoming'] = is_incoming
        return d
    relations = [prep_relation(r, False) for r in outgoing] + [prep_relation(r, True) for r in incoming]
    all_tags = conn.execute("SELECT name FROM tags ORDER BY name ASC").fetchall()
    all_books_list = conn.execute("SELECT b.title FROM books b JOIN user_books ub ON b.id = ub.book_id WHERE ub.is_owned = 1 ORDER BY b.title ASC").fetchall()
    conn.close()
    return templates.TemplateResponse("book_detail.html", {"request": request, "book": book, "logs": logs, "tags": tags, "all_tags": all_tags, "relations": relations, "all_books_list": all_books_list, "formats_owned": book['formats'], "calculated_rating": calculated_rating})

@app.post("/api/tag/add")
async def add_tag_to_book(book_id: int = Form(...), user_book_id: int = Form(...), tag_name: str = Form(...)):
    if not tag_name.strip(): return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)
    conn = get_db_connection()
    clean_name = tag_name.strip()
    conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (clean_name,))
    tag = conn.execute("SELECT id FROM tags WHERE name = ?", (clean_name,)).fetchone()
    tag_id = tag['id']
    conn.execute("INSERT OR IGNORE INTO book_tags (book_id, tag_id) VALUES (?, ?)", (book_id, tag_id))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.post("/api/tag/remove")
async def remove_tag_from_book(book_id: int = Form(...), user_book_id: int = Form(...), tag_id: int = Form(...)):
    conn = get_db_connection()
    conn.execute("DELETE FROM book_tags WHERE book_id = ? AND tag_id = ?", (book_id, tag_id))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.post("/api/relation/add")
async def add_relation(source_book_id: int = Form(...), user_book_id: int = Form(...), target_book_title: str = Form(...), relation_type: str = Form(...)):
    conn = get_db_connection()
    target = conn.execute("SELECT id FROM books WHERE title = ?", (target_book_title,)).fetchone()
    if target:
        target_book_id = target['id']
        if source_book_id != target_book_id:
            conn.execute("INSERT OR IGNORE INTO book_relations (source_book_id, target_book_id, relation_type) VALUES (?, ?, ?)", (source_book_id, target_book_id, relation_type))
            conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.post("/api/relation/remove")
async def remove_relation(relation_id: int = Form(...), user_book_id: int = Form(...)):
    conn = get_db_connection()
    conn.execute("DELETE FROM book_relations WHERE id = ?", (relation_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.post("/book/{id}/delete")
async def delete_book(id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    row = cursor.execute("SELECT book_id FROM user_books WHERE id = ?", (id,)).fetchone()
    if not row:
        conn.close()
        return RedirectResponse(url="/library", status_code=303)
    book_id = row['book_id']
    cursor.execute("DELETE FROM reading_logs WHERE user_book_id = ?", (id,))
    cursor.execute("DELETE FROM user_books WHERE id = ?", (id,))
    cursor.execute("SELECT COUNT(*) FROM user_books WHERE book_id = ?", (book_id,))
    count = cursor.fetchone()[0]
    if count == 0:
        cursor.execute("DELETE FROM books WHERE id = ?", (book_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/library", status_code=303)

@app.post("/book/{id}/update_inventory")
async def update_inventory(id: int, shelf_status: str = Form(...), inventory_notes: str = Form(""), physical: str = Form(None), kindle: str = Form(None), audible: str = Form(None), libby_audio: str = Form(None), libby_physical: str = Form(None), libby_ebook: str = Form(None)):
    formats = []
    if physical: formats.append("Physical")
    if kindle: formats.append("Kindle")
    if audible: formats.append("Audible")
    if libby_audio: formats.append("Libby Audiobook")
    if libby_physical: formats.append("Libby Physical")
    if libby_ebook: formats.append("Libby eBook")
    is_owned = any(f in ["Physical", "Kindle", "Audible"] for f in formats)
    conn = get_db_connection()
    conn.execute("UPDATE user_books SET shelf_status = ?, inventory_notes = ?, formats_owned = ?, is_owned = ? WHERE id = ?", (shelf_status, inventory_notes, json.dumps(formats), is_owned, id))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{id}", status_code=303)

@app.post("/book/{id}/add_log")
async def add_log(id: int, date_finished: str = Form(...), hours: float = Form(...), format_consumed: str = Form(...), pace: str = Form("Medium"), notes: str = Form(""), is_dnf: bool = Form(False), is_borrowed: bool = Form(False), session_rating: float = Form(None)):
    conn = get_db_connection()
    conn.execute("INSERT INTO reading_logs (user_book_id, date_finished, hours_read, format_consumed, pace, log_notes, is_dnf, is_borrowed, session_rating) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, date_finished, hours, format_consumed, pace, notes, is_dnf, is_borrowed, session_rating))
    if not is_dnf: conn.execute("UPDATE user_books SET read_status = 'Read' WHERE id = ?", (id,))
    else: conn.execute("UPDATE user_books SET read_status = 'DNF' WHERE id = ? AND read_status != 'Read'", (id,))
    recalculate_book_rating(conn, id)
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{id}", status_code=303)

@app.get("/author/{name}", response_class=HTMLResponse)
async def author_page(request: Request, name: str):
    conn = get_db_connection()
    books_rows = conn.execute("SELECT ub.id, b.title, b.author, b.cover_url, b.cover_path, ub.read_status, ub.shelf_status, ub.formats_owned, ub.is_owned FROM user_books ub JOIN books b ON ub.book_id = b.id WHERE b.author LIKE ? ORDER BY b.publication_year DESC", (f"%{name}%",)).fetchall()
    conn.close()
    books_data = [process_book_row(r) for r in books_rows]
    return templates.TemplateResponse("author.html", {"request": request, "books": books_data, "author_name": name})

@app.get("/api/cover_proxy")
async def cover_proxy(url: str):
    if not url: return Response(status_code=404)
    if url.startswith("/static"): return RedirectResponse(url)
    try:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "image/*", "Referer": "https://www.google.com/"}
        async with httpx.AsyncClient(follow_redirects=True, verify=False) as client:
            resp = await client.get(url, headers=headers, timeout=10.0)
            if resp.status_code != 200: return RedirectResponse("/static/placeholder.png")
            return Response(content=resp.content, media_type=resp.headers.get("content-type", "image/jpeg"))
    except: return RedirectResponse("/static/placeholder.png")

@app.get("/book/{id}/cover_options", response_class=HTMLResponse)
async def get_cover_options(request: Request, id: int):
    conn = get_db_connection()
    book = conn.execute("SELECT title, author FROM books WHERE id = ?", (id,)).fetchone()
    conn.close()
    if not book: return "Book not found"
    results = await search_aggregated(f"{book['title']} {book['author']}")
    unique_covers = []
    seen = set()
    for r in results:
        url = r.get('cover')
        if url and "placeholder" not in url and url not in seen:
            unique_covers.append(url)
            seen.add(url)
    return templates.TemplateResponse("partials/cover_options.html", {"request": request, "book_id": id, "covers": unique_covers})

@app.post("/book/{id}/set_cover")
async def set_cover(id: int, new_cover_url: str = Form(...)):
    conn = get_db_connection()
    conn.execute("UPDATE books SET cover_url = ?, cover_path = NULL WHERE id = ?", (new_cover_url, id))
    conn.commit()
    row = conn.execute("SELECT id FROM user_books WHERE book_id = ?", (id,)).fetchone()
    conn.close()
    return RedirectResponse(url=f"/book/{row['id']}", status_code=303) if row else RedirectResponse(url="/library", status_code=303)

@app.get("/log/{log_id}/edit", response_class=HTMLResponse)
async def edit_log_page(request: Request, log_id: int):
    conn = get_db_connection()
    log = conn.execute("SELECT * FROM reading_logs WHERE id = ?", (log_id,)).fetchone()
    conn.close()
    if not log: raise HTTPException(status_code=404, detail="Log not found")
    return templates.TemplateResponse("edit_log.html", {"request": request, "log": log})

@app.post("/log/{log_id}/edit")
async def update_log(log_id: int, date_finished: str = Form(...), hours: float = Form(...), format_consumed: str = Form(...), pace: str = Form("Medium"), notes: str = Form(""), is_dnf: bool = Form(False), is_borrowed: bool = Form(False), session_rating: float = Form(None)):
    conn = get_db_connection()
    conn.execute("UPDATE reading_logs SET date_finished = ?, hours_read = ?, format_consumed = ?, pace = ?, log_notes = ?, is_dnf = ?, is_borrowed = ?, session_rating = ? WHERE id = ?", (date_finished, hours, format_consumed, pace, notes, is_dnf, is_borrowed, session_rating, log_id))
    row = conn.execute("SELECT user_book_id FROM reading_logs WHERE id = ?", (log_id,)).fetchone()
    if row: recalculate_book_rating(conn, row['user_book_id'])
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{row['user_book_id']}", status_code=303)

@app.post("/log/{log_id}/delete")
async def delete_log(log_id: int):
    conn = get_db_connection()
    row = conn.execute("SELECT user_book_id FROM reading_logs WHERE id = ?", (log_id,)).fetchone()
    if row:
        book_id = row['user_book_id']
        conn.execute("DELETE FROM reading_logs WHERE id = ?", (log_id,))
        recalculate_book_rating(conn, book_id)
        conn.commit()
        conn.close()
        return RedirectResponse(url=f"/book/{book_id}", status_code=303)
    conn.close()
    return RedirectResponse(url="/", status_code=303)

@app.post("/system/backup", response_class=HTMLResponse)
async def trigger_backup(request: Request):
    result = backup_database()
    is_success = result.startswith("Success:")
    message = "Database successfully backed up." if is_success else result
    filename = os.path.basename(result.replace("Success: ", "")) if is_success else ""
    return templates.TemplateResponse("backup_result.html", {"request": request, "is_success": is_success, "message": message, "filename": filename})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)