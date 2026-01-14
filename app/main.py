from fastapi import FastAPI, Request, Form, HTTPException
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

# --- LOCAL IMPORTS ---
from .database import init_db, get_db_connection, backup_database
from .metadata import search_aggregated
# NEW: Importing logic from utils to keep main.py clean
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

# --- REGISTER FILTERS ---
# These functions are now imported from utils.py
templates.env.filters["format_minutes"] = format_minutes
templates.env.filters["format_runtime"] = format_runtime
templates.env.filters["urlencode"] = urllib.parse.quote_plus

# --- CUSTOM ERROR HANDLERS ---
@app.exception_handler(404)
async def custom_404_handler(request: Request, exc: StarletteHTTPException):
    return templates.TemplateResponse("404.html", {"request": request}, status_code=404)

# --- ROUTES ---
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    conn = get_db_connection()
    
    # 1. Lifetime Stats (All Time)
    stats_query = """
        SELECT 
            COUNT(DISTINCT l.id) as books_read, 
            SUM(l.hours_read) as total_hours
        FROM reading_logs l
        WHERE l.is_dnf = 0
    """
    stats = conn.execute(stats_query).fetchone()
    
    # 2. Total Library Count
    library_count = conn.execute("SELECT COUNT(*) FROM user_books").fetchone()[0]
    
    # 3. On Deck (List + Count)
    on_deck_rows = conn.execute("""
        SELECT ub.id, b.title, b.cover_url, b.cover_path
        FROM user_books ub 
        JOIN books b ON ub.book_id = b.id 
        WHERE ub.shelf_status = 'On Deck'
        LIMIT 5
    """).fetchall()
    on_deck = [process_book_row(r) for r in on_deck_rows]
    
    # Count total items in "On Deck"
    on_deck_count = conn.execute("SELECT COUNT(*) FROM user_books WHERE shelf_status = 'On Deck'").fetchone()[0]
    
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

@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request, year: int = None):
    conn = get_db_connection()
    current_year = date.today().year
    selected_year = year if year else current_year
    
    # 1. Get available years
    years_rows = conn.execute("""
        SELECT DISTINCT strftime('%Y', date_finished) as y 
        FROM reading_logs WHERE date_finished IS NOT NULL ORDER BY y DESC
    """).fetchall()
    available_years = [int(r['y']) for r in years_rows if r['y']]
    if current_year not in available_years: available_years.insert(0, current_year)
    
    # 2. Monthly Data (Bar Chart)
    monthly_query = """
        SELECT 
            strftime('%m', l.date_finished) as month,
            COUNT(DISTINCT l.id) as books,
            SUM(l.hours_read) as hours,
            SUM(b.total_pages) as pages
        FROM reading_logs l
        JOIN user_books ub ON l.user_book_id = ub.id
        JOIN books b ON ub.book_id = b.id
        WHERE strftime('%Y', l.date_finished) = ? AND l.is_dnf = 0
        GROUP BY month
        ORDER BY month
    """
    rows = conn.execute(monthly_query, (str(selected_year),)).fetchall()
    
    # Initialize zero data
    labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    data_books = [0] * 12
    data_hours = [0] * 12
    data_pages = [0] * 12
    
    for r in rows:
        idx = int(r['month']) - 1
        data_books[idx] = r['books']
        data_hours[idx] = round(r['hours'], 1)
        data_pages[idx] = r['pages']

    # 3. Format Breakdown (Pie Chart)
    format_data = conn.execute("""
        SELECT format_consumed, COUNT(*) as count
        FROM reading_logs
        WHERE strftime('%Y', date_finished) = ?
        GROUP BY format_consumed
    """, (str(selected_year),)).fetchall()
    
    format_labels = [row['format_consumed'] for row in format_data]
    format_counts = [row['count'] for row in format_data]

    # 4. Chronological Log List
    logs_rows = conn.execute("""
        SELECT 
            b.id as book_id,
            b.title, 
            b.author, 
            b.cover_url, 
            b.cover_path,
            ub.effective_user_rating as user_rating,
            rl.date_finished, 
            rl.format_consumed, 
            rl.is_borrowed,
            rl.hours_read
        FROM reading_logs rl
        JOIN user_books ub ON rl.user_book_id = ub.id
        JOIN books b ON ub.book_id = b.id
        WHERE strftime('%Y', rl.date_finished) = ?
        ORDER BY rl.date_finished ASC
    """, (str(selected_year),)).fetchall()
    
    # Process logs to swap cover_url if local path exists
    logs = []
    for row in logs_rows:
        r = dict(row)
        if r.get('cover_path'):
            r['cover_url'] = r['cover_path']
        logs.append(r)

    conn.close()
    
    return templates.TemplateResponse("stats.html", {
        "request": request,
        "selected_year": selected_year,
        "available_years": available_years,
        # Bar Chart
        "labels": labels,          
        "data_books": data_books, 
        "data_hours": data_hours, 
        "data_pages": data_pages,
        # Pie Chart
        "format_labels": format_labels,
        "format_counts": format_counts,
        # List
        "logs": logs
    })

@app.get("/top_books", response_class=HTMLResponse)
async def top_books_page(request: Request):
    conn = get_db_connection()
    
    # EFFICIENT QUERY: Relies on the denormalized 'effective_user_rating' column
    query = """
        SELECT 
            b.id, b.title, b.author, b.cover_url, b.cover_path,
            ub.effective_user_rating
        FROM user_books ub
        JOIN books b ON ub.book_id = b.id
        WHERE ub.effective_user_rating IS NOT NULL
        ORDER BY ub.effective_user_rating DESC
        LIMIT 20
    """
    
    rows = conn.execute(query).fetchall()
    conn.close()
    
    books = []
    for r in rows:
        book = dict(r)
        if book.get('cover_path'): book['cover_url'] = book['cover_path']
        # Round it for display
        if book['effective_user_rating']:
            book['effective_user_rating'] = round(book['effective_user_rating'], 1)
        books.append(book)

    return templates.TemplateResponse("top_books.html", {"request": request, "books": books})

@app.get("/library", response_class=HTMLResponse)
async def library(request: Request, q: str = "", sort: str = "title_asc", tag: str = None, page: int = 1):
    conn = get_db_connection()
    
    ITEMS_PER_PAGE = 24
    offset = (page - 1) * ITEMS_PER_PAGE

    # 1. Fetch all tags for the dropdown
    all_tags = conn.execute("SELECT * FROM tags ORDER BY name ASC").fetchall()
    
    # 2. Process Tag Filter
    selected_tag_id = None
    if tag and tag.isdigit():
        selected_tag_id = int(tag)

    # --- BUILD QUERY PARTS ---
    # We build the "core" of the query (FROM + WHERE) separately so we can reuse it
    # for both counting the total results and fetching the specific page.
    
    base_query = """
        FROM user_books ub
        JOIN books b ON ub.book_id = b.id
    """
    
    params = []
    conditions = []
    
    # Filter by Text
    if q:
        conditions.append("(b.title LIKE ? OR b.author LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])
        
    # Filter by Tag
    if selected_tag_id:
        base_query += " JOIN book_tags bt ON b.id = bt.book_id "
        conditions.append("bt.tag_id = ?")
        params.append(selected_tag_id)

    # Apply Conditions
    where_clause = ""
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)

    # --- QUERY 1: GET TOTAL COUNT ---
    count_sql = f"SELECT COUNT(*) {base_query} {where_clause}"
    total_books = conn.execute(count_sql, params).fetchone()[0]
    total_pages = math.ceil(total_books / ITEMS_PER_PAGE)

    # --- QUERY 2: GET PAGINATED DATA ---
    # Sorting Logic
    order_clause = " ORDER BY b.title ASC" # Default
    if sort == "date_desc":
        order_clause = " ORDER BY ub.date_added DESC"
    elif sort == "author_asc":
        order_clause = " ORDER BY b.author ASC"
    elif sort == "rating_desc":
        order_clause = " ORDER BY ub.effective_user_rating DESC"

    data_sql = f"""
        SELECT ub.id, b.title, b.author, b.cover_url, b.cover_path, 
               ub.read_status, ub.shelf_status, ub.formats_owned, ub.is_owned, 
               ub.effective_user_rating
        {base_query}
        {where_clause}
        {order_clause}
        LIMIT ? OFFSET ?
    """
    
    # Add limit/offset params to the existing params list
    data_params = params + [ITEMS_PER_PAGE, offset]
    
    books_rows = conn.execute(data_sql, data_params).fetchall()
    conn.close()
    
    books_data = [process_book_row(r) for r in books_rows]

    return templates.TemplateResponse("library.html", {
        "request": request, 
        "books": books_data, 
        "query": q, 
        "sort": sort,
        "all_tags": all_tags,
        "selected_tag": selected_tag_id,
        # Pagination Data
        "current_page": page,
        "total_pages": total_pages,
        "total_books": total_books
    })

@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    return templates.TemplateResponse("search.html", {"request": request})

@app.post("/api/search")
async def search_api(request: Request, query: str = Form(...)):
    if not query: return ""
    
    # Calls metadata.py to search Google + Open Library
    results = await search_aggregated(query)
            
    return templates.TemplateResponse("partials/search_row.html", {"request": request, "results": results})

@app.post("/api/add_book")
async def add_book(
    google_id: str = Form(...), 
    title: str = Form(...), 
    author: str = Form(...), 
    cover: str = Form(...),
    pages: int = Form(0),
    summary: str = Form(""),
    genres: str = Form(""),        
    rating: float = Form(0.0),      
    year: str = Form(""),
    isbn13: str = Form(None),
    olid: str = Form(None),
    content_score: int = Form(0)
):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check for existing book by ID
    cursor.execute("SELECT id FROM books WHERE google_id = ?", (google_id,))
    row = cursor.fetchone()
    
    if row:
        book_id = row['id']
        # Update existing record
        cursor.execute("""
            UPDATE books 
            SET publication_year = ?, genres = ?, average_rating = ?, summary = ?, isbn13 = ?, olid = ?, content_score = ?
            WHERE id = ?
        """, (year, genres, rating, summary, isbn13, olid, content_score, book_id))
    else:
        # Insert new record
        cursor.execute("""
            INSERT INTO books (google_id, isbn13, title, author, cover_url, total_pages, summary, genres, average_rating, publication_year, olid, content_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (google_id, isbn13, title, author, cover, pages, summary, genres, rating, year, olid, content_score))
        book_id = cursor.lastrowid
    
    cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "Already in Library"
    
    cursor.execute("INSERT INTO user_books (book_id) VALUES (?)", (book_id,))
    conn.commit()
    conn.close()
    
    return "✅ Added"

# --- UPDATED MANUAL ADD ROUTE ---
@app.get("/add_manual", response_class=HTMLResponse)
async def add_manual_page(request: Request):
    return templates.TemplateResponse("add_manual.html", {"request": request})

@app.post("/add_manual")
async def add_manual_post(
    # --- REQUIRED ---
    title: str = Form(...),
    author: str = Form(...),
    
    # --- OPTIONAL / ADVANCED ---
    subtitle: str = Form(""),
    year: str = Form(""),
    pages: int = Form(0),
    audio_minutes: int = Form(0),
    isbn13: str = Form(None),
    asin: str = Form(None),          
    goodreads_id: str = Form(None),
    olid: str = Form(None),          
    publisher: str = Form(""),
    series_name: str = Form(""),
    series_index: float = Form(None),
    language: str = Form("en"),
    genres: str = Form(""),          
    summary: str = Form(""),
    cover_url: str = Form(None),
    
    # --- INVENTORY ---
    format_owned: str = Form("Physical"),
    status: str = Form("Shelved")
):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clean inputs
    final_cover = cover_url.strip() if cover_url and cover_url.strip() else None

    # 1. Search for Existing Book
    existing_book = None
    if isbn13:
        cursor.execute("SELECT id, cover_url FROM books WHERE isbn13 = ?", (isbn13,))
        existing_book = cursor.fetchone()
    
    # If not found by ISBN, try Title + Author
    if not existing_book:
        cursor.execute("SELECT id, cover_url FROM books WHERE title = ? AND author = ?", (title, author))
        existing_book = cursor.fetchone()

    if existing_book:
        # --- UPDATE EXISTING RECORD ---
        book_id = existing_book['id']
        
        current_db_cover = existing_book['cover_url']
        should_update_cover = final_cover and ("placeholder" in str(current_db_cover) or not current_db_cover)

        # Update SQL includes genres, asin, olid
        sql = """
            UPDATE books 
            SET subtitle = ?, publisher = ?, publication_year = ?, 
                total_pages = ?, total_audio_minutes = ?, summary = ?, 
                series_name = ?, series_index = ?, goodreads_id = ?, 
                asin = ?, olid = ?, genres = ?
        """
        params = [
            subtitle, publisher, year, pages, audio_minutes, summary, 
            series_name, series_index, goodreads_id, asin, olid, genres
        ]
        
        if should_update_cover:
            sql += ", cover_url = ?"
            params.append(final_cover)
            
        sql += " WHERE id = ?"
        params.append(book_id)
        
        cursor.execute(sql, tuple(params))

    else:
        # --- INSERT NEW RECORD ---
        unique_id = str(uuid.uuid4())
        custom_google_id = f"manual_{unique_id}"
        
        insert_cover = final_cover if final_cover else "/static/placeholder.png"

        cursor.execute("""
            INSERT INTO books (
                google_id, isbn13, asin, olid, goodreads_id, title, subtitle, author, 
                series_name, series_index, publisher, publication_year, language, genres,
                total_pages, total_audio_minutes, summary, cover_url, content_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            custom_google_id, isbn13, asin, olid, goodreads_id, title, subtitle, author, 
            series_name, series_index, publisher, year, language, genres,
            pages, audio_minutes, summary, insert_cover, 100
        ))
        book_id = cursor.lastrowid

    # 3. Add to Inventory (User Books)
    cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
    existing_inventory = cursor.fetchone()
    
    if existing_inventory:
        user_book_id = existing_inventory['id']
    else:
        formats = [format_owned]
        is_owned = True
        if format_owned in ["Libby Audiobook", "Libby eBook", "Libby Physical"]:
            is_owned = False
            
        cursor.execute("""
            INSERT INTO user_books (book_id, shelf_status, formats_owned, is_owned)
            VALUES (?, ?, ?, ?)
        """, (book_id, status, json.dumps(formats), is_owned))
        user_book_id = cursor.lastrowid

    conn.commit()
    conn.close()
    
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.get("/book/{id}", response_class=HTMLResponse)
async def book_detail(request: Request, id: int):
    conn = get_db_connection()
    row = conn.execute("""
        SELECT ub.*, b.* FROM user_books ub 
        JOIN books b ON ub.book_id = b.id 
        WHERE ub.id = ?
    """, (id,)).fetchone()
    
    if not row: raise HTTPException(status_code=404, detail="Book not found")
    
    book = process_book_row(row)
        
    logs = conn.execute("SELECT * FROM reading_logs WHERE user_book_id = ? ORDER BY date_finished DESC", (id,)).fetchall()
    
    # Calculate Rating for display
    # Since we are using the 'sync' approach, we can use the value from the book row
    calculated_rating = book.get('effective_user_rating')
    if calculated_rating:
        calculated_rating = round(calculated_rating, 1)

    # --- FETCH TAGS FOR THIS BOOK ---
    # Using book['book_id'] to ensure we match the Definition ID, not Inventory ID
    tags = conn.execute("""
        SELECT t.* FROM tags t
        JOIN book_tags bt ON t.id = bt.tag_id
        WHERE bt.book_id = ?
        ORDER BY t.name
    """, (book['book_id'],)).fetchall()

    # --- FETCH RELATIONS (UPDATED LOGIC) ---
    # 1. Outgoing (I point to them)
    outgoing = conn.execute("""
        SELECT r.id as relation_id, r.relation_type, b.title, ub.id as related_inventory_id, b.cover_url, b.cover_path
        FROM book_relations r
        JOIN books b ON r.target_book_id = b.id
        LEFT JOIN user_books ub ON b.id = ub.book_id
        WHERE r.source_book_id = ?
    """, (book['book_id'],)).fetchall()

    # 2. Incoming (They point to me)
    incoming = conn.execute("""
        SELECT r.id as relation_id, r.relation_type, b.title, ub.id as related_inventory_id, b.cover_url, b.cover_path
        FROM book_relations r
        JOIN books b ON r.source_book_id = b.id
        LEFT JOIN user_books ub ON b.id = ub.book_id
        WHERE r.target_book_id = ?
    """, (book['book_id'],)).fetchall()

    # Helper to apply the smart labels (Uses imported RELATION_MAP)
    def prep_relation(r, is_incoming=False):
        d = dict(r)
        
        # 1. Handle Covers
        if d.get('cover_path'): d['cover_url'] = d['cover_path']
        
        # 2. Handle Smart Labels
        raw_type = d['relation_type']
        
        # Get the config tuple, default to raw text if not found
        labels = RELATION_MAP.get(raw_type, (raw_type, raw_type))
        
        if is_incoming:
            # If I am the target, use the REVERSE label (Index 1)
            d['label'] = labels[1]
            d['direction_icon'] = "←" # Visual cue (optional)
        else:
            # If I am the source, use the FORWARD label (Index 0)
            d['label'] = labels[0]
            d['direction_icon'] = "→" # Visual cue (optional)

        d['is_incoming'] = is_incoming
        return d

    relations = [prep_relation(r, False) for r in outgoing] + [prep_relation(r, True) for r in incoming]

    # --- FETCH ALL TAGS & BOOKS (For Autocomplete) ---
    all_tags = conn.execute("SELECT name FROM tags ORDER BY name ASC").fetchall()
    
    # Fetch all books for the "Add Relation" dropdown
    all_books_list = conn.execute("SELECT title FROM books ORDER BY title ASC").fetchall()

    conn.close()
    
    return templates.TemplateResponse("book_detail.html", {
        "request": request, 
        "book": book, 
        "logs": logs,
        "tags": tags,           
        "all_tags": all_tags,   
        "relations": relations, 
        "all_books_list": all_books_list, 
        "formats_owned": book['formats'],
        "calculated_rating": calculated_rating 
    })

# --- TAG & RELATION API ENDPOINTS ---

@app.post("/api/tag/add")
async def add_tag_to_book(book_id: int = Form(...), user_book_id: int = Form(...), tag_name: str = Form(...)):
    if not tag_name.strip():
        return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)
        
    conn = get_db_connection()
    clean_name = tag_name.strip()
    
    # 1. Ensure Tag Exists
    conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (clean_name,))
    tag = conn.execute("SELECT id FROM tags WHERE name = ?", (clean_name,)).fetchone()
    tag_id = tag['id']
    
    # 2. Link Book to Tag
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
async def add_relation(
    source_book_id: int = Form(...), # The Definition ID of current book
    user_book_id: int = Form(...),   # The Inventory ID (for redirect)
    target_book_title: str = Form(...),
    relation_type: str = Form(...)
):
    conn = get_db_connection()
    
    # 1. Find the target book ID by title
    target = conn.execute("SELECT id FROM books WHERE title = ?", (target_book_title,)).fetchone()
    
    if target:
        target_book_id = target['id']
        
        # Prevent self-linking
        if source_book_id != target_book_id:
            # 2. Insert Relation
            conn.execute("""
                INSERT OR IGNORE INTO book_relations (source_book_id, target_book_id, relation_type)
                VALUES (?, ?, ?)
            """, (source_book_id, target_book_id, relation_type))
            conn.commit()
    
    conn.close()
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.post("/api/relation/remove")
async def remove_relation(
    relation_id: int = Form(...),
    user_book_id: int = Form(...)
):
    conn = get_db_connection()
    conn.execute("DELETE FROM book_relations WHERE id = ?", (relation_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{user_book_id}", status_code=303)

@app.post("/book/{id}/delete")
async def delete_book(id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Find the parent book_id before we delete the inventory entry
    row = cursor.execute("SELECT book_id FROM user_books WHERE id = ?", (id,)).fetchone()
    if not row:
        conn.close()
        return RedirectResponse(url="/library", status_code=303)
        
    book_id = row['book_id']
    
    # 2. Delete Logs & Inventory Entry
    cursor.execute("DELETE FROM reading_logs WHERE user_book_id = ?", (id,))
    cursor.execute("DELETE FROM user_books WHERE id = ?", (id,))
    
    # 3. ORPHAN CHECK: Does anyone else own this book?
    # If count is 0, we should delete the 'books' record too so it doesn't haunt us.
    cursor.execute("SELECT COUNT(*) FROM user_books WHERE book_id = ?", (book_id,))
    count = cursor.fetchone()[0]
    
    if count == 0:
        cursor.execute("DELETE FROM books WHERE id = ?", (book_id,))
        print(f"🗑️ Deleted orphaned book definition (ID: {book_id})")
        
    conn.commit()
    conn.close()
    
    return RedirectResponse(url="/library", status_code=303)

@app.post("/book/{id}/update_inventory")
async def update_inventory(
    id: int, 
    shelf_status: str = Form(...), 
    inventory_notes: str = Form(""),
    # Standard
    physical: str = Form(None),
    kindle: str = Form(None),
    audible: str = Form(None),
    # Libby
    libby_audio: str = Form(None),
    libby_physical: str = Form(None),
    libby_ebook: str = Form(None)
):
    formats = []
    owned_formats = 0
    
    if physical: 
        formats.append("Physical")
        owned_formats += 1
    if kindle: 
        formats.append("Kindle")
        owned_formats += 1
    if audible: 
        formats.append("Audible")
        owned_formats += 1
        
    if libby_audio: formats.append("Libby Audiobook")
    if libby_physical: formats.append("Libby Physical")
    if libby_ebook: formats.append("Libby eBook")
    
    is_owned = True if owned_formats > 0 else False
    
    conn = get_db_connection()
    
    conn.execute("""
        UPDATE user_books 
        SET shelf_status = ?, inventory_notes = ?, formats_owned = ?, is_owned = ?
        WHERE id = ?
    """, (shelf_status, inventory_notes, json.dumps(formats), is_owned, id))
    
    conn.commit()
    conn.close()
    
    return RedirectResponse(url=f"/book/{id}", status_code=303)

@app.post("/book/{id}/add_log")
async def add_log(
    id: int, 
    date_finished: str = Form(...), 
    hours: float = Form(...), 
    format_consumed: str = Form(...), 
    pace: str = Form("Medium"), 
    notes: str = Form(""), 
    is_dnf: bool = Form(False), 
    is_borrowed: bool = Form(False),
    session_rating: float = Form(None)
):
    conn = get_db_connection()
    conn.execute("""
        INSERT INTO reading_logs (user_book_id, date_finished, hours_read, format_consumed, pace, log_notes, is_dnf, is_borrowed, session_rating)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (id, date_finished, hours, format_consumed, pace, notes, is_dnf, is_borrowed, session_rating))
    
    if not is_dnf:
        conn.execute("UPDATE user_books SET read_status = 'Read' WHERE id = ?", (id,))
    else:
        conn.execute("UPDATE user_books SET read_status = 'DNF' WHERE id = ? AND read_status != 'Read'", (id,))
    
    # Recalculate Rating Average
    recalculate_book_rating(conn, id)
    
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{id}", status_code=303)

@app.get("/author/{name}", response_class=HTMLResponse)
async def author_page(request: Request, name: str):
    conn = get_db_connection()
    query = """
        SELECT ub.id, b.title, b.author, b.cover_url, b.cover_path, ub.read_status, ub.shelf_status, ub.formats_owned, ub.is_owned
        FROM user_books ub
        JOIN books b ON ub.book_id = b.id
        WHERE b.author LIKE ?
        ORDER BY b.publication_year DESC
    """
    
    books_rows = conn.execute(query, (f"%{name}%",)).fetchall()
    conn.close()
    
    # Apply Helper Logic
    books_data = [process_book_row(r) for r in books_rows]

    return templates.TemplateResponse("author.html", {
        "request": request, 
        "books": books_data, 
        "author_name": name
    })

@app.get("/api/cover_proxy")
async def cover_proxy(url: str):
    # 1. Validation
    if not url: return Response(status_code=404)
    if url.startswith("/static"):
        return RedirectResponse(url)

    try:
        # 2. The Spoof: Mimic a real browser so we don't get blocked
        # (These are the new lines that fix the issue)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Referer": "https://www.google.com/",  
            "Accept-Language": "en-US,en;q=0.9"
        }

        # 3. The Fetch
        async with httpx.AsyncClient(follow_redirects=True, verify=False) as client:
            resp = await client.get(url, headers=headers, timeout=10.0)
            
            if resp.status_code != 200:
                # If proxy fails, fallback to placeholder
                return RedirectResponse("/static/placeholder.png")
                
            # 4. The Handoff
            return Response(content=resp.content, media_type=resp.headers.get("content-type", "image/jpeg"))
    except Exception as e:
        print(f"Proxy Error: {e}")
        return RedirectResponse("/static/placeholder.png")

# --- COVER SWAPPER ROUTE ---
@app.get("/book/{id}/cover_options", response_class=HTMLResponse)
async def get_cover_options(request: Request, id: int):
    conn = get_db_connection()
    book = conn.execute("SELECT title, author FROM books WHERE id = ?", (id,)).fetchone()
    conn.close()
    
    if not book: return "Book not found"

    # --- STRATEGY: Title + Author Only ---
    # We ignore ISBN here intentionally to find "Visual Variations" 
    # (e.g. Kindle vs. Hardcover vs. 10th Anniversary Edition)
    query = f"{book['title']} {book['author']}"
    
    # Run the search
    results = await search_aggregated(query)
    
    # Filter down to just unique, valid images
    unique_covers = []
    seen_urls = set()
    
    for r in results:
        url = r.get('cover')
        # We also check if the URL is actually a valid image string
        if url and "placeholder" not in url and url not in seen_urls:
            unique_covers.append(url)
            seen_urls.add(url)
            
    return templates.TemplateResponse("partials/cover_options.html", {
        "request": request, 
        "book_id": id, 
        "covers": unique_covers
    })

@app.post("/book/{id}/set_cover")
async def set_cover(id: int, new_cover_url: str = Form(...)):
    conn = get_db_connection()
    
    # 1. Update the Book Definition (This 'id' is the book_id/Definition ID)
    conn.execute("UPDATE books SET cover_url = ?, cover_path = NULL WHERE id = ?", (new_cover_url, id))
    conn.commit()
    
    # 2. Find the correct Inventory ID (user_book_id) to redirect back to
    # We look for the user_book entry that points to this book definition
    row = conn.execute("SELECT id FROM user_books WHERE book_id = ?", (id,)).fetchone()
    conn.close()
    
    # 3. Redirect to the User Book (Inventory) page, not the Definition ID
    if row:
        return RedirectResponse(url=f"/book/{row['id']}", status_code=303)
    else:
        # Fallback if something weird happens (e.g. orphan book)
        return RedirectResponse(url="/library", status_code=303)

# --- LOG MANAGEMENT ---
@app.get("/log/{log_id}/edit", response_class=HTMLResponse)
async def edit_log_page(request: Request, log_id: int):
    conn = get_db_connection()
    log = conn.execute("SELECT * FROM reading_logs WHERE id = ?", (log_id,)).fetchone()
    conn.close()
    if not log: raise HTTPException(status_code=404, detail="Log not found")
    return templates.TemplateResponse("edit_log.html", {"request": request, "log": log})

@app.post("/log/{log_id}/edit")
async def update_log(
    log_id: int, 
    date_finished: str = Form(...), 
    hours: float = Form(...), 
    format_consumed: str = Form(...), 
    pace: str = Form("Medium"), 
    notes: str = Form(""), 
    is_dnf: bool = Form(False), 
    is_borrowed: bool = Form(False),
    session_rating: float = Form(None)
):
    conn = get_db_connection()
    conn.execute("""
        UPDATE reading_logs 
        SET date_finished = ?, hours_read = ?, format_consumed = ?, pace = ?, log_notes = ?, is_dnf = ?, is_borrowed = ?, session_rating = ?
        WHERE id = ?
    """, (date_finished, hours, format_consumed, pace, notes, is_dnf, is_borrowed, session_rating, log_id))
    
    row = conn.execute("SELECT user_book_id FROM reading_logs WHERE id = ?", (log_id,)).fetchone()
    
    # Recalculate Rating
    if row:
        recalculate_book_rating(conn, row['user_book_id'])

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
        
        # Recalculate Rating after delete
        recalculate_book_rating(conn, book_id)
        
        conn.commit()
        conn.close()
        return RedirectResponse(url=f"/book/{book_id}", status_code=303)
    conn.close()
    return RedirectResponse(url="/", status_code=303)

# --- BACKUP ROUTE ---
@app.post("/system/backup", response_class=HTMLResponse)
async def trigger_backup(request: Request):
    # 1. Run the backup
    result = backup_database() 
    
    # 2. Parse the result to look nicer
    message = result
    filename = ""
    is_success = result.startswith("Success:")
    
    if is_success:
        full_path = result.replace("Success: ", "")
        filename = os.path.basename(full_path)
        message = "Database successfully backed up."
    
    # 3. Show the success page
    return templates.TemplateResponse("backup_result.html", {
        "request": request, 
        "is_success": is_success,
        "message": message,
        "filename": filename
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)