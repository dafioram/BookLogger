from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from starlette.exceptions import HTTPException as StarletteHTTPException
import httpx
import json
import math
import os
from datetime import date
from .database import init_db, get_db_connection, backup_database

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)
os.makedirs("app/static", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# --- CUSTOM ERROR HANDLERS ---
@app.exception_handler(404)
async def custom_404_handler(request: Request, exc: StarletteHTTPException):
    return templates.TemplateResponse("404.html", {"request": request}, status_code=404)

# --- UTILITIES ---
def format_minutes(mins):
    if not mins: return ""
    h = math.floor(mins / 60)
    m = mins % 60
    return f"{h}h {m}m"

templates.env.filters["format_minutes"] = format_minutes

# --- ROUTES ---
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    conn = get_db_connection()
    
    # --- DASHBOARD: ALL TIME STATS ---
    
    # 1. Lifetime Stats (Books Finished & Hours)
    # Note: Removed the "WHERE strftime('%Y'...)" clause
    stats_query = """
        SELECT 
            COUNT(DISTINCT l.id) as books_read, 
            SUM(l.hours_read) as total_hours
        FROM reading_logs l
        WHERE l.is_dnf = 0
    """
    stats = conn.execute(stats_query).fetchone()
    
    # 2. Total Library Count (Inventory Size)
    library_count = conn.execute("SELECT COUNT(*) FROM user_books").fetchone()[0]
    
    # 3. On Deck
    on_deck = conn.execute("""
        SELECT ub.id, b.title, b.cover_url 
        FROM user_books ub 
        JOIN books b ON ub.book_id = b.id 
        WHERE ub.shelf_status = 'On Deck'
        LIMIT 5
    """).fetchall()
    
    # 4. Recent Logs
    recent = conn.execute("""
        SELECT b.title, l.date_finished, l.hours_read, l.is_dnf
        FROM reading_logs l
        JOIN user_books ub ON l.user_book_id = ub.id
        JOIN books b ON ub.book_id = b.id
        ORDER BY l.date_finished DESC
        LIMIT 5
    """).fetchall()
    
    conn.close()
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "stats": stats, 
        "total_books": library_count, 
        "on_deck": on_deck, 
        "recent": recent
    })

@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request, year: int = None):
    conn = get_db_connection()
    current_year = date.today().year
    selected_year = year if year else current_year
    
    # Get available years
    years_rows = conn.execute("""
        SELECT DISTINCT strftime('%Y', date_finished) as y 
        FROM reading_logs WHERE date_finished IS NOT NULL ORDER BY y DESC
    """).fetchall()
    available_years = [int(r['y']) for r in years_rows if r['y']]
    if current_year not in available_years: available_years.insert(0, current_year)
    
    # Monthly Data Query
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
    conn.close()
    
    # Initialize 12 months of zero data
    labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    data_books = [0] * 12
    data_hours = [0] * 12
    data_pages = [0] * 12
    
    # Fill in the actual data
    for r in rows:
        idx = int(r['month']) - 1
        data_books[idx] = r['books']
        data_hours[idx] = round(r['hours'], 1)
        data_pages[idx] = r['pages']
        
    # FIX: Pass RAW LISTS (removed json.dumps)
    return templates.TemplateResponse("stats.html", {
        "request": request,
        "selected_year": selected_year,
        "available_years": available_years,
        "labels": labels,         # Raw List
        "data_books": data_books, # Raw List
        "data_hours": data_hours, # Raw List
        "data_pages": data_pages  # Raw List
    })

@app.get("/library", response_class=HTMLResponse)
async def library(request: Request, q: str = ""):
    conn = get_db_connection()
    query = """
        SELECT ub.id, b.title, b.author, b.cover_url, ub.read_status, ub.shelf_status, ub.formats_owned 
        FROM user_books ub
        JOIN books b ON ub.book_id = b.id
    """
    params = []
    if q:
        query += " WHERE b.title LIKE ? OR b.author LIKE ?"
        params = [f"%{q}%", f"%{q}%"]
    
    query += " ORDER BY ub.date_added DESC"
    books = conn.execute(query, params).fetchall()
    conn.close()
    
    books_data = []
    for row in books:
        r = dict(row)
        r['formats'] = json.loads(r['formats_owned']) if r['formats_owned'] else []
        books_data.append(r)

    return templates.TemplateResponse("library.html", {"request": request, "books": books_data, "query": q})

@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    return templates.TemplateResponse("search.html", {"request": request})

@app.post("/api/search_google")
async def search_google(request: Request, query: str = Form(...)):
    if not query: return ""
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=10")
        data = resp.json()
    
    results = []
    if "items" in data:
        for item in data["items"]:
            vol = item.get("volumeInfo", {})
            
            raw_cover = vol.get("imageLinks", {}).get("thumbnail", "/static/placeholder.png")
            cover = raw_cover.replace("http://", "https://").replace("&edge=curl", "").replace("zoom=1", "zoom=0")
            
            # ISBN Extraction
            isbn = None
            for ident in vol.get("industryIdentifiers", []):
                if ident["type"] == "ISBN_13":
                    isbn = ident["identifier"]
            
            genres = vol.get("categories", ["Unknown"])
            rating = vol.get("averageRating", 0)
            
            results.append({
                "google_id": item["id"],
                "isbn13": isbn,
                "title": vol.get("title", "Unknown Title"),
                "author": ", ".join(vol.get("authors", ["Unknown"])),
                "year": vol.get("publishedDate", "")[:4],
                "cover": cover,
                "pages": vol.get("pageCount", 0),
                "summary": vol.get("description", "No description available."),
                "genres": ", ".join(genres),
                "rating": rating
            })
            
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
    isbn13: str = Form(None) # Capture ISBN
):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM books WHERE google_id = ?", (google_id,))
    row = cursor.fetchone()
    
    if row:
        book_id = row['id']
        cursor.execute("""
            UPDATE books 
            SET publication_year = ?, genres = ?, average_rating = ?, summary = ?, isbn13 = ?
            WHERE id = ?
        """, (year, genres, rating, summary, isbn13, book_id))
    else:
        cursor.execute("""
            INSERT INTO books (google_id, isbn13, title, author, cover_url, total_pages, summary, genres, average_rating, publication_year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (google_id, isbn13, title, author, cover, pages, summary, genres, rating, year))
        book_id = cursor.lastrowid
    
    cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "Already in Library"
    
    cursor.execute("INSERT INTO user_books (book_id) VALUES (?)", (book_id,))
    conn.commit()
    conn.close()
    
    return "✅ Added"

@app.get("/book/{id}", response_class=HTMLResponse)
async def book_detail(request: Request, id: int):
    conn = get_db_connection()
    book = conn.execute("""
        SELECT ub.*, b.* FROM user_books ub 
        JOIN books b ON ub.book_id = b.id 
        WHERE ub.id = ?
    """, (id,)).fetchone()
    
    if not book: raise HTTPException(status_code=404, detail="Book not found")
        
    logs = conn.execute("SELECT * FROM reading_logs WHERE user_book_id = ? ORDER BY date_finished DESC", (id,)).fetchall()
    conn.close()
    
    formats_owned = json.loads(book['formats_owned']) if book['formats_owned'] else []
    
    return templates.TemplateResponse("book_detail.html", {
        "request": request, 
        "book": book, 
        "logs": logs,
        "formats_owned": formats_owned
    })

@app.post("/book/{id}/delete")
async def delete_book(id: int):
    conn = get_db_connection()
    # Cascade Delete: Logs first, then the book
    conn.execute("DELETE FROM reading_logs WHERE user_book_id = ?", (id,))
    conn.execute("DELETE FROM user_books WHERE id = ?", (id,))
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
    # Permanent Ownership Check
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
        
    # Borrowed Check
    if libby_audio: formats.append("Libby Audiobook")
    if libby_physical: formats.append("Libby Physical")
    if libby_ebook: formats.append("Libby eBook")
    
    # Ownership Logic: If ANY permanent format is present, I own it.
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

# ... [Keep add_log, edit_log, trigger_backup from previous versions] ...
# (They don't need changes for this update)

# --- NEW AUTHOR ROUTE ---
@app.get("/author/{name}", response_class=HTMLResponse)
async def author_page(request: Request, name: str):
    conn = get_db_connection()
    
    # We use LIKE to find the author. 
    # This handles exact matches perfectly.
    query = """
        SELECT ub.id, b.title, b.author, b.cover_url, ub.read_status, ub.shelf_status, ub.formats_owned, ub.is_owned
        FROM user_books ub
        JOIN books b ON ub.book_id = b.id
        WHERE b.author LIKE ?
        ORDER BY b.publication_year DESC
    """
    
    # The % signs allow for flexibility if your data is messy
    books = conn.execute(query, (f"%{name}%",)).fetchall()
    conn.close()
    
    # Parse formats JSON for the template
    books_data = []
    for row in books:
        r = dict(row)
        r['formats'] = json.loads(r['formats_owned']) if r['formats_owned'] else []
        books_data.append(r)

    return templates.TemplateResponse("author.html", {
        "request": request, 
        "books": books_data, 
        "author_name": name
    })

# Re-pasting add_log just for completeness so you don't miss it
@app.post("/book/{id}/add_log")
async def add_log(id: int, date_finished: str = Form(...), hours: float = Form(...), format_consumed: str = Form(...), pace: str = Form("Medium"), notes: str = Form(""), is_dnf: bool = Form(False)):
    conn = get_db_connection()
    conn.execute("INSERT INTO reading_logs (user_book_id, date_finished, hours_read, format_consumed, pace, log_notes, is_dnf) VALUES (?, ?, ?, ?, ?, ?, ?)", (id, date_finished, hours, format_consumed, pace, notes, is_dnf))
    if not is_dnf:
        conn.execute("UPDATE user_books SET read_status = 'Read' WHERE id = ?", (id,))
    else:
        conn.execute("UPDATE user_books SET read_status = 'DNF' WHERE id = ? AND read_status != 'Read'", (id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{id}", status_code=303)

# Log Management Routes (Edit/Delete Log) from previous step
@app.get("/log/{log_id}/edit", response_class=HTMLResponse)
async def edit_log_page(request: Request, log_id: int):
    conn = get_db_connection()
    log = conn.execute("SELECT * FROM reading_logs WHERE id = ?", (log_id,)).fetchone()
    conn.close()
    if not log: raise HTTPException(status_code=404, detail="Log not found")
    return templates.TemplateResponse("edit_log.html", {"request": request, "log": log})

@app.post("/log/{log_id}/edit")
async def update_log(log_id: int, date_finished: str = Form(...), hours: float = Form(...), format_consumed: str = Form(...), pace: str = Form("Medium"), notes: str = Form(""), is_dnf: bool = Form(False)):
    conn = get_db_connection()
    conn.execute("UPDATE reading_logs SET date_finished = ?, hours_read = ?, format_consumed = ?, pace = ?, log_notes = ?, is_dnf = ? WHERE id = ?", (date_finished, hours, format_consumed, pace, notes, is_dnf, log_id))
    row = conn.execute("SELECT user_book_id FROM reading_logs WHERE id = ?", (log_id,)).fetchone()
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
        conn.commit()
        conn.close()
        return RedirectResponse(url=f"/book/{book_id}", status_code=303)
    conn.close()
    return RedirectResponse(url="/", status_code=303)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)