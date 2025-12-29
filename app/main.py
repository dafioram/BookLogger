from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import httpx
import json
import math
from datetime import date
from .database import init_db, get_db_connection, backup_database

# --- LIFESPAN (Startup/Shutdown) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic: Run this before the app starts accepting requests
    init_db()
    yield
    # Shutdown logic: Run this when the app stops (optional)
    # e.g., print("Goodbye!")

# --- APP DEFINITION ---
app = FastAPI(lifespan=lifespan)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# --- UTILITIES ---
def format_minutes(mins):
    if not mins: return ""
    h = math.floor(mins / 60)
    m = mins % 60
    return f"{h}h {m}m"

templates.env.filters["format_minutes"] = format_minutes

# --- ROUTES ---

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    conn = get_db_connection()
    
    # Stats: Current Year
    current_year = date.today().year
    stats_query = """
        SELECT 
            COUNT(DISTINCT l.id) as books_read, 
            SUM(l.hours_read) as total_hours,
            SUM(b.total_pages) as total_pages
        FROM reading_logs l
        JOIN user_books ub ON l.user_book_id = ub.id
        JOIN books b ON ub.book_id = b.id
        WHERE strftime('%Y', l.date_finished) = ? AND l.is_dnf = 0
    """
    stats = conn.execute(stats_query, (str(current_year),)).fetchone()
    
    # On Deck
    on_deck = conn.execute("""
        SELECT ub.id, b.title, b.cover_url 
        FROM user_books ub 
        JOIN books b ON ub.book_id = b.id 
        WHERE ub.status = 'On Deck'
        LIMIT 5
    """).fetchall()
    
    # Recent Logs
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
        "on_deck": on_deck, 
        "recent": recent,
        "year": current_year
    })

@app.get("/library", response_class=HTMLResponse)
async def library(request: Request, q: str = ""):
    conn = get_db_connection()
    query = """
        SELECT ub.id, b.title, b.author, b.cover_url, ub.status, ub.formats_owned 
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
    
    # Helper to parse JSON formats for the template
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
    if not query:
        return ""
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=10")
        data = resp.json()
    
    results = []
    if "items" in data:
        for item in data["items"]:
            vol = item.get("volumeInfo", {})
            # Safe extraction
            results.append({
                "google_id": item["id"],
                "title": vol.get("title", "Unknown Title"),
                "author": ", ".join(vol.get("authors", ["Unknown"])),
                "year": vol.get("publishedDate", "")[:4],
                "cover": vol.get("imageLinks", {}).get("thumbnail", "/static/placeholder.png"),
                "pages": vol.get("pageCount", 0),
                "summary": vol.get("description", "")
            })
            
    return templates.TemplateResponse("partials/search_row.html", {"request": request, "results": results})

@app.post("/api/add_book")
async def add_book(
    google_id: str = Form(...), 
    title: str = Form(...), 
    author: str = Form(...), 
    cover: str = Form(...),
    pages: int = Form(0),
    summary: str = Form("")
):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Check if book exists in Global Reference
    cursor.execute("SELECT id FROM books WHERE google_id = ?", (google_id,))
    row = cursor.fetchone()
    
    if row:
        book_id = row['id']
    else:
        cursor.execute("""
            INSERT INTO books (google_id, title, author, cover_url, total_pages, summary)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (google_id, title, author, cover, pages, summary))
        book_id = cursor.lastrowid
    
    # 2. Check if user already has it
    cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "Already in Library"
    
    # 3. Add to User Library
    cursor.execute("INSERT INTO user_books (book_id) VALUES (?)", (book_id,))
    conn.commit()
    conn.close()
    
    return "✅ Added"

@app.get("/book/{id}", response_class=HTMLResponse)
async def book_detail(request: Request, id: int):
    conn = get_db_connection()
    
    # Fetch Book & User Data
    book = conn.execute("""
        SELECT ub.*, b.* FROM user_books ub 
        JOIN books b ON ub.book_id = b.id 
        WHERE ub.id = ?
    """, (id,)).fetchone()
    
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
        
    # Fetch Logs
    logs = conn.execute("""
        SELECT * FROM reading_logs WHERE user_book_id = ? ORDER BY date_finished DESC
    """, (id,)).fetchall()
    
    conn.close()
    
    # Parse Formats JSON
    formats_owned = json.loads(book['formats_owned']) if book['formats_owned'] else []
    
    return templates.TemplateResponse("book_detail.html", {
        "request": request, 
        "book": book, 
        "logs": logs,
        "formats_owned": formats_owned
    })

# --- INVENTORY UPDATES ---

@app.post("/book/{id}/update_inventory")
async def update_inventory(
    id: int, 
    status: str = Form(...),
    inventory_notes: str = Form(""),
    physical: str = Form(None),
    kindle: str = Form(None),
    audible: str = Form(None)
):
    # Collect formats into JSON
    formats = []
    if physical: formats.append("Physical")
    if kindle: formats.append("Kindle")
    if audible: formats.append("Audible")
    
    conn = get_db_connection()
    conn.execute("""
        UPDATE user_books 
        SET status = ?, inventory_notes = ?, formats_owned = ?
        WHERE id = ?
    """, (status, inventory_notes, json.dumps(formats), id))
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
    is_dnf: bool = Form(False)
):
    conn = get_db_connection()
    conn.execute("""
        INSERT INTO reading_logs (user_book_id, date_finished, hours_read, format_consumed, pace, log_notes, is_dnf)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (id, date_finished, hours, format_consumed, pace, notes, is_dnf))
    
    # Update status to Read if not DNF
    if not is_dnf:
        conn.execute("UPDATE user_books SET status = 'Read' WHERE id = ?", (id,))
        
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/book/{id}", status_code=303)

@app.post("/system/backup")
async def trigger_backup():
    result = backup_database()
    return result
    
if __name__ == "__main__":
    import uvicorn
    # This block only runs if you execute 'python -m app.main'
    # It is IGNORED if you run 'uvicorn app.main:app'
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)