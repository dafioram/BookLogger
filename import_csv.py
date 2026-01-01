import csv
import sqlite3
import time
import requests
import json
import os
import sys
import difflib
from dateutil import parser
from datetime import datetime

# --- CONFIGURATION ---
DEFAULT_CSV = "Books Read.csv" 
FAILURE_FILE = "import_failures.csv"
DB_PATH = "data/library.db"
MATCH_THRESHOLD = 0.65 

# Fallback defaults if "Own?" column is missing or empty
SERVICE_DEFAULTS = {
    "Audible": ("Audible", False),
    "Kindle": ("Kindle", False),
    "Physical": ("Physical", False),
    "Paperback": ("Physical", False),
    "Hardcover": ("Physical", False),
    "Kindle Unlimited": ("Kindle", True), 
    "Libby": ("Libby Audiobook", True),   
    "Library": ("Physical", True),
    "Spotify": ("Audible", True),
}

if len(sys.argv) > 1:
    CSV_FILE = sys.argv[1]
    print(f"--> Using custom file: {CSV_FILE}")
else:
    CSV_FILE = DEFAULT_CSV
    print(f"--> Using default file: {CSV_FILE}")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def calculate_similarity(s1, s2):
    if not s1 or not s2: return 0.0
    s1 = s1.lower().strip()
    s2 = s2.lower().strip()
    if s1 in s2 or s2 in s1: return 0.9 
    return difflib.SequenceMatcher(None, s1, s2).ratio()

def fetch_google_candidates(title, author, isbn=None, limit=3):
    if isbn:
        query = f"isbn:{isbn}"
        search_type = "ISBN"
    else:
        query = f"intitle:{title}"
        if author:
            query += f" inauthor:{author}"
        search_type = "Title"
        
    results = []
    try:
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={limit}&printType=books"
        response = requests.get(url)
        data = response.json()
        
        if "items" in data:
            for item in data["items"]:
                info = item.get("volumeInfo", {})
                raw_cover = info.get("imageLinks", {}).get("thumbnail", "")
                cover = raw_cover.replace("http://", "https://").replace("&edge=curl", "").replace("zoom=1", "zoom=0")
                
                # Extract ISBN
                found_isbn = ""
                for ident in info.get("industryIdentifiers", []):
                    if ident["type"] == "ISBN_13":
                        found_isbn = ident["identifier"]
                        break
                
                results.append({
                    "google_id": item["id"],
                    "title": info.get("title", "Unknown"),
                    "subtitle": info.get("subtitle", ""),
                    "author": ", ".join(info.get("authors", ["Unknown"])),
                    "year": info.get("publishedDate", "")[:4],
                    "cover": cover,
                    "pages": info.get("pageCount", 0),
                    "summary": info.get("description", ""),
                    "genres": ", ".join(info.get("categories", [])),
                    "rating": info.get("averageRating", 0),
                    "isbn13": found_isbn,
                    "search_method": search_type
                })
    except Exception as e:
        print(f"  -> API Error: {e}")
    return results

def clean_date(date_str):
    try:
        dt = parser.parse(date_str)
        return dt.strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

def run_import():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    success_count = 0
    failure_count = 0
    failed_rows = []

    print(f"--- STARTING IMPORT FROM {CSV_FILE} ---")

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames + ['Error_Reason', 'Found_Title']
        
        for row in reader:
            title = row.get("Title", "").strip()
            author = row.get("Author", "").strip()
            
            # --- 1. CAPTURE CSV METADATA ---
            csv_subtitle = row.get("Subtitle", "").strip()
            csv_isbn = row.get("ISBN13", "").strip().replace("-", "")
            csv_asin = row.get("ASIN", "").strip()
            csv_olid = row.get("OLID", "").strip()
            
            # --- 2. CAPTURE USER DATA (RATING & OWNERSHIP) ---
            # Parse Rating (Handle empty or "N/A")
            user_rating_raw = row.get("My Rating") or row.get("Rating")
            user_rating = None
            if user_rating_raw:
                try:
                    user_rating = float(user_rating_raw)
                except:
                    user_rating = None

            # Parse Ownership (Look for "Own?" column first)
            service_raw = row.get("Service", "Physical").strip()
            own_raw = row.get("Own?", "").lower()
            
            defaults = SERVICE_DEFAULTS.get(service_raw, ("Physical", False))
            
            # Format is usually tied to service
            format_consumed = defaults[0]
            
            # Logic: If CSV says "Yes", owned is True. If "No", owned is False.
            # If empty, fallback to Service Defaults (e.g. Kindle Unlimited = False)
            if "yes" in own_raw:
                is_owned = True
                is_borrowed = False
            elif "no" in own_raw:
                is_owned = False
                is_borrowed = True
            else:
                is_owned = not defaults[1] # Flip the "Is Borrowed" default
                is_borrowed = defaults[1]

            if not title: continue 
            print(f"Processing: {title}...")

            # --- 3. BOOK LOOKUP ---
            book_row = None
            if csv_isbn:
                cursor.execute("SELECT id, title, total_pages FROM books WHERE isbn13 = ?", (csv_isbn,))
                book_row = cursor.fetchone()
            
            if not book_row:
                cursor.execute("SELECT id, title, total_pages FROM books WHERE title LIKE ?", (f"{title}%",))
                book_row = cursor.fetchone()
            
            book_id = None
            total_pages = 0
            
            if book_row:
                book_id = book_row['id']
                total_pages = book_row['total_pages'] or 0
                print(f"  -> [MATCH] Existing DB ID {book_id}: '{book_row['title']}'")
            else:
                candidates = fetch_google_candidates(title, author, isbn=csv_isbn, limit=3)
                if not candidates and csv_isbn:
                    candidates = fetch_google_candidates(title, author, isbn=None, limit=3)

                if not candidates:
                    print(f"  -> [FAIL] No results found.")
                    row['Error_Reason'] = "No Google Results"
                    failed_rows.append(row)
                    failure_count += 1
                    continue
                
                best_match = candidates[0]
                similarity = calculate_similarity(title, best_match['title'])
                
                # Validation Logic
                if best_match['search_method'] == 'Title' and similarity < MATCH_THRESHOLD:
                     print(f"  -> [FAIL] Low Confidence ({int(similarity*100)}%)")
                     row['Error_Reason'] = f"Low Confidence ({int(similarity*100)}%)"
                     failed_rows.append(row)
                     failure_count += 1
                     continue
                
                print(f"  -> [MATCH] Confidence {int(similarity*100)}%: '{best_match['title']}'")

                # Merge CSV data with Google Data
                final_subtitle = csv_subtitle if csv_subtitle else best_match['subtitle']
                final_isbn = csv_isbn if csv_isbn else best_match['isbn13']

                total_pages = best_match['pages']
                cursor.execute("""
                    INSERT INTO books (google_id, isbn13, asin, olid, title, subtitle, author, publication_year, cover_url, total_pages, summary, genres, average_rating)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    best_match['google_id'], final_isbn, csv_asin, csv_olid,
                    best_match['title'], final_subtitle, best_match['author'], 
                    best_match['year'], best_match['cover'], best_match['pages'], 
                    best_match['summary'], best_match['genres'], best_match['rating']
                ))
                book_id = cursor.lastrowid
                time.sleep(0.5) 

            # --- 4. CREATE USER_BOOKS (With Rating & Ownership) ---
            cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
            ub_row = cursor.fetchone()
            
            if ub_row:
                user_book_id = ub_row['id']
                # Optional: Update rating if it exists in CSV but not in DB
                if user_rating:
                    cursor.execute("UPDATE user_books SET user_rating = ? WHERE id = ?", (user_rating, user_book_id))
            else:
                formats = [format_consumed]
                cursor.execute("""
                    INSERT INTO user_books (book_id, read_status, shelf_status, is_owned, formats_owned, user_rating)
                    VALUES (?, 'Read', 'Shelved', ?, ?, ?)
                """, (book_id, is_owned, json.dumps(formats), user_rating))
                user_book_id = cursor.lastrowid

            # --- 5. LOG READING ---
            try:
                date_raw = row.get("Date Finished")
                clean_dt = clean_date(date_raw)
                hours_raw = row.get("Hours")
                try:
                    final_hours = float(hours_raw)
                except:
                    final_hours = round(total_pages / 40, 1) if total_pages else 0

                cursor.execute("SELECT id FROM reading_logs WHERE user_book_id = ? AND date_finished = ?", (user_book_id, clean_dt))
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO reading_logs (user_book_id, date_finished, hours_read, format_consumed, is_borrowed)
                        VALUES (?, ?, ?, ?, ?)
                    """, (user_book_id, clean_dt, final_hours, format_consumed, is_borrowed))
                    print(f"     -> Log added.")
                    success_count += 1
                else:
                    print(f"     -> Log exists.")
            
            except Exception as e:
                print(f"  -> [FAIL] DB Error: {e}")
                row['Error_Reason'] = f"DB Error: {e}"
                failed_rows.append(row)
                failure_count += 1
                continue

    conn.commit()
    conn.close()

    print("\n" + "="*40)
    print(f"Summary: {success_count} Success, {failure_count} Failed")
    if failed_rows:
        with open(FAILURE_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(failed_rows)
        print(f"Check {FAILURE_FILE} for errors.")

if __name__ == "__main__":
    run_import()