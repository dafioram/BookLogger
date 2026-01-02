import csv
import sqlite3
import time
import json
import os
import sys
import difflib
import asyncio
from dateutil import parser
from datetime import datetime

# --- IMPORT YOUR SMART SEARCH ---
# This requires that 'app' is a python package (has __init__.py)
# and you run this script from the root folder.
try:
    from app.metadata import search_aggregated
except ImportError:
    print("Error: Could not import 'app.metadata'. Make sure you run this from the project root.")
    print("Usage: python import_csv.py [filename]")
    sys.exit(1)

# --- CONFIGURATION ---
DEFAULT_CSV = "Books Read.csv" 
FAILURE_FILE = "import_failures.csv"
DB_PATH = "data/library.db"
MATCH_THRESHOLD = 0.65 

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

# --- ARGUMENT PARSING ---
if len(sys.argv) > 1:
    CSV_FILE = sys.argv[1]
else:
    CSV_FILE = DEFAULT_CSV

FILTER_YEAR = None 
if len(sys.argv) > 2:
    try:
        FILTER_YEAR = int(sys.argv[2])
    except ValueError:
        print("Error: Year must be a number.")
        sys.exit(1)

print(f"--> Using file: {CSV_FILE}")
if FILTER_YEAR:
    print(f"--> Filtering for Year: {FILTER_YEAR}")
else:
    print(f"--> Mode: Processing ALL years")

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

def clean_date(date_str):
    try:
        dt = parser.parse(date_str)
        return dt.strftime("%Y-%m-%d"), dt.year
    except:
        now = datetime.now()
        return now.strftime("%Y-%m-%d"), now.year

def run_import():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    success_count = 0
    failure_count = 0
    skipped_count = 0
    failed_rows = []

    print(f"--- STARTING IMPORT ---")

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames + ['Error_Reason', 'Found_Title']
        
        for row in reader:
            title = row.get("Title", "").strip()
            date_raw = row.get("Date Finished")
            
            # --- 0. YEAR FILTER CHECK ---
            clean_dt, row_year = clean_date(date_raw)
            if FILTER_YEAR is not None and row_year != FILTER_YEAR:
                skipped_count += 1
                continue

            if not title: continue 
            print(f"Processing: {title}...")

            author = row.get("Author", "").strip()
            
            # --- 1. CAPTURE CSV METADATA ---
            csv_subtitle = row.get("Subtitle", "").strip()
            csv_isbn = row.get("ISBN13", "").strip().replace("-", "")
            csv_asin = row.get("ASIN", "").strip()
            csv_olid = row.get("OLID", "").strip()
            
            # --- 2. CAPTURE USER DATA ---
            user_rating_raw = row.get("My Rating") or row.get("Rating")
            user_rating = None
            if user_rating_raw:
                try: user_rating = float(user_rating_raw)
                except: user_rating = None

            service_raw = row.get("Service", "Physical").strip()
            own_raw = row.get("Own?", "").lower()
            
            defaults = SERVICE_DEFAULTS.get(service_raw, ("Physical", False))
            format_consumed = defaults[0]
            
            if "yes" in own_raw:
                is_owned = True
                is_borrowed = False
            elif "no" in own_raw:
                is_owned = False
                is_borrowed = True
            else:
                is_owned = not defaults[1] 
                is_borrowed = defaults[1]

            # --- 3. BOOK LOOKUP ---
            book_row = None
            # Check DB by ISBN
            if csv_isbn:
                cursor.execute("SELECT id, title, total_pages FROM books WHERE isbn13 = ?", (csv_isbn,))
                book_row = cursor.fetchone()
            
            # Check DB by Title
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
                # --- NEW LOGIC: Use Smart Aggregated Search ---
                search_query = title
                if csv_isbn:
                    search_query = f"isbn:{csv_isbn}"
                else:
                    search_query = f"{title} {author}"

                # Run the Async function synchronously
                candidates = asyncio.run(search_aggregated(search_query))
                
                # Filter candidates by similarity to ensure we didn't get a high-quality WRONG book
                best_match = None
                
                for cand in candidates:
                    # If searched by ISBN, we trust it implicitly
                    if csv_isbn:
                        best_match = cand
                        break
                        
                    # Otherwise, check similarity
                    sim = calculate_similarity(title, cand['title'])
                    if sim >= MATCH_THRESHOLD:
                        best_match = cand
                        print(f"  -> [MATCH] Source: {cand['source']} | Score: {cand['score']} | Similarity: {int(sim*100)}%")
                        break
                
                if not best_match:
                    print(f"  -> [FAIL] No matching results found.")
                    row['Error_Reason'] = "No Matches Found"
                    failed_rows.append(row)
                    failure_count += 1
                    continue

                final_subtitle = csv_subtitle # We don't get subtitle from aggregator currently
                final_isbn = csv_isbn if csv_isbn else best_match['isbn']
                final_olid = csv_olid if csv_olid else best_match['olid']
                
                # Map standardized keys to DB columns
                # Note: We use 'source_id' for 'google_id' column to enforce uniqueness
                cursor.execute("""
                    INSERT INTO books (google_id, isbn13, asin, olid, title, subtitle, author, publication_year, cover_url, total_pages, summary, genres, average_rating)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    best_match['source_id'], final_isbn, csv_asin, final_olid,
                    best_match['title'], final_subtitle, best_match['author'], 
                    best_match['year'], best_match['cover'], best_match['pages'], 
                    best_match['summary'], best_match['genres'], best_match['rating']
                ))
                book_id = cursor.lastrowid
                total_pages = best_match['pages']
                time.sleep(0.1) 

            # --- 4. CREATE USER_BOOKS ---
            cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
            ub_row = cursor.fetchone()
            
            if ub_row:
                user_book_id = ub_row['id']
                if user_rating:
                    cursor.execute("UPDATE user_books SET effective_user_rating = ? WHERE id = ?", (user_rating, user_book_id))
            else:
                formats = [format_consumed]
                cursor.execute("""
                    INSERT INTO user_books (book_id, read_status, shelf_status, is_owned, formats_owned, effective_user_rating)
                    VALUES (?, 'Read', 'Shelved', ?, ?, ?)
                """, (book_id, is_owned, json.dumps(formats), user_rating))
                user_book_id = cursor.lastrowid

            # --- 5. LOG READING ---
            try:
                hours_raw = row.get("Hours")
                try:
                    final_hours = float(hours_raw)
                except:
                    final_hours = round(total_pages / 40, 1) if total_pages else 0

                cursor.execute("SELECT id FROM reading_logs WHERE user_book_id = ? AND date_finished = ?", (user_book_id, clean_dt))
                if not cursor.fetchone():
                    # UPDATED: Includes session_rating
                    cursor.execute("""
                        INSERT INTO reading_logs (user_book_id, date_finished, hours_read, format_consumed, is_borrowed, session_rating)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (user_book_id, clean_dt, final_hours, format_consumed, is_borrowed, user_rating))
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
    year_label = str(FILTER_YEAR) if FILTER_YEAR else "ALL YEARS"
    print(f"Summary for {year_label}:")
    print(f"Imported: {success_count}")
    print(f"Skipped:  {skipped_count} (Filter)")
    print(f"Failed:   {failure_count}")
    print("="*40)

    if failed_rows:
        with open(FAILURE_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(failed_rows)
        print(f"Check {FAILURE_FILE} for errors.")

if __name__ == "__main__":
    run_import()