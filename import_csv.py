import csv
import sqlite3
import time
import json
import os
import sys
import asyncio
from dateutil import parser
from datetime import datetime

# --- IMPORT YOUR SMART SEARCH ---
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

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def clean_date(date_str):
    try:
        dt = parser.parse(date_str)
        return dt.strftime("%Y-%m-%d"), dt.year
    except:
        now = datetime.now()
        return now.strftime("%Y-%m-%d"), now.year

def get_skip_value(row):
    """Robustly finds 'Skip Import' column."""
    val = row.get("Skip Import")
    if val is not None: return val
    for key in row.keys():
        if key and key.strip().lower() == "skip import":
            return row[key]
    return ""

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
            
            # --- 0. YEAR FILTER ---
            clean_dt, row_year = clean_date(date_raw)
            if FILTER_YEAR is not None and row_year != FILTER_YEAR:
                skipped_count += 1
                continue

            if not title: continue 
            print(f"Processing: {title}...")

            # --- CHECK SKIP IMPORT ---
            raw_skip = get_skip_value(row)
            is_skipped = "yes" in str(raw_skip).strip().lower()

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
            if csv_isbn:
                cursor.execute("SELECT id, title, total_pages FROM books WHERE isbn13 = ?", (csv_isbn,))
                book_row = cursor.fetchone()
            
            if not book_row:
                cursor.execute("SELECT id, title, total_pages FROM books WHERE title LIKE ?", (f"{title}%",))
                book_row = cursor.fetchone()
            
            book_id = None
            total_pages = 0
            
            if book_row:
                # EXISTING BOOK
                if is_skipped:
                      print(f"  -> [SKIP] User flagged to skip (Book exists in DB).")
                      row['Error_Reason'] = "User Skipped (Exists in DB)"
                      row['Found_Title'] = book_row['title']
                      failed_rows.append(row)
                      failure_count += 1
                      continue

                book_id = book_row['id']
                total_pages = book_row['total_pages'] or 0
                print(f"  -> [MATCH] Existing DB ID {book_id}: '{book_row['title']}'")
            else:
                # --- NEW BOOK LOGIC (UPDATED) ---
                
                # STRATEGY CHANGE: 
                # Instead of searching strictly by "isbn:...", we search by Title + Author.
                # Why? This casts a wider net (20 results) instead of just 1.
                # Then we pass the ISBN to 'match_isbn' so metadata.py can pick the perfect winner
                # from that list of 20, even if the API result title is slightly weird.
                search_query = f"{title} {author}".strip()
                
                # Run Search with the ISBN Validator
                candidates = asyncio.run(search_aggregated(search_query, match_isbn=csv_isbn))
                
                best_match = None
                if candidates:
                    # Candidates are already sorted by Match Score -> Content Score
                    best_match = candidates[0]
                    print(f"  -> [MATCH] Source: {best_match['source']} | Match: {best_match['match_score']} | Content: {best_match['content_score']}")

                if best_match:
                    row['Found_Title'] = best_match['title']

                # --- CHECK SKIP ---
                if is_skipped:
                    print(f"  -> [SKIP] User flagged to skip.")
                    row['Error_Reason'] = "User Skipped"
                    failed_rows.append(row)
                    failure_count += 1
                    continue

                # --- VALIDATE ---
                if not best_match:
                    print(f"  -> [FAIL] No matches met threshold.")
                    row['Error_Reason'] = "Low Confidence / No Match"
                    failed_rows.append(row)
                    failure_count += 1
                    continue

                # --- INSERT BOOK ---
                final_subtitle = csv_subtitle
                final_isbn = csv_isbn if csv_isbn else best_match['isbn']
                final_olid = csv_olid if csv_olid else best_match['olid']
                
                # Use .get() specifically for content_score since older searches might not have it
                content_score = best_match.get('content_score', 0)

                cursor.execute("""
                    INSERT INTO books (google_id, isbn13, asin, olid, title, subtitle, author, publication_year, cover_url, total_pages, summary, genres, average_rating, content_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    best_match['source_id'], final_isbn, csv_asin, final_olid,
                    best_match['title'], final_subtitle, best_match['author'], 
                    best_match['year'], best_match['cover'], best_match['pages'], 
                    best_match['summary'], best_match['genres'], best_match['rating'],
                    content_score
                ))
                book_id = cursor.lastrowid
                total_pages = best_match['pages']
                time.sleep(0.1) 

            # --- 4. CREATE USER_BOOKS ---
            if book_id is None:
                print("CRITICAL ERROR: book_id is None. Skipping row.")
                continue

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
                try: final_hours = float(hours_raw)
                except: final_hours = round(total_pages / 40, 1) if total_pages else 0

                cursor.execute("SELECT id FROM reading_logs WHERE user_book_id = ? AND date_finished = ?", (user_book_id, clean_dt))
                if not cursor.fetchone():
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