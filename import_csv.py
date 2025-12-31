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

# Minimum similarity ratio (0.0 to 1.0)
MATCH_THRESHOLD = 0.65 

SERVICE_MAP = {
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

def fetch_google_candidates(title, author, limit=3):
    query = f"intitle:{title}"
    if author:
        query += f" inauthor:{author}"
        
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
                
                results.append({
                    "google_id": item["id"],
                    "title": info.get("title", "Unknown"),
                    "subtitle": info.get("subtitle", ""),  # <-- Capture Google Subtitle
                    "author": ", ".join(info.get("authors", ["Unknown"])),
                    "year": info.get("publishedDate", "")[:4],
                    "cover": cover,
                    "pages": info.get("pageCount", 0),
                    "summary": info.get("description", ""),
                    "genres": ", ".join(info.get("categories", [])),
                    "rating": info.get("averageRating", 0)
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
        fieldnames = reader.fieldnames + ['Error_Reason', 'Found_Title', 'Alternative_1', 'Alternative_2']
        
        for row in reader:
            title = row.get("Title", "").strip()
            csv_subtitle = row.get("Subtitle", "").strip() # <-- Read CSV Subtitle
            author = row.get("Author", "").strip()
            
            if not title: continue 

            print(f"Processing: {title}...")

            # 1. SEARCH LOCAL DB FIRST
            cursor.execute("SELECT id, title, total_pages FROM books WHERE title LIKE ?", (f"{title}%",))
            book_row = cursor.fetchone()
            
            book_id = None
            total_pages = 0
            
            if book_row:
                book_id = book_row['id']
                total_pages = book_row['total_pages'] or 0
                print(f"  -> [MATCH] Found existing book ID {book_id}: '{book_row['title']}'")
            else:
                # 2. SEARCH GOOGLE
                candidates = fetch_google_candidates(title, author, limit=3)
                
                if not candidates:
                    print(f"  -> [FAIL] No results found on Google Books.")
                    row['Error_Reason'] = "No Google Results"
                    failed_rows.append(row)
                    failure_count += 1
                    continue
                
                best_match = candidates[0]
                similarity = calculate_similarity(title, best_match['title'])
                
                row['Found_Title'] = best_match['title']
                if len(candidates) > 1: row['Alternative_1'] = f"{candidates[1]['title']} ({candidates[1]['author']})"
                if len(candidates) > 2: row['Alternative_2'] = f"{candidates[2]['title']} ({candidates[2]['author']})"

                # 3. STRICT CHECK
                if similarity < MATCH_THRESHOLD:
                    print(f"  -> [FAIL] Poor Match ({int(similarity*100)}%). Searched '{title}', Found '{best_match['title']}'")
                    row['Error_Reason'] = f"Low Confidence Match ({int(similarity*100)}%)"
                    failed_rows.append(row)
                    failure_count += 1
                    continue
                
                print(f"  -> [MATCH] Confidence {int(similarity*100)}%: '{best_match['title']}'")

                # PRIORITY: CSV Subtitle > Google Subtitle > Empty
                final_subtitle = csv_subtitle if csv_subtitle else best_match['subtitle']

                # Create Book
                total_pages = best_match['pages']
                # Updated INSERT to include subtitle
                cursor.execute("""
                    INSERT INTO books (google_id, title, subtitle, author, publication_year, cover_url, total_pages, summary, genres, average_rating)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (best_match['google_id'], best_match['title'], final_subtitle, best_match['author'], best_match['year'], best_match['cover'], best_match['pages'], best_match['summary'], best_match['genres'], best_match['rating']))
                book_id = cursor.lastrowid
                time.sleep(0.5) 

            # 4. CREATE LOGS
            service_raw = row.get("Service", "Physical").strip()
            fmt_info = SERVICE_MAP.get(service_raw, ("Physical", False))
            
            cursor.execute("SELECT id FROM user_books WHERE book_id = ?", (book_id,))
            ub_row = cursor.fetchone()
            if ub_row:
                user_book_id = ub_row['id']
            else:
                is_owned = not fmt_info[1]
                formats = [fmt_info[0]]
                cursor.execute("""
                    INSERT INTO user_books (book_id, read_status, shelf_status, is_owned, formats_owned)
                    VALUES (?, 'Read', 'Shelved', ?, ?)
                """, (book_id, is_owned, json.dumps(formats)))
                user_book_id = cursor.lastrowid

            try:
                date_raw = row.get("Date Finished")
                clean_dt = clean_date(date_raw)
                
                hours_raw = row.get("Hours")
                try:
                    final_hours = float(hours_raw)
                except:
                    final_hours = round(total_pages / 40, 1) if total_pages else 0

                format_consumed = fmt_info[0]
                is_borrowed = fmt_info[1]

                cursor.execute("SELECT id FROM reading_logs WHERE user_book_id = ? AND date_finished = ?", (user_book_id, clean_dt))
                if not cursor.fetchone():
                    # Removed read_status_snapshot to prevent error
                    cursor.execute("""
                        INSERT INTO reading_logs (user_book_id, date_finished, hours_read, format_consumed, is_borrowed)
                        VALUES (?, ?, ?, ?, ?)
                    """, (user_book_id, clean_dt, final_hours, format_consumed, is_borrowed))
                    print(f"     -> Log added.")
                    success_count += 1
                else:
                    print(f"     -> Log exists. Skipped.")
            
            except Exception as e:
                print(f"  -> [FAIL] DB Error: {e}")
                row['Error_Reason'] = f"DB Error: {e}"
                failed_rows.append(row)
                failure_count += 1
                continue

    conn.commit()
    conn.close()

    print("\n" + "="*40)
    print(f"STRICT IMPORT SUMMARY")
    print(f"Matches: {success_count}")
    print(f"Failed:  {failure_count}")
    print("="*40)

    if failed_rows:
        with open(FAILURE_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(failed_rows)
        print(f"\n[!] Failures saved to '{FAILURE_FILE}'.")

if __name__ == "__main__":
    run_import()