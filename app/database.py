import sqlite3
import os
from datetime import datetime

# Relative path for Windows/Docker compatibility
DB_FOLDER = os.path.join(os.getcwd(), "data") 
DB_PATH = os.path.join(DB_FOLDER, "library.db")
BACKUP_DIR = os.path.join(DB_FOLDER, "backups")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    os.makedirs(DB_FOLDER, exist_ok=True)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # 1. BOOKS (Reference)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        google_id TEXT UNIQUE,
        isbn13 TEXT,
        asin TEXT,              -- Amazon ID
        olid TEXT,              -- Open Library ID
        goodreads_id TEXT,
        title TEXT NOT NULL,
        subtitle TEXT,
        author TEXT,
        series_name TEXT,
        series_index REAL,
        publisher TEXT,
        publication_year TEXT,
        language TEXT DEFAULT 'en',
        cover_url TEXT,
        cover_path TEXT,
        total_pages INTEGER,
        summary TEXT,
        genres TEXT,
        average_rating REAL,
        content_score INTEGER DEFAULT 0  -- <--- NEW COLUMN
    )
    ''')
    
    # 2. User Inventory
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER NOT NULL,
        read_status TEXT DEFAULT 'Unread',
        shelf_status TEXT DEFAULT 'Shelved',
        effective_user_rating REAL, -- Calculated average from log sessions
        is_owned BOOLEAN DEFAULT 0,
        formats_owned TEXT,
        inventory_notes TEXT,
        acquired_source TEXT,   -- e.g., "Amazon", "Gift", "Used Bookstore"
        acquired_date DATE,     
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(book_id) REFERENCES books(id)
    )
    ''')
    
    # 3. Reading Logs
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reading_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_book_id INTEGER,
        date_finished DATE,
        hours_read REAL,
        format_consumed TEXT,
        is_borrowed BOOLEAN DEFAULT 0,
        is_dnf BOOLEAN DEFAULT 0,
        pace TEXT,
        log_notes TEXT,
        session_rating REAL,  -- The rating for this specific read
        FOREIGN KEY(user_book_id) REFERENCES user_books(id)
    )
    ''')
    
    conn.commit()
    conn.close()

def backup_database():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_file = os.path.join(BACKUP_DIR, f"library_{timestamp}.db")
    try:
        src = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        dst = sqlite3.connect(backup_file)
        src.backup(dst)
        dst.close()
        src.close()
        return f"Success: {backup_file}"
    except Exception as e:
        return f"Error: {e}"