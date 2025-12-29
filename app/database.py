import sqlite3
import os
from datetime import datetime

# Use a relative path so it works on Windows AND Docker
# If running from root, this puts data in ./data
DB_FOLDER = os.path.join(os.getcwd(), "data") 

DB_PATH = os.path.join(DB_FOLDER, "library.db")
BACKUP_DIR = os.path.join(DB_FOLDER, "backups")

def get_db_connection():
    """Returns a connection with Row factory enabled for dictionary-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Creates tables if they don't exist."""
    os.makedirs(DB_FOLDER, exist_ok=True)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Enable WAL mode for concurrency
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # 1. Books (Reference)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        google_id TEXT UNIQUE,
        title TEXT NOT NULL,
        author TEXT,
        publication_year INTEGER,
        cover_url TEXT,
        total_pages INTEGER,
        summary TEXT,
        genres TEXT,
        average_rating REAL,
        series_name TEXT,
        series_index REAL,
        audio_duration_minutes INTEGER
    )
    ''')
    
    # 2. User Inventory
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER NOT NULL,
        status TEXT DEFAULT 'Unread', -- Unread, Read, DNF, On Deck
        is_owned BOOLEAN DEFAULT 0,
        formats_owned TEXT, -- JSON list
        inventory_notes TEXT,
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(book_id) REFERENCES books(id)
    )
    ''')
    
    # 3. Reading Logs (History)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reading_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_book_id INTEGER NOT NULL,
        format_consumed TEXT,
        date_started DATE,
        date_finished DATE,
        hours_read REAL,
        pace TEXT,
        is_dnf BOOLEAN DEFAULT 0,
        log_notes TEXT,
        FOREIGN KEY(user_book_id) REFERENCES user_books(id)
    )
    ''')
    
    conn.commit()
    conn.close()

def backup_database():
    """Binary backup of the SQLite file."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_file = os.path.join(BACKUP_DIR, f"library_{timestamp}.db")
    
    try:
        # Connect to source (Read Only)
        src = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        dst = sqlite3.connect(backup_file)
        
        src.backup(dst)
        
        dst.close()
        src.close()
        return f"Success: {backup_file}"
    except Exception as e:
        return f"Error: {e}"