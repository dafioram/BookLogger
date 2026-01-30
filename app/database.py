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
        asin TEXT,
        olid TEXT,
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
        total_audio_minutes INTEGER DEFAULT 0,
        summary TEXT,
        genres TEXT,
        average_rating REAL,
        content_score INTEGER DEFAULT 0
    )
    ''')
    
    # 2. User Inventory
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER NOT NULL,
        read_status TEXT DEFAULT 'Unread',
        shelf_status TEXT DEFAULT 'Shelved',
        on_deck_order INTEGER,  -- NEW: For sorting the dashboard
        effective_user_rating REAL, 
        is_owned BOOLEAN DEFAULT 0,
        formats_owned TEXT,
        inventory_notes TEXT,
        acquired_source TEXT,
        acquired_date DATE,      
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(book_id) REFERENCES books(id)
    )
    ''')

    # --- MIGRATION: Ensure on_deck_order exists for old databases ---
    try:
        cursor.execute("ALTER TABLE user_books ADD COLUMN on_deck_order INTEGER")
        print("✅ Added 'on_deck_order' column to database.")
    except sqlite3.OperationalError:
        # Column likely already exists, ignore
        pass
    
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
        session_rating REAL,
        FOREIGN KEY(user_book_id) REFERENCES user_books(id)
    )
    ''')

    # 4. Tags Definition
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    )
    ''')

    # 5. Book-Tag Links
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS book_tags (
        book_id INTEGER,
        tag_id INTEGER,
        PRIMARY KEY (book_id, tag_id),
        FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE,
        FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
    )
    ''')

    # 6. Book Relations
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS book_relations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_book_id INTEGER NOT NULL,
        target_book_id INTEGER NOT NULL,
        relation_type TEXT NOT NULL,
        FOREIGN KEY(source_book_id) REFERENCES books(id) ON DELETE CASCADE,
        FOREIGN KEY(target_book_id) REFERENCES books(id) ON DELETE CASCADE,
        UNIQUE(source_book_id, target_book_id, relation_type)
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