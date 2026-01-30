import sqlite3
import os

# Adjust path if your DB is located elsewhere
DB_PATH = os.path.join("data", "library.db")

def add_column_if_missing(cursor, table, column, definition):
    """
    Attempts to add a column to a table.
    Catches the error if the column already exists.
    """
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        print(f"✅ Added column: {column}")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print(f"ℹ️  Column already exists: {column}")
        else:
            print(f"❌ Error adding {column}: {e}")

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print(f"--- Migrating Database: {DB_PATH} ---")
    
    # 1. Extended Metadata Fields (Manual Add Support)
    add_column_if_missing(cursor, "books", "subtitle", "TEXT")
    add_column_if_missing(cursor, "books", "publisher", "TEXT")
    add_column_if_missing(cursor, "books", "series_name", "TEXT")
    add_column_if_missing(cursor, "books", "series_index", "REAL")
    add_column_if_missing(cursor, "books", "language", "TEXT DEFAULT 'en'")
    add_column_if_missing(cursor, "books", "goodreads_id", "TEXT")
    
    # 2. Quality Score (Search Sort)
    add_column_if_missing(cursor, "books", "content_score", "INTEGER DEFAULT 0")

    # 3. Audio Length (NEW)
    add_column_if_missing(cursor, "books", "total_audio_minutes", "INTEGER DEFAULT 0")

    conn.commit()
    conn.close()
    print("--- Migration Complete ---")

if __name__ == "__main__":
    migrate()