import math
import json
import urllib.parse

# --- CONSTANTS ---
# The Map for Relationship Logic
RELATION_MAP = {
    # Symmetric (Bidirectional)
    "reads_like":       ("Reads Like", "Reads Like"),
    "contrast":         ("Contrast", "Contrast"),
    "complementary":    ("Complementary", "Complementary"),
    "same_universe":    ("Same Universe", "Same Universe"),
    "shared_trope":     ("Shared Trope", "Shared Trope"),
    
    # Asymmetric (Directional)
    "prequel_to":       ("Prequel To", "Sequel"),
    "sequel_to":        ("Sequel To", "Prequel"),
    "prerequisite_for": ("Prerequisite For", "Requires"),
    "more_detailed_than": ("More Detailed Than", "Simpler Than"),
    "simpler_than":     ("Simpler Than", "More Detailed Than"),
    "inspired_by":      ("Inspired By", "Inspired This"),
    
    # NEW: Quality Relations
    "better_than":      ("Better Than", "Worse Than"),
    "worse_than":       ("Worse Than", "Better Than")
}

# --- TEMPLATE FILTERS ---
def format_minutes(mins):
    if not mins: return ""
    h = math.floor(mins / 60)
    m = mins % 60
    return f"{h}h {m}m"

def format_runtime(mins):
    """Formats audio length (e.g. 630 -> '10h 30m')"""
    if not mins: return ""
    h = mins // 60
    m = mins % 60
    return f"{h}h {m}m"

# --- DATA PROCESSING HELPERS ---
def process_book_row(row):
    """
    Converts a SQLite Row to a dict and handles logic like:
    - JSON parsing for formats
    - Swapping cover_url for cover_path ONLY if cover_path is valid
    """
    r = dict(row)
    
    # 1. Format Parsing
    if 'formats_owned' in r:
        r['formats'] = json.loads(r['formats_owned']) if r['formats_owned'] else []
    
    # 2. IMAGE LOGIC (Updated Safety Check)
    local_path = r.get('cover_path')
    if local_path and isinstance(local_path, str) and local_path.strip():
        r['cover_url'] = local_path
        
    return r

def recalculate_book_rating(conn, user_book_id):
    """
    Calculates the average of all non-null session_ratings for a book
    and updates the user_books.effective_user_rating column.
    """
    # 1. Get the average of valid ratings
    row = conn.execute("""
        SELECT AVG(session_rating) as avg_val 
        FROM reading_logs 
        WHERE user_book_id = ? AND session_rating IS NOT NULL AND session_rating > 0
    """, (user_book_id,)).fetchone()
    
    new_rating = row['avg_val'] if row['avg_val'] else None
    
    # 2. Update the parent book record
    conn.execute("""
        UPDATE user_books 
        SET effective_user_rating = ? 
        WHERE id = ?
    """, (new_rating, user_book_id))