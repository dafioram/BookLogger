import sqlite3

conn = sqlite3.connect("data/library.db")
cursor = conn.cursor()

# Get list of all columns in the 'books' table
data = cursor.execute("PRAGMA table_info(books)").fetchall()
columns = [row[1] for row in data]

print("--- Current Columns in 'books' ---")
print(columns)

# Quick check for the ones we care about
missing = []
expected = ["subtitle", "series_name", "publisher", "goodreads_id", "content_score"]
for col in expected:
    if col not in columns:
        missing.append(col)

if missing:
    print(f"\n❌ You are missing: {', '.join(missing)}")
    print("👉 Run the update_db_schema.py script I gave you.")
else:
    print("\n✅ All columns are present. You are good to go!")

conn.close()