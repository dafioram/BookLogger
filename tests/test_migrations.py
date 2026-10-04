import sqlite3

from app import database


def columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def test_init_db_upgrades_old_schema(data_dir):
    data_dir.mkdir()
    old = sqlite3.connect(database.DB_PATH)
    old.executescript("""
        CREATE TABLE books (id INTEGER PRIMARY KEY AUTOINCREMENT, google_id TEXT UNIQUE, title TEXT NOT NULL, author TEXT);
        CREATE TABLE user_books (id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER NOT NULL, read_status TEXT DEFAULT 'Unread');
        INSERT INTO books (google_id, title, author) VALUES ('g1', 'Old Book', 'Old Author');
        INSERT INTO user_books (book_id, read_status) VALUES (1, 'Read');
    """)
    old.commit()
    old.close()

    database.init_db()
    database.init_db()  # idempotent

    conn = database.get_db_connection()
    for table, column, _ in database.COLUMN_MIGRATIONS:
        assert column in columns(conn, table), f"{table}.{column}"
    assert tuple(conn.execute("SELECT title, language, content_score FROM books").fetchone()) == ("Old Book", "en", 0)
    # No readings logged, so the stale 'Read' is corrected
    assert conn.execute("SELECT read_status FROM user_books").fetchone()[0] == "Unread"
    conn.close()


def test_fresh_schema_already_has_migrated_columns(data_dir):
    database.init_db()
    conn = database.get_db_connection()
    for table, column, _ in database.COLUMN_MIGRATIONS:
        assert column in columns(conn, table)
    conn.close()


def test_init_db_removes_orphaned_links(data_dir):
    database.init_db()
    conn = sqlite3.connect(database.DB_PATH)  # foreign keys off, like the old app
    conn.executescript("""
        INSERT INTO books (id, google_id, title) VALUES (1, 'a', 'Alive');
        INSERT INTO tags (id, name) VALUES (1, 'scifi');
        INSERT INTO book_tags VALUES (1, 1), (2, 1);
        INSERT INTO book_relations (source_book_id, target_book_id, relation_type)
            VALUES (1, 2, 'reads_like'), (2, 1, 'reads_like');
    """)
    conn.commit()
    conn.close()

    database.init_db()

    conn = database.get_db_connection()
    assert [tuple(r) for r in conn.execute("SELECT book_id, tag_id FROM book_tags")] == [(1, 1)]
    assert conn.execute("SELECT COUNT(*) FROM book_relations").fetchone()[0] == 0
    assert conn.execute("SELECT name FROM tags").fetchone()[0] == "scifi"  # tags themselves are kept
    conn.close()
