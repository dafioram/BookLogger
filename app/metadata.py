import httpx
import asyncio
import difflib
import unicodedata

# --- CONFIGURATION ---
PREFER_OPEN_LIBRARY_COVERS = True 
MATCH_THRESHOLD = 40  # Minimum 'Match Score' required to even be considered

# --- HELPER FUNCTIONS ---
def is_isbn(query):
    if not query: return False
    clean = query.replace("-", "").replace(" ", "")
    return clean.isdigit() and len(clean) in [10, 13]

def normalize_text(text):
    """
    Robust text cleaner. 
    1. Normalizes unicode characters (turns 'â€”' into simple text or removes it).
    2. Removes punctuation but preserves spaces.
    3. Collapses whitespace.
    """
    if not text: return ""
    
    # unicode normalization (NFKD decomposes characters, e.g., é -> e + acute)
    # .encode('ascii', 'ignore') throws away the non-ascii trash like 'â'
    clean_ascii = unicodedata.normalize('NFKD', str(text)).encode('ascii', 'ignore').decode('utf-8')
    
    # Keep alphanumeric and spaces, drop everything else
    cleaned = "".join(c for c in clean_ascii.lower() if c.isalnum() or c.isspace())
    
    # Collapse multiple spaces into one
    return " ".join(cleaned.split())

def calculate_match_score(book, query, match_isbn=None):
    """
    PHASE 1: IDENTITY
    How well does this result match what the user asked for?
    Returns 0-100.
    """
    score = 0
    q_norm = normalize_text(query)
    t_norm = normalize_text(book.get('title', ''))
    a_norm = normalize_text(book.get('author', ''))
    
    # --- 1. THE "CHEAT CODE" (External ISBN Verification) ---
    # If the caller passed an ISBN from their CSV, verify it immediately.
    if match_isbn:
        clean_target = str(match_isbn).replace("-", "").replace(" ", "")
        book_isbn = str(book.get('isbn', '')).replace("-", "").replace(" ", "")
        # If the CSV ISBN matches the API Result ISBN, it's a 100% match.
        if clean_target and clean_target == book_isbn:
            return 100

    # --- 2. QUERY IS ISBN (The User searched by ISBN directly) ---
    if is_isbn(query):
        clean_q = query.replace("-", "").replace(" ", "")
        book_isbn = str(book.get('isbn', '')).replace("-", "").replace(" ", "")
        if clean_q == book_isbn:
            return 100 
            
    # --- 3. TITLE MATCHING ---
    if t_norm == q_norm:
        score += 60  # Exact Title Match
        
    # BIDIRECTIONAL SUBTITLE MATCH
    # Case A: Query is short ("Dune"), Result is long ("Dune: Messiah")
    elif t_norm.startswith(q_norm + " "):
        score += 55 
    # Case B: Query is long ("How Not To Invest: The Ideas..."), Result is short ("How Not To Invest")
    elif q_norm.startswith(t_norm + " "):
        score += 55
        
    elif t_norm.startswith(q_norm) or q_norm.startswith(t_norm):
        score += 40  # Strong Partial Match
        
    elif q_norm in t_norm:
        score += 20  # Weak Partial Match
        
    # --- 4. AUTHOR MATCHING ---
    if a_norm and a_norm in q_norm:
        score += 30
    elif a_norm and q_norm in a_norm:
        score += 30

    return min(score, 100)

def calculate_content_score(book):
    """
    PHASE 2: QUALITY
    How rich/complete is this record?
    Returns 0-100 (can go negative internally, clipped at 0).
    """
    score = 0
    
    # 1. Visuals
    if book.get('cover') and "placeholder" not in book['cover']:
        score += 30
    
    # 2. Context
    summary = book.get('summary', '')
    if summary and len(summary) > 50:
        score += 30
    elif not summary:
        score -= 10
        
    # 3. Metadata Basics
    author = book.get('author', 'Unknown')
    title = book.get('title', '')
    
    clean_title = normalize_text(title)
    clean_author = normalize_text(author)

    if len(clean_author) > 3 and (clean_author in clean_title or clean_title in clean_author):
        score -= 40
    elif author != "Unknown":
        score += 10

    if book.get('year'):
        score += 10
        
    # 4. PAGE COUNT REALISM (UPDATED)
    pages = book.get('pages', 0)
    
    if pages == 0:
        score -= 50  # <--- NUCLEAR OPTION: Kill books with 0 pages
    elif pages < 50:
        score -= 15  # Pamphlet penalty
    else:
        score += 20  # <--- BOOST: Valid length books get a hefty bonus
    
    if book.get('isbn'):
        score += 5
        
    return max(0, min(score, 100))

# --- SEARCH FUNCTIONS ---
async def search_google(client, query, match_isbn=None):
    results = []
    
    if is_isbn(query):
        clean_isbn = query.replace("-", "").replace(" ", "")
        strategies = [f"isbn:{clean_isbn}"]
    else:
        strategies = [query, f"intitle:{query}"]

    tasks = [client.get(f"https://www.googleapis.com/books/v1/volumes?q={s}&maxResults=20") for s in strategies]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    seen_ids = set()
    
    for resp in responses:
        if isinstance(resp, Exception) or resp.status_code != 200: continue
        data = resp.json()
        if "items" not in data: continue
            
        for item in data["items"]:
            if item["id"] in seen_ids: continue
            seen_ids.add(item["id"])
            
            vol = item.get("volumeInfo", {})
            # NEW CODE: Fallback to smallThumbnail if thumbnail is missing
            image_links = vol.get("imageLinks", {})
            raw_cover = image_links.get("thumbnail") or image_links.get("smallThumbnail", "")
            cover = raw_cover.replace("http://", "https://").replace("&edge=curl", "").replace("zoom=1", "zoom=0")
            if not cover: cover = "/static/placeholder.png"

            isbn = None
            for ident in vol.get("industryIdentifiers", []):
                if ident["type"] == "ISBN_13": isbn = ident["identifier"]
            
            book = {
                "source": "Google",
                "source_id": item["id"],
                "title": vol.get("title", "Unknown"),
                "author": ", ".join(vol.get("authors", ["Unknown"])),
                "year": vol.get("publishedDate", "")[:4],
                "cover": cover,
                "pages": vol.get("pageCount", 0),
                "summary": vol.get("description", ""),
                "genres": ", ".join(vol.get("categories", [])),
                "rating": vol.get("averageRating", 0),
                "isbn": isbn,
                "olid": None
            }
            
            # --- CALCULATE SCORES ---
            raw_match = calculate_match_score(book, query, match_isbn)
            raw_content = calculate_content_score(book)
            
            final_score = raw_match + raw_content
            
            # --- UI OUTPUTS ---
            book['match_score'] = int(raw_match)
            book['content_score'] = int(raw_content)
            book['rank_score'] = int(final_score)
            book['score'] = final_score
            
            results.append(book)
    return results

async def search_open_library(client, query, match_isbn=None):
    results = []
    try:
        resp = await client.get(f"https://openlibrary.org/search.json?q={query}&limit=20")
        if resp.status_code != 200: return []
        data = resp.json()
        
        if "docs" in data:
            for item in data["docs"]:
                cover_id = item.get("cover_i")
                cover = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else "/static/placeholder.png"

                isbn = item.get("isbn", [None])[0]
                author = ", ".join(item.get("author_name", ["Unknown"])[:2])

                book = {
                    "source": "OpenLibrary",
                    "source_id": item.get("key", "").replace("/works/", ""),
                    "title": item.get("title", "Unknown"),
                    "author": author,
                    "year": str(item.get("first_publish_year", "")),
                    "cover": cover,
                    "pages": item.get("number_of_pages_median", 0),
                    "summary": "", 
                    "genres": ", ".join(item.get("subject", [])[:3]),
                    "rating": 0,
                    "isbn": isbn,
                    "olid": item.get("key", "").replace("/works/", "")
                }
                
                # --- CALCULATE SCORES ---
                raw_match = calculate_match_score(book, query, match_isbn)
                raw_content = calculate_content_score(book)
                
                final_score = raw_match + raw_content
                
                book['match_score'] = int(raw_match)
                book['content_score'] = int(raw_content)
                book['rank_score'] = int(final_score)
                book['score'] = final_score
                
                results.append(book)
    except Exception as e:
        print(f"OpenLibrary Search Error: {e}")
        
    return results

# --- AGGREGATOR ---
async def search_aggregated(query, match_isbn=None):
    async with httpx.AsyncClient() as client:
        # We now pass match_isbn down to the search functions
        google_task = search_google(client, query, match_isbn)
        ol_task = search_open_library(client, query, match_isbn)
        
        g_results, ol_results = await asyncio.gather(google_task, ol_task)
        
    combined = g_results + ol_results
    
    # 1. FILTER
    filtered = [b for b in combined if b['match_score'] >= MATCH_THRESHOLD]
    
    # 2. SORT
    filtered.sort(key=lambda x: x['score'], reverse=True)
    
    return filtered