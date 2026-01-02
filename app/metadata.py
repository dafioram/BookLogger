import httpx
import asyncio
import difflib

# --- CONFIGURATION ---
MATCH_THRESHOLD = 40  # Minimum 'Match Score' required to even be considered

# --- HELPER FUNCTIONS ---
def is_isbn(query):
    if not query: return False
    clean = query.replace("-", "").replace(" ", "")
    return clean.isdigit() and len(clean) in [10, 13]

def normalize_text(text):
    """Simple text cleaner for comparisons."""
    if not text: return ""
    return text.lower().strip().replace(":", "").replace("-", "")

def calculate_match_score(book, query):
    """
    PHASE 1: IDENTITY
    How well does this result match what the user asked for?
    Returns 0-100.
    """
    score = 0
    q_norm = normalize_text(query)
    t_norm = normalize_text(book.get('title', ''))
    a_norm = normalize_text(book.get('author', ''))
    
    # 1. ISBN MATCH (The Golden Ticket)
    if is_isbn(query):
        clean_q = query.replace("-", "").replace(" ", "")
        book_isbn = str(book.get('isbn', '')).replace("-", "")
        if clean_q == book_isbn:
            return 100 # Perfect Match
            
    # 2. TITLE MATCHING
    if t_norm == q_norm:
        score += 60  # Exact Title Match
    elif t_norm.startswith(q_norm) or q_norm.startswith(t_norm):
        score += 40  # Strong Partial Match
    elif q_norm in t_norm:
        score += 20  # Weak Partial Match
        
    # 3. AUTHOR MATCHING
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
    
    # 1. Visuals (Heavy Weight)
    if book.get('cover') and "placeholder" not in book['cover']:
        score += 30  # Reduced slightly to make room for penalties
    
    # 2. Context (Heavy Weight)
    summary = book.get('summary', '')
    if summary and len(summary) > 50:
        score += 30
    elif not summary:
        score -= 10  # Penalty for being a "ghost" record
        
    # 3. Metadata Basics (Medium Weight)
    author = book.get('author', 'Unknown')
    title = book.get('title', '')
    
    # --- UPDATED "IDEA FACTORY" FIX ---
    clean_title = title.lower().strip()
    clean_author = author.lower().strip()

    # Check if author is inside title OR title is inside author (Fuzzy Echo)
    if len(clean_author) > 3 and (clean_author in clean_title or clean_title in clean_author):
        score -= 40  # Massive penalty for bad data (e.g. Author="Idea Factory")
    elif author != "Unknown":
        score += 10

    if book.get('year'):
        score += 10
        
    # 4. Page Count Realism
    pages = book.get('pages', 0)
    if pages > 0:
        if pages < 50:
            score -= 15  # Penalty: Likely a pamphlet/metadata fragment
        else:
            score += 5   # Reward: Standard book length
    
    if book.get('isbn'):
        score += 5
        
    return max(0, min(score, 100))

# --- SEARCH FUNCTIONS ---
async def search_google(client, query):
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
            raw_cover = vol.get("imageLinks", {}).get("thumbnail", "")
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
            raw_match = calculate_match_score(book, query)
            raw_content = calculate_content_score(book)
            
            book['match_score'] = raw_match
            book['content_score'] = raw_content
            
            # Weighted Score: Content has 50% voting power of Match
            # Allows high quality to beat exact text matches
            book['score'] = raw_match + (raw_content / 2)

            # Debug Info (for frontend display)
            book['debug'] = {
                'match': raw_match,
                'content': raw_content,
                'total': book['score']
            }
            
            results.append(book)
    return results

async def search_open_library(client, query):
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
                raw_match = calculate_match_score(book, query)
                raw_content = calculate_content_score(book)
                
                book['match_score'] = raw_match
                book['content_score'] = raw_content
                
                # Consistent weighting with Google
                book['score'] = raw_match + (raw_content / 2)

                book['debug'] = {
                    'match': raw_match,
                    'content': raw_content,
                    'total': book['score']
                }
                
                results.append(book)
    except Exception as e:
        print(f"OpenLibrary Search Error: {e}")
        
    return results

# --- AGGREGATOR ---
async def search_aggregated(query):
    async with httpx.AsyncClient() as client:
        google_task = search_google(client, query)
        ol_task = search_open_library(client, query)
        
        g_results, ol_results = await asyncio.gather(google_task, ol_task)
        
    combined = g_results + ol_results
    
    # 1. FILTER: The Gatekeeper
    filtered = [b for b in combined if b['match_score'] >= MATCH_THRESHOLD]
    
    # 2. SORT: The Ranker
    # CRITICAL FIX: Sort by the calculated 'score', NOT the tuple.
    # The tuple sort (match, content) ignored our weighting logic!
    filtered.sort(key=lambda x: x['score'], reverse=True)
    
    return filtered