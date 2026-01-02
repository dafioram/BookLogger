import httpx
import asyncio
import difflib

# --- HELPER FUNCTIONS ---
def is_isbn(query):
    """Checks if the query looks like an ISBN (10 or 13 digits)."""
    if not query: return False
    clean = query.replace("-", "").replace(" ", "")
    return clean.isdigit() and len(clean) in [10, 13]

def calculate_score(book, search_query):
    """
    Assigns a quality score (0-100) based on metadata completeness,
    PLUS a relevance score based on the search query.
    """
    score = 0
    query_lower = search_query.lower().strip()
    title_lower = book.get('title', '').lower()
    author_lower = book.get('author', '').lower()

    # --- 1. METADATA QUALITY (Max 100) ---
    # Visuals
    if book.get('cover') and "placeholder" not in book['cover']:
        score += 40
    # Context
    if book.get('summary'):
        score += 30
    # Basics
    if book.get('author') and book['author'] != "Unknown":
        score += 10
    if book.get('year'):
        score += 10
    if book.get('isbn'):
        score += 10

    # --- 2. RELEVANCE BOOST (The "Exact Match" Fix) ---
    
    # EXACT Title Match (Highest Priority)
    if title_lower == query_lower:
        score += 100  # Massive boost ensures it tops the list
        
    # STARTS WITH Title (High Priority)
    elif title_lower.startswith(query_lower):
        score += 50
        
    # CONTAINS Title (Medium Priority)
    elif query_lower in title_lower:
        score += 20
        
    # EXACT Author Match
    if author_lower == query_lower:
        score += 60 # Authors search for themselves often
        
    return score

# --- SEARCH FUNCTIONS ---
async def search_google(client, query):
    results = []
    
    if is_isbn(query):
        clean_isbn = query.replace("-", "").replace(" ", "")
        strategies = [f"isbn:{clean_isbn}"]
    else:
        strategies = [
            query,              # Broad
            f"intitle:{query}"  # Targeted
        ]

    tasks = []
    for strat in strategies:
        url = f"https://www.googleapis.com/books/v1/volumes?q={strat}&maxResults=30"
        tasks.append(client.get(url))
        
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    seen_ids = set()
    
    for resp in responses:
        if isinstance(resp, Exception) or resp.status_code != 200: 
            continue
            
        data = resp.json()
        if "items" not in data: 
            continue
            
        for item in data["items"]:
            if item["id"] in seen_ids:
                continue
            seen_ids.add(item["id"])
            
            vol = item.get("volumeInfo", {})
            
            raw_cover = vol.get("imageLinks", {}).get("thumbnail", "")
            cover = raw_cover.replace("http://", "https://").replace("&edge=curl", "").replace("zoom=1", "zoom=0")
            if not cover: cover = "/static/placeholder.png"

            isbn = None
            for ident in vol.get("industryIdentifiers", []):
                if ident["type"] == "ISBN_13":
                    isbn = ident["identifier"]
            
            rating = vol.get("averageRating", 0)

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
                "rating": rating,
                "isbn": isbn,
                "olid": None
            }
            # PASS QUERY TO SCORE
            book['score'] = calculate_score(book, query)
            results.append(book)

    return results

async def search_open_library(client, query):
    results = []
    try:
        resp = await client.get(f"https://openlibrary.org/search.json?q={query}&limit=30")
        if resp.status_code != 200: return []
        data = resp.json()
        
        if "docs" in data:
            for item in data["docs"]:
                cover_id = item.get("cover_i")
                cover = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else "/static/placeholder.png"

                isbn_list = item.get("isbn", [])
                isbn = isbn_list[0] if isbn_list else None

                author_list = item.get("author_name", ["Unknown"])
                author = ", ".join(author_list[:2])

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
                # PASS QUERY TO SCORE
                book['score'] = calculate_score(book, query)
                results.append(book)
    except Exception as e:
        print(f"OpenLibrary Search Error: {e}")
        
    return results

# --- AGGREGATOR ---
async def search_aggregated(query):
    async with httpx.AsyncClient() as client:
        # 1. Run both searches in parallel
        # We still want to search both because sometimes Google misses a book 
        # that Open Library finds (and vice versa).
        google_task = search_google(client, query)
        ol_task = search_open_library(client, query)
        
        g_results, ol_results = await asyncio.gather(google_task, ol_task)
        
    # 2. NO MERGING
    # We simply combine the lists. 
    # This means you might see duplicate titles in the UI (one from Google, one from OL),
    # but that is actually transparent and honest—you get to pick the source you prefer.
    combined_list = g_results + ol_results
    
    # 3. SORT BY SCORE (The "Smart" part)
    # We still rely on your scoring logic (Exact Title Match = +100 points).
    # This ensures the most relevant book hits the top, regardless of which API found it.
    combined_list.sort(key=lambda x: x['score'], reverse=True)
    
    return combined_list