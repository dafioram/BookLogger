import httpx
import asyncio

def calculate_score(book):
    score = 0
    # Visuals are most important for the UI
    if book.get('cover') and "placeholder" not in book['cover']:
        score += 40
    
    # Context is second most important
    if book.get('summary'):
        score += 30
        
    # Metadata basics
    if book.get('author') and book['author'] != "Unknown":
        score += 10
    if book.get('year'):
        score += 10
    if book.get('isbn'):
        score += 10
        
    return score

async def search_google(client, query):
    results = []
    try:
        resp = await client.get(f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=10")
        if resp.status_code != 200: return []
        data = resp.json()
        
        if "items" in data:
            for item in data["items"]:
                vol = item.get("volumeInfo", {})
                
                # Image Logic
                raw_cover = vol.get("imageLinks", {}).get("thumbnail", "")
                cover = raw_cover.replace("http://", "https://").replace("&edge=curl", "").replace("zoom=1", "zoom=0")
                if not cover: cover = "/static/placeholder.png"

                # ISBN Logic
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
                # Calculate Score immediately
                book['score'] = calculate_score(book)
                results.append(book)
                
    except Exception as e:
        print(f"Google Search Error: {e}")
        
    return results

async def search_open_library(client, query):
    results = []
    try:
        resp = await client.get(f"https://openlibrary.org/search.json?q={query}&limit=10")
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
                # Calculate Score
                book['score'] = calculate_score(book)
                results.append(book)
                
    except Exception as e:
        print(f"OpenLibrary Search Error: {e}")
        
    return results

async def search_aggregated(query):
    async with httpx.AsyncClient() as client:
        google_task = search_google(client, query)
        ol_task = search_open_library(client, query)
        
        g_results, ol_results = await asyncio.gather(google_task, ol_task)
        
    combined = g_results + ol_results
    
    # SORTING MAGIC: Sort by 'score' descending (Highest quality first)
    combined.sort(key=lambda x: x['score'], reverse=True)
    
    return combined