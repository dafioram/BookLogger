import asyncio

import httpx
import pytest

from app import metadata


def capture_queries(search_fn, query):
    """Runs a search against a fake transport and returns the q= values sent."""
    sent = []

    def handler(request):
        sent.append(request.url.params["q"])
        return httpx.Response(200, json={})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await search_fn(client, query)

    asyncio.run(run())
    return sent


@pytest.mark.parametrize("query", ["Pride & Prejudice", "C# in Depth", "100% Wrong?"])
def test_google_query_is_encoded(query):
    assert capture_queries(metadata.search_google, query) == [query, f"intitle:{query}"]


@pytest.mark.parametrize("query", ["Pride & Prejudice", "C# in Depth", "100% Wrong?"])
def test_open_library_query_is_encoded(query):
    assert capture_queries(metadata.search_open_library, query) == [query]
