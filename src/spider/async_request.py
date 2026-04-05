import asyncio
import aiohttp

async def fetch(session: aiohttp.ClientSession, url: str, headers: dict):
    try:
        async with session.get(url, allow_redirects=True, headers=headers) as response:
            return {
                "url" : url,
                "ok" :  response.ok,
                "status" : response.status,
                "headers" : response.headers,
                "text" : await response.text(encoding='utf-8', errors='replace'),
                "received" : True
            }

    except asyncio.TimeoutError:
        print(f"Request to {url} timed out")
        return {"url" : url, "received" : False}
    
    except Exception as e:
        return {"url" : url, "received" : False}

    