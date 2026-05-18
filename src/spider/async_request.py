import asyncio
import aiohttp
from time import perf_counter

async def fetch(session: aiohttp.ClientSession, url: str, headers: dict, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            t = perf_counter()
            async with session.get(url, allow_redirects=True, headers=headers) as response:
                return {
                    "url" : url,
                    "ok" :  response.ok,
                    "status" : response.status,
                    "headers" : response.headers,
                    "text" : await response.text(encoding='utf-8', errors='replace'),
                    "received" : True
                }

        except asyncio.TimeoutError as e:
            return {"url" : url, "received" : False}
        
        except Exception as e:
            return {"url" : url, "received" : False}

    