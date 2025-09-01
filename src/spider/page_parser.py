import urllib
import asyncio
import time

from util import unique_queue
from db import db_info

from selectolax.lexbor import LexborHTMLParser
from concurrent.futures import ProcessPoolExecutor


class page_info:
    def __init__(self, url, content):
        self.url = url
        self.content = content

    def __repr__(self):
        return self.url


def parse_page(content, base_url):
    tree = LexborHTMLParser(content)

    if not tree:
        return [False, None, None]
    
    outlinks = []
    for node in tree.css("a"):
        link = node.attributes.get("href")
        if not link:
            continue

        link = link.rstrip("/")

        if link.endswith((".jpg", ".png", ".pdf", ".css", ".js", ".zip", ".exe")):
            continue
    
        if "mailto@" in link or "mailto:" in link:
            continue

        if "#" in link:
            link = link.split("#")[0]
    
        if "https://" in link:
            outlinks.append(link)
        else:
            link = urllib.parse.urljoin(base_url, link)
            outlinks.append(link)

    text = tree.text(strip=True)
    return [True, text, outlinks]


async def add_page(page_info, parse_queue, link_queue, db_queue, executor):
    run_loop = asyncio.get_running_loop()

    start = time.perf_counter()
    try:
        sucess, text, outlinks = await run_loop.run_in_executor(executor, parse_page, page_info.content, page_info.url)
    
        if not sucess or not text:
            return 
        
        for link in outlinks:
            link_queue.put(link)

        await db_queue.put(db_info(page_info.url, text, outlinks))

    except Exception as e:
        print(f"Exception in add pages {e}")
        with open("log.txt", "a") as f:
            f.write(f"add_page threw an error: {e}")

    finally: 
        parse_queue.task_done()    


async def parse_worker(parse_queue, link_queue, db_queue):
    with ProcessPoolExecutor() as executor:
        while True:
            try:
                info = await parse_queue.get()
                await add_page(info, parse_queue, link_queue, db_queue, executor)
            except asyncio.CancelledError:
                break
        