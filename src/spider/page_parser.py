import asyncio
import time
import urllib
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
from concurrent.futures import ThreadPoolExecutor

from selectolax.lexbor import LexborHTMLParser

from db import db_info
from util import unique_queue, silent_log

#TODO: REFACTOR IN A CLASS???

executor = ThreadPoolExecutor()

class page_info:
    def __init__(self, url, content):
        self.url = url
        self.content = content

    def __repr__(self):
        return f"{self.url} with a content character length of {len(self.content)}"


async def parse_worker(parse_queue, link_queue, db_queue):
    while True:
        try:
            page_info = await parse_queue.get()
            await add_page(page_info, parse_queue, link_queue, db_queue)
        except asyncio.CancelledError:
            break


async def run_parser(page_info):
    run_loop = asyncio.get_running_loop()
    return await run_loop.run_in_executor(executor, parse_page, page_info.content, page_info.url)
    

async def add_page(page_info, parse_queue, link_queue, db_queue):
    try:
        text, outlinks = await run_parser(page_info)
    
        if not text:
            return 
        
        for link in outlinks:
            link_queue.put(link)
        
        url = page_info.url.replace('\x00', '')
        url = clean_link(url)
        text = text.replace('\x00', '')

        await db_queue.put(db_info(page_info.url, text, outlinks))

    except Exception as e:
        silent_log(e, "add_page")

    finally: 
        parse_queue.task_done()    


def parse_page(content, base_url):
    tree = LexborHTMLParser(content)

    if not tree:
        return (None, None)

    tree.strip_tags(['style', 'script'])
    
    outlinks = []
    for node in tree.css("a"):
        link = node.attributes.get("href")
        if not link:
            continue

        link = link.rstrip("/")

        if link.endswith((".jpg", ".png", ".pdf", ".css", ".js", ".zip", ".exe")):
            continue
    
        if "mailto@" in link or "mailto:" in link or "tel:" in link:
            continue

        if "#" in link:
            link = link.split("#")[0]
    
        if "https://" in link:
            outlinks.append(link)
        else:
            link = urljoin(base_url, link)
            outlinks.append(link)

    text = tree.text(strip=True)
    return (text, outlinks)
    

def clean_link(link):
    #removes tracking parameters 

    blocked_params = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', "e",
                    'ref', "source", "ref_source", '_hsfp', '_hssc', '_hstc', 'gclid', 'fbclid']
    parsed_link = urlparse(link)
    query_params = parse_qs(parsed_link.query)
    query_params =  {key: value for key, value in query_params.items() if key.lower() not in blocked_params}
    new_query = urlencode(query_params, doseq=True)
    cleaned_link = urlunparse(parsed_link._replace(query=new_query))
    
    return cleaned_link
