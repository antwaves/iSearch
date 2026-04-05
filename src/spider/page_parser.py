import asyncio
import time
import urllib
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
from concurrent.futures import ProcessPoolExecutor

from selectolax.lexbor import LexborHTMLParser

from spider.util import silent_log 

class page_info:
    def __init__(self, url, content):
        self.url = url
        self.content = content

    def __repr__(self):
        return f"{self.url} with a content character length of {len(self.content)}"


class parser:
    def __init__(self, link_queue, parse_queue, db_queue, workers : int):
        self.adding_new_links = True
        self.executor = ProcessPoolExecutor(workers)

        self.link_queue = link_queue
        self.parse_queue = parse_queue
        self.db_queue = db_queue

        self.batch_size = 15


    def still_running(self):
        return self.adding_new_links or not self.parse_queue.empty()

    async def worker(self):
        while self.still_running():      
            page_info = await self.parse_queue.get()
            await self.add_page_to_db(page_info)


    async def add_page_to_db(self, page_info):
        try:
            text, outlinks = await self.run_parse_page(page_info)
        
            if not text:
                return    

            batches = [tuple(outlinks[i : i + self.batch_size]) for i in range(0, len(outlinks), self.batch_size)]  #some bullshit to batch the list
            for batch in batches:
                self.link_queue.put(batch)

            url = page_info.url.replace('\x00', '')
            url = clean_link(url)

            await self.db_queue.put((page_info.url, text, outlinks))

        except Exception as e:
            silent_log(e, "add_page")

        finally: 
            self.parse_queue.task_done()    


    async def run_parse_page(self, page_info):
        run_loop = asyncio.get_running_loop()
        return await run_loop.run_in_executor(self.executor, parse_page, page_info.content, page_info.url, self.adding_new_links)


def parse_page(content, base_url: str, adding_new_links: bool):
    tree = LexborHTMLParser(content)

    if not tree:
        return (None, None)
    
    outlinks = []
    if adding_new_links:
        for node in tree.css("a"):
            link = node.attributes.get("href")
            if not link:
                continue

            link = link.rstrip("/")

            if len(link) >= 750:
                continue

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

    #get text, remove trailing whitespace, including that which is inbetween words and remove other non-essential stuff
    tree.strip_tags(['style', 'script', 'head', 'title', 'meta', '[document]'])
    text = tree.text(separator=' ')
    text = " ".join(text.split())
    text = text.replace('\x00', '')

    return (text, outlinks)


def clean_link(link: str):
    ''' Removes tracking parameters from a given link '''

    blocked_params = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', "e",
                    'ref', "source", "ref_source", '_hsfp', '_hssc', '_hstc', 'gclid', 'fbclid']
    parsed_link = urlparse(link)
    query_params = parse_qs(parsed_link.query)
    query_params =  {key: value for key, value in query_params.items() if key.lower() not in blocked_params}
    new_query = urlencode(query_params, doseq=True)
    cleaned_link = urlunparse(parsed_link._replace(query=new_query))
    
    return cleaned_link
