import time
from collections import deque   
import asyncio
import tldextract
from functools import lru_cache

@lru_cache(maxsize=5000)
def to_domain(link: str):
    parse = tldextract.extract(link)
    domain = parse.fqdn
    return domain

class unique_queue:
    def __init__(self):
        self.queue = asyncio.Queue(maxsize=25000)
        self.shuffle_queue = deque(maxlen=25000)
        self.seen_pages = set()
    
    def put(self, item: str) -> None:
        if item not in self.seen_pages:
            self.seen_pages.add(item)
            self.shuffle_queue.append(item)
            

    async def shuffle(self):
        t = time.perf_counter()
        self.shuffle_queue.extend(self.queue._queue)
        self.queue._queue = deque()

        domains = set()
        domain_pages = {}

        domain_pages = {}
        for link in self.shuffle_queue:
            domain = to_domain(link)
            domain_pages.setdefault(domain, deque()).append(link)

        
        remaining_domains = list(domain_pages.keys())
        while remaining_domains:
            temp = []
            for domain in remaining_domains:
                queue = domain_pages[domain]

                if queue:
                    await self.queue.put(queue.popleft())
                
                if queue:
                    temp.append(domain)
            remaining_domains = temp        
        self.shuffle_queue.clear()
