import urllib
from collections import deque   
import asyncio
import tldextract

def to_domain(link: str):
    parse = tldextract.extract(link)
    return parse.top_domain_under_public_suffix


class unique_queue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.shuffle_queue = deque()
        self.seen_pages = set()
    
    def put(self, item: str) -> None:
        if item not in self.seen_pages:
            self.seen_pages.add(item)
            self.shuffle_queue.append(item)
            

    async def shuffle(self):
        self.shuffle_queue.extend(self.queue._queue)
        self.queue._queue = deque()

        if len(self.shuffle_queue) > 0:
            print("Shuffling")

            current_id = 0
            domains = set()
            domain_ids = {}
            domain_pages = {}

            for link in self.shuffle_queue:
                domain = to_domain(link)

                if domain not in domains:
                    domains.add(domain)
                    domain_ids[domain] = current_id
                    current_id += 1

                domain_pages.setdefault(domain_ids[domain], deque()).append(link)

            keys = list(domain_pages.keys())
            while len(keys) > 0:
                for key in keys:   
                    queue = domain_pages[key]

                    if len(queue) > 0:
                        await self.queue.put(queue[0])
                        queue.popleft()

                keys = [key for key in domain_pages.keys() if len(domain_pages[key]) > 0]
        
            self.shuffle_queue.clear()
