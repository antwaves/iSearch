from collections import deque
import itertools
from functools import cache

import janus
import asyncio
import tldextract


@cache
def to_top_domain(link: str): #i dont care about DRY **** you!!! i know this is repeated in util.py!
    parse = tldextract.extract(link)
    domain = parse.top_domain_under_public_suffix
    return domain


class queue:
    def __init__(self):
        self.queue = asyncio.Queue()
    
    async def put(self, item) -> None:
        await self.queue.put(item)

    async def get(self):
        return await self.queue.get()

    def task_done(self) -> None:
        if self.queue._unfinished_tasks > 0:
            self.queue.task_done()

    def length(self) -> int:
        return len(self.queue._queue)


class jqueue:
    def __init__(self):
        self.queue = janus.Queue()
    
    async def put(self, item: str) -> None:
        await self.queue.async_q.put(item)

    async def get(self):
        item = await self.queue.async_q.get()
        return item

    def task_done(self) -> None:
        self.queue.async_q.task_done()
    
    async def close(self) -> None:
        await queue.aclose()
    
    def length(self):
        return self.queue._qsize()

    def empty(self) -> bool:
        return self.queue.async_q.empty()


#TODO move into webcrawler
class unique_queue:
    def __init__(self):
        self.queue = asyncio.Queue(maxsize=25000)
        self.shuffle_queue = deque(maxlen=25000)
        self.seen_pages = set()
    

    def put(self, item: str) -> None:
        if item not in self.seen_pages:
            self.seen_pages.add(item)
            self.shuffle_queue.append(item)
    

    async def get(self) -> None:
        return await self.queue.get()


    def task_done(self) -> None:
        if self.queue._unfinished_tasks > 0:
            self.queue.task_done()


    def empty(self) -> bool:
        return self.queue.empty()
    
    
    def length(self) -> int:
        return len(self.queue._queue)


    async def shuffle(self):
        try:
            self.shuffle_queue.extend(self.queue._queue)
            self.queue._queue = deque()

            size = min(len(self.shuffle_queue), 15000)
            temp_queue = deque(itertools.islice(self.shuffle_queue, 0, size))
            leftover = deque(itertools.islice(self.shuffle_queue, size, len(self.shuffle_queue)))

            domains = set()
            domain_pages = {}

            for link in temp_queue:
                domain = to_top_domain(link)
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
            self.shuffle_queue = leftover
        except Exception as e:
            print(e)