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
        return self.queue.qsize()


class jqueue:
    def __init__(self):
        self.queue = janus.Queue(maxsize=500) #i have no idea why im using janus
    
    async def put(self, item) -> None:
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

    def __contains__(self, item):
        return item in self.queue


class sized_set:
    def __init__(self, max_size):
        self.max_size = max_size
        self.set = set()
        self.queue = deque()
    
    
    def add(self, item):
        if item in self.set: return
        self.set.add(item)
        self.queue.append(item)

        if len(self.queue) > self.max_size:
            item = self.queue.popleft()
            self.set.remove(item)


    def remove(self, item):
        self.queue.remove(item)
        self.set.remove(item)
    

    def __repr__(self):
        return self.set.__repr__()

    
    def __contains__(self, item):
        return item in self.set


#TODO move into webcrawler
class unique_queue:
    def __init__(self):
        self.queue = asyncio.Queue(maxsize=5000)
        self.shuffle_queue = deque(maxlen=5000)
        self.seen_pages = sized_set(25000)
    

    def put(self, item) -> None: # assume item is iterable
        dedupe = []
        for obj in item:
            if obj not in self.seen_pages:
                dedupe.append(obj)

        if dedupe:
            self.shuffle_queue.append(tuple(dedupe))
    

    async def get(self) -> None:
        return await self.queue.get()


    def task_done(self) -> None:
        if self.queue._unfinished_tasks > 0:
            self.queue.task_done()


    def empty(self) -> bool:
        return self.queue.empty()
    
    
    def length(self) -> int:
        return len(self.queue._queue)

    # this is super messy but idc
    async def shuffle(self, domain_distance: int, batch_size: int = 10) -> None:
        self.shuffle_queue.extend(self.queue._queue)
        self.queue._queue = deque()

        size = min(len(self.shuffle_queue), 15000)
        temp_queue = deque(itertools.islice(self.shuffle_queue, 0, size))
        leftover = deque(itertools.islice(self.shuffle_queue, size, len(self.shuffle_queue)))

        domains = set()
        domain_pages = {}
        
        for link_batch in temp_queue:
            for link in link_batch:
                domain = to_top_domain(link)
                domain_pages.setdefault(domain, []).append(link)

        for key in domain_pages.keys():
            domain_pages[key] = deque(set(domain_pages[key]))
        remaining_domains = sorted(set(domain_pages.keys()), key=lambda x: len(domain_pages[x]), reverse=True)
       
        if len(remaining_domains) > 2: 
            exit_amount = 2  
        else:
            exit_amount = 0 # edge case, usually at the start of program execution

        added = 0
        batch = []
        while len(remaining_domains) > exit_amount:
            temp = []

            for domain in remaining_domains:
                queue = domain_pages[domain]

                if queue:
                    batch.append(queue.pop())
                    if len(batch) >= batch_size:
                        await self.queue.put(batch)
                        batch = []
                    added += 1
                
                if queue:
                    temp.append(domain)

                if added >= domain_distance: 
                    added = 0
                    break

            remaining_domains = temp     
        self.shuffle_queue = leftover
        if batch:
            await self.queue.put(batch)
