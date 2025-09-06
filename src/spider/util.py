import asyncio
import time
from collections import deque
import itertools
from functools import lru_cache

import janus
import tldextract


@lru_cache(maxsize=10000)
def to_domain(link: str):
    parse = tldextract.extract(link)
    domain = "https://" + parse.fqdn
    return domain


@lru_cache(maxsize=100000)
def to_top_domain(link: str):
    parse = tldextract.extract(link)
    domain = parse.top_domain_under_public_suffix
    return domain


class queue:
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
            print("Shuffling")
            self.shuffle_queue.extend(self.queue._queue)
            self.queue._queue = deque()

            size = min(len(self.shuffle_queue), 10000)
            temp_queue = deque(itertools.islice(self.shuffle_queue, 0, size))
            leftover = deque(itertools.islice(self.shuffle_queue, size, len(self.shuffle_queue)))

            domains = set()
            domain_pages = {}

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


class lock:
    def __init__(self):
        self._condition = asyncio.Condition() #true if locked, false if not
        self._locked = False

    @property
    def locked(self):
        return self._locked


    async def lock(self):
        async with self._condition:
            while self._locked:
                await self._condition.wait()
            self._locked = True


    async def unlock(self):
        async with self._condition:
            self._locked = False
            self._condition.notify_all()


    async def wait_for_unlock(self):
        await self.lock()


    async def __aenter__(self):
        await self.lock()


    async def __aexit__(self, exc_type, exc, tb):
        await self.unlock()
