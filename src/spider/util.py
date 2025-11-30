import asyncio
import time
from collections import deque
import itertools
from functools import cache
import traceback

import janus
import tldextract


@cache
def to_domain(link: str):
    parse = tldextract.extract(link)

    if "https" in link:
        domain = "https://" + parse.fqdn
    else: #i really dont care about a website that isnt https or http
        domain = "http://" + parse.fqdn

    return domain


@cache
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
    
    def length(self):
        return self.queue._qsize()

    def empty(self) -> bool:
        return self.queue.isEmpty()


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


    #TODO: CLEAN THIS!!!
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


class log_info:
    def __init__(self):
        self.crawled = 0
        self.added_to_db = 0
        self.last_visisted = ""
        self.last_added = ""
    

    def display(self) -> None:
        txt = f"{self.crawled} crawled\t|\t{self.added_to_db} added to database{" " * 20}\nLast visited website: {self.last_visisted}{" " * 75} \nLast added to databse: {self.last_added}{" " * 75}"
        print(txt)
       # print("\033[H", txt)


    def inc(self, crawl: bool = False, added: bool = False) -> None:
        if not((crawl or added) and not(crawl and added)):
            print("Supply a valid increment!")
            return None
        
        if crawl:
            self.crawled += 1
        else: 
            self.added_to_db += 1

        #self.display()
    

    def update(self, visited = False, added = False) -> None:
        if not((visited or added) and not(visited and added)):
            print("Supply a valid update!")
            return None
        
        if visited:
            self.last_visisted = visited
        else:
            self.last_added = added


def silent_log(e, function_name="A function", other_info: list = []):
    tb = traceback.extract_tb(e.__traceback__)
    trace = ""

    for frame in tb:
        trace = f"{trace} {frame.filename} {frame.name} {frame.lineno} |"
    
    with open("log.txt", "a") as f:
        try:
            f.write(f"\n{function_name} threw an error: {str(e)}")
            f.write(f"\nTraceback: {trace}\n")
            for item in other_info:
                f.write(item + " ")
            f.write("\n\n")

        except Exception as e: #invalid character while writing error
            pass

    return None