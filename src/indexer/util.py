import asyncio

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

class page_info:
    #contains page id and content

    def __init__(self, id, content):
        self.id = id
        self.content = content

    def __repr__(self):
        return f"page id {self.id}"


class term_data:
    #contains the total occurences of a list of terms, also link said terms back to their orginal pages
    
    def __init__(self):
        self.pages = []
    
    def add(self, page):
        self.pages.append(page)

    def __repr__(self):
        return f"{len(self.pages)} total pages and {len(pages)} total occurring pages"
