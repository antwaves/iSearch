import asyncio

class queue:
    def __init__(self):
        self.queue = asyncio.Queue(maxsize=25000)
    

    async def put(self, item) -> None:
        await self.queue.put(item)


    async def get(self):
        return await self.queue.get()


    def task_done(self) -> None:
        if self.queue._unfinished_tasks > 0:
            self.queue.task_done()
    

    def length(self) -> int:
        return len(self.queue._queue)
