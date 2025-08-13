import urllib
import asyncio

class unique_queue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.seen = set()
    
    async def put(self, item: str) -> None:
        if item not in self.seen:
            self.seen.add(item)
            await self.queue.put(item)


def to_domain(link: str):
    parseResult = urllib.parse.urlparse(link)
    if parseResult.netloc.startswith("www."):
        domainName = parseResult.netloc.split("www.")[1]
    else:
        domainName = parseResult.netloc
    domainUrl = parseResult.scheme + "://" + domainName

    return domainUrl