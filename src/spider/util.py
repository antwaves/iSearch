import urllib
from collections import deque

class unique_queue:
    def __init__(self):
        self.queue = deque()
        self.seen = set()
    
    def append(self, item: str) -> None:
        if item not in self.seen:
            self.queue.append(item)
            self.seen.add(item)
    
    def popleft(self) -> None:
        if self.queue[0]:
            self.queue.popleft()
    
    def __getitem__(self, key):
        return self.queue[key]


def to_domain(link: str):
    parseResult = urllib.parse.urlparse(link)
    if parseResult.netloc.startswith("www."):
        domainName = parseResult.netloc.split("www.")[1]
    else:
        domainName = parseResult.netloc
    domainUrl = parseResult.scheme + "://" + domainName

    return domainUrl