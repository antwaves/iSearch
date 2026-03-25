import asyncio
import time
from functools import cache
import traceback

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


#TODO move into webcrawler
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


#TODO: turn this logging competent
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