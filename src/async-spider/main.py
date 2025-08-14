import re
import time
import urllib
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from lxml import etree
import httpx
import asyncio

import requests
from bs4 import BeautifulSoup as soup

from util import unique_queue, to_domain
from db import connect_to_db, create_page
from robots import check_robots, robotsTxt
import yappi

class webcrawler:
    def __init__(self, client, workers):
        self.client = client
        self.link_queue = unique_queue()
    
        self.workers = workers
        self.domain_wait_times = {}
        self.domain_robot_rules = {}

        self.crawled = 0
        self.max_crawl = 1000

        self.shuffle_count = 100

        self.last_crawl = None

        self.db_session = connect_to_db()
        print("Connected to db!")


    async def add_links(self, urls):
        for url in urls:
            self.link_queue.put(url)
        
        await self.link_queue.shuffle()
        

    async def run_crawler(self):
        workers = [asyncio.create_task(self.worker(worker_num)) 
                    for worker_num in range(self.workers)]

        await asyncio.gather(*workers)


    async def worker(self, worker_num):
        while True:
            if self.crawled <= self.max_crawl:
                try:
                    if self.shuffle_count == 0 or self.link_queue.queue.empty():
                        self.shuffle_count = 100
                        await self.link_queue.shuffle()

                    self.shuffle_count -= 1
                    await self.get_page(worker_num)

                except asyncio.CancelledError:
                    break
                except asyncio.queues.QueueShutDown:
                    break
            else:
                self.link_queue.queue.shutdown(immediate=True)
                print(f"Worker number {worker_num} exited.")
                break
        

    async def get_page(self, worker_num):
        url = await self.link_queue.queue.get()

        robot_rules = await check_robots(self.client, url, self.domain_robot_rules)
        
        if robot_rules and (not robot_rules.parser.can_fetch("*", url)):
            self.link_queue.queue.task_done()
            return

        domain = to_domain(url)

        if domain in self.domain_wait_times.keys():
            now = datetime.now(timezone.utc)
            sleep_time = (self.domain_wait_times[domain] - now)
            sleep_seconds = sleep_time.total_seconds()

            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

        try:
            headers = {'User-Agent': 'iSearch'}
            now = datetime.now(timezone.utc)
            response = await self.client.get(url, follow_redirects=True, headers=headers)
            self.handle_limits(response, url, robot_rules)
        
            sucess_color, fail_color, reset_foreground = "\033[32m", "\033[31m", "\033[0m"
            if response.status_code == httpx.codes.OK:
                print(f"{sucess_color}Worker number {worker_num} grabbed {url} with status code {response.status_code} at {now}{reset_foreground}")
            else:
                print(f"{fail_color}Worker number {worker_num} failed to grab {url} with response code {response.status_code} at {now}{reset_foreground}")
            
            await self.add_pages(response, url)
        except Exception as e:
            print("Exception in get page:", e, url)


        self.crawled += 1
        self.link_queue.queue.task_done()


    async def add_pages(self, response, url):
        content = soup(response.content, "lxml")     

        if not content:
            return 
        
        outlinks = []
        for link in content.find_all('a', href=True):
            link = link["href"] 
            if not link:
                continue

            if link[-1] == "/":
                link = link[:-1]
            
            if "mailto@" in link or "mailto:" in link:
                continue
            
            if "https://" in link:
                outlinks.append(link)
                self.link_queue.put(link)
            else:
                link = urllib.parse.urljoin(url, link)
                outlinks.append(link)
                self.link_queue.put(link)
        
    
        
        text = content.get_text()
        create_page(self.db_session, url, text, outlinks)


    def handle_limits(self, response, url, robot_rules):
        can_request_at = None

        if response.status_code == 429 or response.status_code == 503:
            if response.headers:
                retry_after = response.headers["Retry-After"]

                if retry_after:
                    if retry_after.isdigit():
                        time_delta = timedelta(seconds=int(retry_after))
                        can_request_at = datetime.now(timezone.utc) + time_delta
                    else:
                        try:
                            can_request_at = parsedate_to_datetime(retry_after)
                        except (TypeError, ValueError):
                            can_request_at = datetime.now(timezone.utc) + timedelta(seconds=5)
                else:
                    can_request_at = datetime.now(timezone.utc) + timedelta(seconds=5)
            else:
                    can_request_at = datetime.now(timezone.utc) + timedelta(seconds=5)

        elif robot_rules and (robot_rules.crawl_delay or robot_rules.request_rate):
            crawl_delay = 0 if robot_rules.crawl_delay == None else robot_rules.crawl_delay
            request_rate = 0 if robot_rules.request_rate == None else robot_rules.request_rate

            wait_time = 0
            if crawl_delay >= request_rate:
                wait_time = crawl_delay
            else:
                wait_time = request_rate
        
            if wait_time < 0.2:
                wait_time = 0.2

            wait_milliseconds = wait_time * 100
            can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=wait_milliseconds)
        
        else:
            can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=200)

        domain_url = to_domain(url)
        self.domain_wait_times[domain_url] = can_request_at


async def main():
    start_urls = ["https://nodejs.org/en"]

    workers = 15

    start = time.perf_counter()

    async with httpx.AsyncClient() as client:
        crawler = webcrawler(client, workers)
        await crawler.add_links(start_urls)
        await crawler.run_crawler()

    print(time.perf_counter() - start)


if __name__ == '__main__':
    asyncio.run(main())
