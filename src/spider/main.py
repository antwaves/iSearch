import re
import time
import urllib
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import aiohttp
import asyncio

import requests
from selectolax.parser import HTMLParser

from util import unique_queue, queue, to_domain
from db import connect_to_db, create_page, db_worker
from robots import check_robots, robotsTxt
from page_parser import page_info, parse_worker

#and cchardet and pip-system-certs

t = time.perf_counter()

class webcrawler:
    def __init__(self, client, workers):
        self.client = client
        self.link_queue = unique_queue()
        self.parse_queue = queue()
        self.db_queue = queue()
    
        self.workers = workers

        self.domain_wait_times = {}
        self.domain_robot_rules = {}

        self.crawled = 0
        self.max_crawl = 500
        self.max_response_size = 5 * 1024 * 1024  

        self.shuffle_count = 100
        self.last_crawl = None

        print("Connected to db!")


    async def add_links(self, urls):
        for url in urls:
            self.link_queue.put(url)
        
        await self.link_queue.shuffle()
        
    
    async def shuffle_handler(self):
        while True:
            try:
                if self.crawled < 2:
                    await asyncio.sleep(0.1)
                    await self.link_queue.shuffle()
                else:
                    await asyncio.sleep(5)
                    await self.link_queue.shuffle()
            except asyncio.CancelledError:
                break


    async def run_crawler(self, client_session):
        workers = [asyncio.create_task(self.worker(client_session, worker_num)) 
                    for worker_num in range(self.workers)]
        
        workers = workers + [asyncio.create_task(db_worker(self.db_queue)) for _ in range(self.workers)]
        
        workers.append(asyncio.create_task(parse_worker(self.parse_queue, self.link_queue, self.db_queue)))
        workers.append(asyncio.create_task(self.shuffle_handler()))

        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            pass


    async def worker(self, session_maker, worker_num):
        async with session_maker() as session:
            print(f"Worker {worker_num} connected to session")
            while True:
                if self.crawled <= self.max_crawl:
                    try:
                        await self.get_page(session, worker_num)
                    except asyncio.CancelledError:
                        break
                else:
                    for task in asyncio.all_tasks():
                        task.cancel()
                    break
        

    async def get_page(self, session, worker_num):
        url = await self.link_queue.get()
        domain = to_domain(url)

        sucessful_crawl = True

        try:
            robot_rules = await check_robots(self.client, domain, self.domain_robot_rules)
            if robot_rules and (not robot_rules.parser.can_fetch("*", url)):
                self.link_queue.task_done()
                sucessful_crawl = False
                return

            if domain in self.domain_wait_times.keys():
                now = datetime.now(timezone.utc)
                sleep_time = (self.domain_wait_times[domain] - now)
                sleep_seconds = sleep_time.total_seconds()

                if sleep_seconds > 0:
                    print(f"Worker {worker_num} sleeping for {sleep_seconds}")
                    await asyncio.sleep(sleep_seconds)

            headers = {'User-Agent': 'iSearch'}
            now = datetime.now(timezone.utc)

            async with self.client.get(url, allow_redirects=True, headers=headers) as response:
                if not int(response.headers.get("Content-Length", 0)) > self.max_response_size:
                    content_type = response.headers.get('Content-Type')
                    content_lang = response.headers.get('Content-Language')

                    if content_type and ("text/html" not in content_type):
                        print("Skipping due to not being html")
                        self.link_queue.task_done()
                        sucessful_crawl = False
                        return

                    if content_lang and ("en" not in content_lang):
                        print("Skipping due to not being en")
                        self.link_queue.task_done()
                        sucessful_crawl = False
                        return

                    self.handle_limits(response, url, robot_rules)

                    sucess_color, fail_color, reset_foreground = "\033[32m", "\033[31m", "\033[0m"
                    if response.ok:
                        print(f"{sucess_color}Worker number {worker_num} grabbed {url} with status code {response.status} at {now}{reset_foreground}")
                    else:
                        print(f"{fail_color}Worker number {worker_num} failed to grab {url} with response code {response.status} at {now}{reset_foreground}")

                    text = await response.text()
                    await self.parse_queue.put(page_info(url, text, session))

        except Exception as e:
            with open("log.txt", "a") as f:
                f.write("get_page threw an error:")
                f.write(str(e) + "  -  " + str(url) + '  -  ' + str(domain) +'\n')
            self.link_queue.task_done()
            sucessful_crawl = False
            return 

        finally:
            if sucessful_crawl:
                self.crawled += 1
                self.link_queue.task_done()


    def handle_limits(self, response, url, robot_rules):
        can_request_at = None

        if response.status == 429 or response.status== 503:
            if response.headers:
                retry_after = response.headers.get("Retry-After")

                if retry_after:
                    print("Retry After:", retry_after)
                    if retry_after.isdigit():
                        time_delta = timedelta(seconds=int(retry_after))
                        can_request_at = datetime.now(timezone.utc) + time_delta
                    else:
                        try:
                            can_request_at = parsedate_to_datetime(retry_after)
                        except (TypeError, ValueError):
                            can_request_at = datetime.now(timezone.utc) + timedelta(seconds=15)
                else:
                    can_request_at = datetime.now(timezone.utc) + timedelta(seconds=15)
            else:
                can_request_at = datetime.now(timezone.utc) + timedelta(seconds=15)
            
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

            wait_milliseconds = wait_time * 1000
            can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=wait_milliseconds)
    
        else:
            can_request_at = datetime.now(timezone.utc) + timedelta(milliseconds=200)

        domain_url = to_domain(url)
        self.domain_wait_times[domain_url] = can_request_at


async def main():
    start_urls = ["https://nodejs.org/en"]

    workers = 30
    start = time.perf_counter()

    timeout = aiohttp.ClientTimeout(total=6)
    Session = await connect_to_db(workers)

    conn = aiohttp.TCPConnector(limit_per_host=60)
    async with aiohttp.ClientSession(timeout=timeout, connector=conn, max_line_size=8190 * 2, max_field_size=8190 * 2) as client:
        crawler = webcrawler(client, workers)
        await crawler.add_links(start_urls)
        await crawler.run_crawler(Session)

    print(time.perf_counter() - start)


if __name__ == '__main__':
    asyncio.run(main())
