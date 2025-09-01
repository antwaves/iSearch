import re
import time
import urllib
from datetime import datetime, timezone
import aiohttp
import asyncio

import requests
from selectolax.parser import HTMLParser

from util import unique_queue, queue, to_domain
from db import connect_to_db, create_page, db_worker
from rate_limit import check_robots, robotsTxt, handle_limits, get_domain_lock
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
        self.domain_locks = {}
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
                    await asyncio.sleep(1)
                    await self.link_queue.shuffle()
                else:
                    await asyncio.sleep(5)
                    await self.link_queue.shuffle()
            except asyncio.CancelledError:
                break


    async def run_crawler(self, client_session):
        workers = [asyncio.create_task(self.worker(worker_num)) 
                    for worker_num in range(self.workers)]
        
        workers = workers + [asyncio.create_task(db_worker(client_session, self.db_queue)) for _ in range(self.workers)]
        
        workers.append(asyncio.create_task(parse_worker(self.parse_queue, self.link_queue, self.db_queue)))
        workers.append(asyncio.create_task(self.shuffle_handler()))

        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            pass


    async def worker(self, worker_num):
        print(f"Worker {worker_num} started up")
        while True:
            if self.crawled <= self.max_crawl:
                try:
                    await self.get_page(worker_num)
                except asyncio.CancelledError:
                    break
            else:
                for task in asyncio.all_tasks():
                    try:
                        task.cancel()
                    except Exception as e:
                        pass
                break
        

    async def get_page(self, worker_num):
        url = await self.link_queue.get()
        domain = to_domain(url)

        sucessful_crawl = True

        try:
            robot_rules = await check_robots(self.client, domain, self.domain_robot_rules)
            if robot_rules and (not robot_rules.parser.can_fetch("*", url)):
                self.link_queue.task_done()
                sucessful_crawl = False
                return

            lock = get_domain_lock(url, self.domain_locks)
            async with lock:
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

                        handle_limits(response, url, robot_rules, self.domain_wait_times)

                        sucess_color, fail_color, reset_foreground = "\033[32m", "\033[31m", "\033[0m"
                        if response.ok:
                            print(f"{sucess_color}Worker number {worker_num} grabbed {url} with status code {response.status} at {now}{reset_foreground}")
                        else:
                            print(f"{fail_color}Worker number {worker_num} failed to grab {url} with response code {response.status} at {now}{reset_foreground}")

                        text = await response.text(encoding='utf-8')
                        await self.parse_queue.put(page_info(url, text))
            

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
    try:
        asyncio.run(main())
    except Exception as e:
        pass
