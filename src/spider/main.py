import asyncio
import re
import time
import urllib
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import requests
from selectolax.parser import HTMLParser

from db import connect_to_db, create_page, db_worker
from page_parser import page_info, parse_worker, filter_response
from rate_limit import (check_robots, get_domain_lock, get_sleep_time,
						handle_limits, robotsTxt)
from util import queue, to_domain, unique_queue, log_info

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

		self.log = log_info()

		self.max_crawl = 1000
		self.max_response_size = 5 * 1024 * 1024  

		self.shuffle_count = 100000
		self.last_crawl = None

		self.response_headers = {'User-Agent': 'iSearch'}


	async def add_links(self, urls):
		for url in urls:
			self.link_queue.put(url)
		
		await self.link_queue.shuffle()
		
	
	async def shuffle_handler(self):
		while True:
			try:
				if self.log.crawled < 2:
					await asyncio.sleep(1)
					await self.link_queue.shuffle()
				else:
					await asyncio.sleep(5)
					await self.link_queue.shuffle()
			except asyncio.CancelledError:
				break


	async def run_crawler(self, client_session):
		print("Setting up..")

		workers = [asyncio.create_task(self.worker(worker_num)) 
					for worker_num in range(self.workers)]
		
		workers += [asyncio.create_task(db_worker(client_session, self.db_queue, self.log)) for _ in range(self.workers // 2)]

		executor = ThreadPoolExecutor()
		workers += workers + [asyncio.create_task(parse_worker(self.parse_queue, self.link_queue, self.db_queue, executor)) for _ in range(self.workers // 2)] 
		
		workers.append(asyncio.create_task(self.shuffle_handler()))

		try:
			await asyncio.gather(*workers)
		except asyncio.CancelledError:
			pass    
	

	async def worker(self, worker_num):
		while True:
			if self.log.crawled <= self.max_crawl:
				try:
					await self.get_page(worker_num)
				except asyncio.CancelledError:
					break
			elif self.log.crawled >= self.max_crawl and not self.link_queue.empty():
				try:
					await self.get_page(worker_num)
				except asyncio.CancelledError:
					break
			else:
				for task in asyncio.all_tasks():
					task.cancel()
				break
		

	async def get_page(self, worker_num):
		url = await self.link_queue.get()
		domain = to_domain(url)

		try:
			robot_rules = await check_robots(self.client, domain, self.domain_robot_rules, self.response_headers)
			if robot_rules and (not robot_rules.parser.can_fetch("*", url)):
				self.link_queue.task_done()
				return None

			lock = get_domain_lock(url, self.domain_locks)
			async with lock:
				sleep_time = get_sleep_time(domain, self.domain_wait_times)
				if sleep_time:
					await asyncio.sleep(sleep_time)

				async with self.client.get(url, allow_redirects=True, headers=self.response_headers) as response:
					if not filter_response(response.headers, self.max_response_size):
						self.link_queue.task_done()
						return None

					handle_limits(response, url, robot_rules, self.domain_wait_times)

					text = await response.text(encoding='utf-8')
					await self.parse_queue.put(page_info(url, text))

					self.log.update(visited=url)

		except Exception as e:
			with open("log.txt", "a") as f:
				try:
					f.write("get_page threw an error:")
					f.write(str(e) + "  -  " + str(url) + '  -  ' + str(domain) +'\n')
				except Exception as e:
					print("An invalid character has been introduced somewhere, but don't panic. The problem is.. we can't print the error")

			self.link_queue.task_done()
			return None

		finally:
			self.log.inc(crawl=True)
			self.link_queue.task_done()


async def main():
	start_urls = ["https://freedom.press/digisec/blog/journalists-digital-security-checklist/", "https://cobalt.tools/"]

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
		print(e)
