import asyncio
import re
import time
import urllib

import aiohttp
import requests
from selectolax.parser import HTMLParser

from db import connect_to_db, create_page, db_worker
from page_parser import page_info, parse_worker
from rate_limit import (check_robots, get_domain_lock, get_sleep_time,
						get_rate_limits, robotsTxt, cannot_fetch)
from util import queue, to_domain, unique_queue, log_info, silent_log

#and cchardet and pip-system-certs

t = time.perf_counter()

#TODO: REFACTOR IT ALLLLLLLLLL

class webcrawler:
	def __init__(self, client, workers):
		self.client = client
		self.link_queue = unique_queue()

		self.domain_wait_times = {}
		self.domain_locks = {}
		self.domain_robot_rules = {}

		self.max_crawl = 1000
		self.max_response_size = 5 * 1024 * 1024  

		self.response_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
								'Accept-Language' : 'en-US,en;q=0.9', 'Accept' : '*/*'}
		self.log = log_info()


		self.parse_queue = queue()
		self.db_queue = queue()

		self.workers = workers



	async def run_crawler(self, client_session):
		print("Setting up..")

		workers = [asyncio.create_task(self.worker(worker_num)) for worker_num in range(self.workers)]
		workers += [asyncio.create_task(db_worker(client_session, self.db_queue, self.log)) for _ in range(self.workers // 2)]
		workers  += [asyncio.create_task(parse_worker(self.parse_queue, self.link_queue, self.db_queue)) for _ in range(self.workers // 2)] 
		
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
				for task in asyncio.all_tasks(): #kill all other workers
					task.cancel()
				break
		

	async def get_page(self, worker_num):
		url = await self.link_queue.get()
		domain = to_domain(url)

		if domain == "https://" or domain == "http://": #invalid domain
			self.link_queue.task_done()
			print('exited')
			return None

		try:
			robot_rules = await check_robots(self.client, domain, self.domain_robot_rules, self.response_headers)
			if cannot_fetch(url, robot_rules):
				return None

			lock = get_domain_lock(url, self.domain_locks)
			async with lock:
				sleep_time = get_sleep_time(domain, self.domain_wait_times)
				if sleep_time:
					print(sleep_time)
					await asyncio.sleep(sleep_time)

				try:
					async with self.client.get(url, allow_redirects=True, headers=self.response_headers) as response:
						if not self.filter_response(response.headers):
							return None

						get_rate_limits(response, url, robot_rules, self.domain_wait_times)
						text = await response.text(encoding='utf-8')
						await self.parse_queue.put(page_info(url, text))

						self.log.update(visited=url)
				except Exception as e:
					silent_log(e, "get_page-request", [url, domain])

			self.log.inc(crawl=True)
		except Exception as e:
			silent_log(e, "get_page", [url, domain])
			return None

		finally:
			self.link_queue.task_done()


	def filter_response(self, headers): 
		if int(headers.get("Content-Length", 0)) > self.max_response_size:
			return False

		content_type = headers.get('Content-Type')
		content_lang = headers.get('Content-Language')

		if content_type and ("text/html" not in content_type):
			return False

		if content_lang and ("en" not in content_lang):
			return False

		return True

	
	async def shuffle_handler(self):
		while True:
			try:
				if self.log.crawled < 2:
					await asyncio.sleep(1)
					await self.link_queue.shuffle()
				else:
					await asyncio.sleep(5)
					await self.link_queue.shuffle()
					self.log.display()
			except asyncio.CancelledError:
				break

	async def add_links(self, urls):
		for url in urls:
			self.link_queue.put(url)
		
		await self.link_queue.shuffle()
	

async def main():
	start_urls = ["https://nodejs.org/en"]
	start = time.perf_counter()

	workers = 35

	timeout = aiohttp.ClientTimeout(total=8)
	database_session = await connect_to_db(workers)

	conn = aiohttp.TCPConnector(limit_per_host=60)
	async with aiohttp.ClientSession(timeout=timeout, connector=conn, max_line_size=8190 * 2, max_field_size=8190 * 2) as client:
		crawler = webcrawler(client, workers)
		await crawler.add_links(start_urls)
		await crawler.run_crawler(database_session)

	print(time.perf_counter() - start)


if __name__ == '__main__':
	try:
		asyncio.run(main())
	except Exception as e:
		print(e)
