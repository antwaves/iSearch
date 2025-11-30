import asyncio
import re
import time
import urllib

import aiohttp

from page_parser import page_info
from rate_limit import (rate_limiter, get_rate_limit_from_response, 
						get_rate_limit_from_robots, cannot_fetch)
from util import queue, to_domain, unique_queue, log_info, silent_log

#and cchardet and pip-system-certs

t = time.perf_counter()

#TODO: REFACTOR IT ALLLLLLLLLL

class webcrawler:
	def __init__(self, client, link_queue, parse_queue, log):
		self.client = client
		self.link_queue = link_queue
		self.parse_queue = parse_queue

		self.max_crawl = 100
		self.max_response_size = 5 * 1024 * 1024  

		self.response_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
								'Accept-Language' : 'en-US,en;q=0.9', 'Accept' : '*/*', "Cache-Control" : "max-age=0"}
		self.log = log

		self.rate_limiter = rate_limiter(client, self.response_headers)
		self.cancelled = False


	async def worker(self):
		while not self.cancelled:
			await self.get_page()
		

	async def get_page(self):
		url = await self.link_queue.get()
		domain = to_domain(url)

		if domain == "https://" or domain == "http://": #invalid domain
			self.link_queue.task_done()
			print('exited')
			return None

		try:
			robot_rules = await self.rate_limiter.check_robots(domain)
			if cannot_fetch(url, robot_rules):
				print("cannot fetch", url)
				return None

			lock = self.rate_limiter.get_domain_lock(url)
			async with lock:
				sleep_time = self.rate_limiter.get_sleep_time(domain)
				if sleep_time:
					await asyncio.sleep(sleep_time)

				try:
					async with self.client.get(url, allow_redirects=True, headers=self.response_headers) as response:
						if not self.filter_response(response.headers):
							return None

						self.rate_limiter.set_rate_limits(response, url, robot_rules)
						text = await response.text(encoding='utf-8', errors='replace')
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
		try:
			while not self.cancelled:
				if self.log.crawled < 2:
					await asyncio.sleep(1)
					await self.link_queue.shuffle()
				else:
					await asyncio.sleep(5)
					await self.link_queue.shuffle()

		except Exception as e:
			silent_log(e, "shuffle_handler")
