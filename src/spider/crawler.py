import re
import time
import urllib
import os

import asyncio
import aiohttp
from dotenv import load_dotenv

from spider.page_parser import page_info
from spider.rate_limit import (rate_limiter, get_rate_limit_from_response, 
						get_rate_limit_from_robots, cannot_fetch)
from spider.util import to_domain, silent_log

#and cchardet and pip-system-certs


#TODO: REFACTOR IT ALLLLLLLLLL
class webcrawler:
	''' Class responsible for crawling. Using an async session provided by the spider, visits (and filters through) pages, and gives their content
		to the page parser. Page parser gives back outlinks which are then visited.'''

	def __init__(self, request_client, link_queue, parse_queue):
		self.request_client = request_client 
		self.link_queue = link_queue
		self.parse_queue = parse_queue

		self.crawled = 0
		self.max_crawl = 100000

		load_dotenv()
		email = os.getenv("CONTACT_EMAIL")
		self.response_headers = {f'User-Agent': f'iSearchBot/1.0 (https://github.com/antwaves/iSearch; {email}) aiohttp/3.13.3',
								'Accept-Language' : 'en-US,en;q=0.9', 'Accept' : '*/*', "Cache-Control" : "max-age=0"}		
		self.max_response_size = 5 * 1024 * 1024  

		self.rate_limiter = rate_limiter(request_client, self.response_headers)


	def still_running(self):
		return self.crawled <= self.max_crawl


	async def worker(self):
		while self.still_running():
			t = time.time()
			p = await self.get_page()	


	async def get_page(self):
		url = await self.link_queue.get()
		domain = to_domain(url)

		if domain == "https://" or domain == "http://": #invalid domain
			self.link_queue.task_done()
			return None

		try:
			robot_rules = await self.rate_limiter.check_robots(domain)
			if cannot_fetch(url, robot_rules):
				return None

			lock = self.rate_limiter.get_domain_lock(url)
			async with lock:
				sleep_time = self.rate_limiter.get_sleep_time(domain)
				if sleep_time:
					await asyncio.sleep(sleep_time)

				try:
					async with self.request_client.get(url, allow_redirects=True, headers=self.response_headers) as response:
						if not self.filter_response(response.headers):
							return None

						self.rate_limiter.set_rate_limits(response, url, robot_rules)
						text = await response.text(encoding='utf-8', errors='replace')
						await self.parse_queue.put(page_info(url, text))
					
					self.crawled += 1
				except Exception as e:
					silent_log(e, "get_page-request", [url, domain])

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
		t = time.time()

		try:
			while self.still_running():
				if self.crawled < 2:
					await asyncio.sleep(1)
					until_next_shuffle = self.crawled + await self.link_queue.shuffle()

				else:
					seconds_elapsed = time.time() - t
					sleep_time = 5
					await asyncio.sleep(sleep_time)
					if until_next_shuffle > self.crawled:
						print(f"Skipped over a shuffle. {until_next_shuffle - self.crawled} until next shuffle")
						continue

					print(f"{"\n" * 3}Shuffling")
					until_next_shuffle = self.crawled + await self.link_queue.shuffle()
					print(time.time() - t, "seconds elapsed")
					print(f"{self.crawled} pages crawled")
					r = self.rate_limiter
					print(f"{(r.cache_hits / (r.cache_hits + r.cache_misses)) * 100}% cache hit rate")
					print(f"{(r.block_rate / (r.block_rate + r.not_blocked_rate)) * 100}% blocked rate{'\n' * 3}")

		except Exception as e:
			silent_log(e, "shuffle_handler")
