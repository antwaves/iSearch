import re
import time
import urllib
import os

import asyncio
import aiohttp
from dotenv import load_dotenv

from spider.page_parser import page_info
from spider.rate_limit import (rate_limiter, get_rate_limit_from_response, 
						get_rate_limit_from_robots, can_fetch)
from spider.util import to_domain, silent_log
from spider.async_request import fetch

#and cchardet and pip-system-certs


#TODO: REFACTOR IT ALLLLLLLLLL
class webcrawler:
	''' Class responsible for crawling. Using an async session provided by the spider, visits (and filters through) pages, and gives their content
		to the page parser. Page parser gives back outlinks which are then visited.'''

	def __init__(self, session, link_queue, parse_queue):
		self.session = session 
		self.link_queue = link_queue
		self.parse_queue = parse_queue

		self.crawled = 0
		self.max_crawl = 100000

		load_dotenv()
		email = os.getenv("CONTACT_EMAIL")
		self.response_headers = {f'User-Agent': f'iSearchBot/1.0 (https://github.com/antwaves/iSearch; {email}) aiohttp/3.13.3',
								'Accept-Language' : 'en-US,en;q=0.9', 'Accept' : '*/*', "Cache-Control" : "max-age=0"}		
		self.max_response_size = 5 * 1024 * 1024  

		self.rate_limiter = rate_limiter(session, self.response_headers)
		self.request_semaphore = asyncio.Semaphore(8)


	def still_running(self):
		return self.crawled <= self.max_crawl


	async def worker(self):
		while self.still_running():
			t = time.time()
			p = await self.get_page()	


	async def get_page(self):
		t = time.perf_counter()

		urls = await self.link_queue.get()

		if not urls:
			self.link_queue.task_done()
			return 

		url_to_domain = {}
		for url in urls:
			domain = to_domain(url)
			if domain == "https://" or domain == "http://": #invalid domain
				continue
			url_to_domain[url] = domain 
		urls = list(url_to_domain.keys())
		domains = list(set(url_to_domain.values()))	
		
		try:
			robot_rules = await self.rate_limiter.check_robots_for_batch(domains)

			if not robot_rules:
				self.link_queue.task_done()
				return

			to_grab = []
			for url in urls:
				domain = url_to_domain[url]
				robot_rule = robot_rules[domain]
				if can_fetch(url, robot_rule):
					to_grab.append(url)
		
			sleep_times = [self.rate_limiter.get_sleep_time(domain) for domain in domains]
			sleep_time = max(sleep_times)
			if sleep_time >= 4:
				print("Ran into a long sleep time")
				self.link_queue.put(tuple(to_grab))
				self.link_queue.task_done()
				return
			await asyncio.sleep(sleep_time)

			async with self.request_semaphore:
				try:
					responses = [fetch(self.session, url, self.response_headers) for url in to_grab]
					response_results = await asyncio.gather(*responses)
					
					for response in response_results:
						if not response['received'] or not self.is_response_good(response['headers']):
							continue
						
						url = response['url']
						domain = url_to_domain[url]
						response_text = response['text']

						self.rate_limiter.set_rate_limits(response, url, robot_rules[domain], domain)
						await self.parse_queue.put(page_info(url, response_text))
					
					self.crawled += len(to_grab)
				except Exception as e:
					print(f"Exception in get-page-request {e}")
					silent_log(e, "get_page-request", [url, domain])

		except Exception as e:
			print(f"Exception in get_page {e}")
			silent_log(e, "get_page", [url, domain])
			return 

		finally:
			self.link_queue.task_done()


	def is_response_good(self, headers): 
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
					await self.link_queue.shuffle(450)

				else:
					seconds_elapsed = time.time() - t
					sleep_time = (5 + (0.07 * seconds_elapsed))
					await asyncio.sleep(sleep_time)
		
					print(f"{"\n" * 3}Shuffling")
					await self.link_queue.shuffle(450)
					print(time.time() - t, "seconds elapsed")
					print(f"{self.crawled} pages crawled")
					r = self.rate_limiter
					print(f"{(r.cache_hits / (r.cache_hits + r.cache_misses)) * 100}% cache hit rate")
					print(f"{(r.block_rate / (r.block_rate + r.not_blocked_rate)) * 100}% blocked rate{'\n' * 3}")

		except Exception as e:
			silent_log(e, "shuffle_handler")
