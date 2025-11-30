import time

import asyncio
import aiohttp

from crawler import webcrawler
from page_parser import parser
from db import database_handler
from util import queue, unique_queue, log_info


class spider:
	def __init__(self):
		#if none, need to init later in an async context

		self.link_queue = unique_queue()
		self.parse_queue = queue()
		self.database_queue = queue()

		self.crawl_handler = None
		self.parse_handler = None
		self.database_handler = None

		self.crawl_workers = []
		self.parse_workers = []
		self.database_workers = []

		self.worker_manager = asyncio.create_task(self.manager())
		self.log = log_info()
	

	async def run(self, workers : int, starting_urls: list, request_timeout : int = 8, tcp_limit : int = 60):
		timeout = aiohttp.ClientTimeout(total=request_timeout)
		conn = aiohttp.TCPConnector(limit_per_host=tcp_limit)

		async with aiohttp.ClientSession(timeout=timeout, connector=conn, max_line_size=8190 * 2, max_field_size=8190 * 2) as client:
			self.crawl_handler = webcrawler(client, self.link_queue, self.parse_queue, self.log)

			for url in starting_urls:
				self.link_queue.put(url)
			await self.link_queue.shuffle()

			self.parse_handler = parser(self.link_queue, self.parse_queue, self.database_queue)

			self.database_handler = database_handler(self.database_queue, self.log)
			await self.database_handler.connect_to_db(workers)

			self.crawl_workers = [asyncio.create_task(self.crawl_handler.worker()) for _ in range(workers)]
			self.crawl_workers.append(asyncio.create_task(self.crawl_handler.shuffle_handler()))
			self.parse_workers = [asyncio.create_task(self.parse_handler.worker()) for _ in range(workers // 2)]
			self.database_workers = [asyncio.create_task(self.database_handler.worker()) for _ in range(workers // 2)]

			try:
				await asyncio.gather(*self.crawl_workers, *self.parse_workers, *self.database_workers, self.worker_manager)
			except asyncio.CancelledError:
				pass    
	
	
	async def manager(self):
		'''Manages and kills workers'''

		while self.log.crawled < self.crawl_handler.max_crawl:
			await asyncio.sleep(5)
			self.log.display()
		self.parse_handler.adding_new_links = False

		while (not self.link_queue.empty()) or (not self.parse_queue.empty()):
			await asyncio.sleep(1)

		self.parse_handler.cancelled = True
		self.crawl_handler.cancelled = True

		while not self.database_queue.empty():
			await asyncio.sleep(1)
		self.database_handler.cancelled = True



async def main():
	start_urls = ["https://code.visualstudio.com/"]
	start = time.perf_counter()

	s = spider()	
	await s.run(workers=35, starting_urls=start_urls)

	print(time.perf_counter() - start)


if __name__ == '__main__':
	try:
		asyncio.run(main())
	except Exception as e:
		print(e)
