import time
import traceback
import random

import asyncio
import aiohttp

from spider.crawler import webcrawler
from spider.page_parser import parser
from db import database_handler
from queues import jqueue, unique_queue



class spider:
	''' Class responsibile for handling crawling, parsing and writing to postgres. '''
	def __init__(self):
		self.link_queue = unique_queue()
		self.parse_queue = jqueue()
		self.database_queue = jqueue()

		#init these later
		self.crawl_handler = None
		self.parse_handler = None
		self.database_handler = None

		self.crawl_workers = []
		self.parse_workers = []
		self.database_workers = []

		#start with the manager
		self.worker_manager = asyncio.create_task(self.manager())
	

	def create_workers(self, crawl_worker_num, parse_worker_num, database_worker_num):
		'''Creates workers for crawling, parsing and database handling. Also  '''
		self.crawl_workers.append(asyncio.create_task(self.crawl_handler.shuffle_handler()))
		self.crawl_workers.extend([asyncio.create_task(self.crawl_handler.worker()) for _ in range(crawl_worker_num)])
		self.parse_workers = [asyncio.create_task(self.parse_handler.worker()) for _ in range(parse_worker_num)]
		self.database_workers = [asyncio.create_task(self.database_handler.worker()) for _ in range(database_worker_num)]


	async def run(self, worker_num : int, starting_urls: list, request_timeout : int = 8, tcp_limit : int = 60):
		'''Runs the spider. Starts the aiohttp session, instantiates the crawl, parse, and database handlers,
		 	adds the starter links, creates workers for crawling, parsing and database stuff and starts them up too.
			Also handles shuffling. Note that the worker list includes the manager. '''
		timeout = aiohttp.ClientTimeout(total=request_timeout)
		conn = aiohttp.TCPConnector(limit_per_host=tcp_limit)

		async with aiohttp.ClientSession(timeout=timeout, connector=conn, max_line_size=8190 * 2, max_field_size=8190 * 2) as request_client:
			self.crawl_handler = webcrawler(request_client, self.link_queue, self.parse_queue)

			for url in starting_urls:
				self.link_queue.put(url)
			await self.link_queue.shuffle()

			self.parse_handler = parser(self.link_queue, self.parse_queue, self.database_queue, worker_num)

			self.database_handler = database_handler(self.database_queue)
			await self.database_handler.connect_to_db(worker_num)

			self.create_workers(worker_num, worker_num // 2, worker_num // 2)

			try:
				await asyncio.gather(*self.crawl_workers, *self.parse_workers, *self.database_workers, self.worker_manager)
			except asyncio.CancelledError:
				pass    
	
	
	async def manager(self):
		'''Manages and kills workers. Makes workers stop adding new links once max_crawl is passed. Will kill workers once their queues are empty. '''
		while self.crawl_handler.still_running(): #i know this is basically busy waiting but idc
			await asyncio.sleep(0.5)	

		print("Stopped adding new links")
		self.parse_handler.adding_new_links = False
		while self.parse_handler.still_running():
			await asyncio.sleep(0.5)

		self.database_handler.being_added_to = False
		while self.database_handler.still_running():
			await asyncio.sleep(0.5)
		print("All done")


async def main():
	''' Get starter links and runs the spider '''

	with open("starter_links.txt", "r") as f:
		lines = f.readlines()
		lines = [line.strip() for line in lines]
	start_urls = [line for line in lines if line != "\n"]
	random.shuffle(start_urls)
	start = time.perf_counter()

	#TODO, figure out the optimal split up of workers
	s = spider()	
	await s.run(worker_num=35, starting_urls=start_urls)

	print(time.perf_counter() - start)


if __name__ == '__main__':
	try:
		asyncio.run(main())
	except Exception as e:
		print(traceback.format_exc())
	
	print("Exited.")