import asyncio
import time
import re
from io import StringIO
from collections import Counter
import traceback
import os
from concurrent.futures import ProcessPoolExecutor
import random

from dataclasses import dataclass

from queues import queue
from db import connect_to_db, get_pages, insert_terms_safe, add_chunk_safe, set_term_counts, retrieve_term_pages

MAX_PARAMS = 14000


@dataclass
class page_info:
    ''' Struct containing page id in the database and page content'''
    p_id: int
    content: str


class index_handler:
    ''' Class that handles everything related to indexing. Reads from page table, finds all terms, adds them to the
    term table and connects term ids to page ids '''
    def __init__(self, workers):
        self.worker_num = workers

        with open("stopwords.txt", "r") as f:
            self.stopwords = set([word.strip() for word in f.readlines()])

        self.adding_new_pages = True
        self.still_inserting = True
        self.current_insert_user = None

        self.page_chunks = asyncio.Queue(self.worker_num * 10)
        self.insert_chunks = asyncio.Queue(self.worker_num * 10)

        self.loop = asyncio.get_running_loop()
        self.pool = ProcessPoolExecutor(max_workers=self.worker_num)

        self.indexed = 0
        self.batch_size = 750 #increase with amount of memory available on machine

        self.batch_requests = 0
        self.condition = asyncio.Condition()
        self.semaphore = asyncio.Semaphore(max(2, self.worker_num // 3))

        self.term_workers = []
        self.term_link_workers = []
        
        self.created_batches = 0
        self.sent_batches = 0


    async def run_indexer(self):
        ''' Runs the indexer. Initalizes all workers. '''

        session_maker = await connect_to_db(self.worker_num) 

        self.term_workers = [asyncio.create_task(self.term_insert_worker(session_maker, i)) for i in range(self.worker_num // 2)]  
        self.term_workers.append(asyncio.create_task(self.page_getter(session_maker, batch_size=self.batch_size)))

        self.term_link_workers = [asyncio.create_task(self.term_link_insert_worker(session_maker)) for _ in range(self.worker_num)]
        
        try:
            await asyncio.gather(*self.term_workers, *self.term_link_workers)      
        except asyncio.exceptions.CancelledError:
            pass

        print(self.created_batches, self.sent_batches)
        await set_term_counts(session_maker)
        print("All done!")


    async def are_term_workers_alive(self):
        for task in self.term_workers:
            if not task.done():
                return True
        return False


    async def page_getter(self, session_maker, batch_size):
        ''' Feeds batches of pages of batch_size length to the page_chunks queue, only adding new batches to the
        queue when term_insert_workers request for an add.'''
        async with session_maker() as session:
            async for page in get_pages(session, batch_size=self.batch_size):
                async with self.condition:
                    await self.condition.wait_for(lambda: self.batch_requests > 0)
                    print("Added new batch")
                    self.batch_requests -= 1
                    await self.page_chunks.put(page)
                    
            self.adding_new_pages = False


    async def term_insert_worker(self, session_maker, worker_id):
        ''' Handles reading and processing page batches, then sends that off to the term_link_insert workers.
        Recieves a batch, gets all terms using process_chunk (running said function in a seperate process), writes the 
        terms to the database, links the terms to the pages that contain them and sends that to the term_link_inser workers '''

        try:
            while self.adding_new_pages or not self.page_chunks.empty():
                async with self.condition:
                    self.batch_requests += 1
                    self.condition.notify()

                chunk = await self.page_chunks.get()
                self.page_chunks.task_done()

                t = time.time()
                term_data = await self.loop.run_in_executor(self.pool, process_chunk, chunk, self.stopwords)

                for term_batch in batch_dict(term_data, self.batch_size):
                    async with self.semaphore:
                        self.current_insert_user = worker_id
                        term_ids = await insert_terms_safe(session_maker, term_batch, MAX_PARAMS) #list containing [term, term_id] for all inserted terms 
                    
                    term_values = []
                    for term, term_id in term_ids:
                        length = min(MAX_PARAMS, len(term_values))
                        if length >= MAX_PARAMS:
                            chunk_values, term_values = term_values[:length], term_values[length:]
                            chunk_values = sorted(chunk_values, key=lambda x: x["term_id"]) #sort to avoid sharelocks
                            await self.insert_chunks.put(chunk_values)
                            self.created_batches += 1

                        pages_containing_term = term_batch[term]

                        row = [{"term_id": term_id, "page_id": page_id} for page_id in pages_containing_term]
                        term_values.extend(row)

                    if term_values:
                        term_values = sorted(term_values, key=lambda x : x['term_id']) #sort to avoid sharelocks
                        await self.insert_chunks.put(term_values)
                        self.created_batches += 1 

                self.indexed += self.batch_size
                print(f"Finished a batch of {self.batch_size} pages. {self.indexed} total pages done")
                print(self.created_batches, self.sent_batches)

            if self.page_chunks.empty():
                self.still_inserting = False

        except asyncio.CancelledError:
            pass

        except Exception as e:
            print(traceback.format_exc())


    async def term_link_insert_worker(self, session_maker):
        ''' Recieves term_id page_id pairs from term_inser worker and writes them to the database. '''
        iteration = 0
        chunk = None
        try:
            while not self.insert_chunks.empty() or self.still_inserting or await self.are_term_workers_alive(): 
                try:
                    chunk = await asyncio.wait_for(self.insert_chunks.get(), 10)
                except asyncio.TimeoutError:
                    print('Timed out!')
                    continue
                    
                await add_chunk_safe(session_maker, chunk)
                self.sent_batches += 1

                iteration += 1
        
            print('Finished!')
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(traceback.format_exc())


def filter_term(term, amount_of_pages):
    '''Filter out terms that are both too infrequent and too long/short'''
    term_length = len(term)
    frequent_term = amount_of_pages > 20
    valid_length_term = term_length > 3 and term_length < 15

    return valid_length_term or frequent_term


def process_chunk(chunk, stopwords):
    vowels = "aeiouy"
    punctuation = ".?!,:;—()[]{}\\\'\"/*&~+"
    translator = str.maketrans("", "", punctuation)
    term_finder = re.compile(r"[A-Za-z0-9_-]+")

    term_data = {}

    for obj in chunk:
        page = page_info(obj[0], obj[1])
        content = page.content

        content = content.encode("ascii", "ignore").decode()
        content = content.translate(translator)
        terms = term_finder.finditer(content)

        final_terms = []
        terms_seen = set()

        for match in terms:
            term = match.group().lower()
   
            if term in stopwords:
                continue

            length = len(term)
            if length <= 1 or length >= 30:
                continue
            
            if length > 20:
                vowel_amount = sum(char in vowels for char in term)
                if vowel_amount > 7 and vowel_amount + 1 < length // 2:
                    continue

            final_terms.append(term)
            terms_seen.add(term)
        
        for term in final_terms:
            term_data.setdefault(term, []).append(page.p_id)

    
    terms = list(term_data.keys())
    for term in terms:
        if not filter_term(term, len(term_data[term])): 
            term_data.pop(term)
        
    return term_data


def batch_dict(full_dict, batch_size):
    size = 0
    batch = {}

    for key in full_dict.keys():
        if size >= batch_size:
            yield batch
            batch = {}
            size = 0
        
        batch[key] = full_dict[key]
        size += 1

    if batch:
        yield batch


async def main():
    workers = 15 #increase with amount of cores on machine, set to one below amount of cores for best effect

    try:
        indexer = index_handler(workers)
        await indexer.run_indexer()
    except Exception as e:
        print(traceback.format_exc())
    

if __name__ == "__main__":
    t = time.time()
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)

    print(time.time() - t)
