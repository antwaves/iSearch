import asyncio
import time
import re
from io import StringIO
from collections import Counter
import traceback
import os
from concurrent.futures import ProcessPoolExecutor
import random


from queues import queue
from db import connect_to_db, get_pages, insert_terms, add_chunk, retrieve_term_pages


MAX_PARAMS = 15000
SAFETY = 1000

class page_info:
    #contains page id and content
    def __init__(self, id, content):
        self.id = id
        self.content = content

    def __repr__(self):
        return f"page id {self.id}"

       
class index_handler:
    def __init__(self, workers):
        self.workers = workers

        with open("stopwords.txt", "r") as f:
            self.stopwords = set([word.strip() for word in f.readlines()])

        self.adding_new_pages = True
        self.still_inserting = True

        self.page_chunks = asyncio.Queue(self.workers * 10)
        self.insert_chunks = asyncio.Queue(self.workers * 10)

        self.loop = asyncio.get_running_loop()
        self.pool = ProcessPoolExecutor(max_workers=self.workers)

        self.indexed = 0
        self.batch_requests = 0

        self.batch_size = 1000
        self.condition = asyncio.Condition()
        self.semaphore = asyncio.Semaphore(max(2, self.workers // 3))

        self.current_insert_user = None


    async def run_indexer(self):
        session_maker = await connect_to_db(self.workers) 

        workers = [asyncio.create_task(self.term_insert_worker(session_maker)) for _ in range(self.workers // 2)]  
        workers.extend([asyncio.create_task(self.term_link_insert_worker(session_maker)) for _ in range(self.workers)])
        workers.append(asyncio.create_task(self.page_getter(session_maker, batch_size=self.batch_size)))

        try:
            await asyncio.gather(*workers)      
        except asyncio.exceptions.CancelledError:
            pass
        
        print("All done!")


    async def page_getter(self, session_maker, batch_size):
        async with session_maker() as session:
            async for page in get_pages(session, batch_size=self.batch_size):
                async with self.condition:
                    await self.condition.wait_for(lambda: self.batch_requests > 0)
                    print("Added new batch")
                    self.batch_requests -= 1
                    await self.page_chunks.put(page)
                    
            self.adding_new_pages = False


    async def term_insert_worker(self, session_maker):
        j = random.randint(1, 1000)
        try:
            while self.adding_new_pages or not self.page_chunks.empty():
                async with self.condition:
                    self.batch_requests += 1
                    self.condition.notify()

                t = time.time()
                #print(f"Worker {j} is waiting on batch")
                chunk = await self.page_chunks.get()

                #print(f"Worker {j} was waiting on a batch for {time.time() - t} seconds")
                self.page_chunks.task_done()


                #stream out THIS data 
                t = time.time()
                term_data = await self.loop.run_in_executor(self.pool, process_chunk, chunk, self.stopwords)
                self.indexed += self.batch_size
                print(f"Worker {j} got term_pages and term_page_frequency. It took {time.time() - t} seconds")

                
                for term_batch in batch_dict(term_data, self.batch_size):
                    t = time.time()
                    print(f"Worker {j} is waiting on term ids")
                    async with session_maker() as session: #add a proper semaphore on the session_maker()
                        t = time.time()
                        print(f"Worker {j} is waiting on insert lock. Current insert user is {self.current_insert_user}")
                        async with self.semaphore:
                            self.current_insert_user = j
                            print(f"Worker {j} escapes insert_lock. Waited {time.time() - t} seconds")
                            term_ids = await insert_terms(session, term_batch, MAX_PARAMS) #list containing [term, term_id] for all inserted terms 
                    print(f"Worker {j} got term ids. It took {time.time() - t} seconds")
                    
                    term_values = []
                    for obj in term_ids:
                        length = len(term_values)
                        if length  >= MAX_PARAMS - SAFETY:
                            chunk_values, temp_values = term_values[:min(MAX_PARAMS, length)], term_values[min(MAX_PARAMS, length):]   
                            await self.insert_chunks.put(chunk_values)
                            print(f"Worker {j} added to insert_chunk")

                            term_values = temp_values

                        term, term_id = obj[0], obj[1]
                        pages_containing_term = term_batch[term]

                        row = [{"term_id": term_id, "page_id": page_id} for page_id in pages_containing_term]
                        term_values.extend(row)
 
                    if term_values:
                        await self.insert_chunks.put(term_values)
                    
                print(f"Finished a batch of {self.batch_size} pages. {self.indexed} total pages done")


        except Exception as e:
            print(traceback.format_exc())

        if self.page_chunks.empty():
            self.still_inserting = False 



    async def term_link_insert_worker(self, session_maker):
        iteration = 0
        async with session_maker() as session:
            try:
                while not self.insert_chunks.empty() or self.adding_new_pages or self.still_inserting:
                    t = time.time()
                    if iteration % 30 == 0:
                        print("commited")
                        print(f"{self.insert_chunks.qsize()} chunks remaining in current queue")

                        await session.commit()
                    
                    chunk = await self.insert_chunks.get()
                    await add_chunk(session, chunk)

                    iteration += 1

            except asyncio.CancelledError:
                await session.commit()

            except Exception as e:
                print(e)
                await session.rollback()
            
            finally:
                self.insert_chunks.task_done()
            
            await session.commit()
        print('Finished!')


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
            term_data.setdefault(term, []).append(page.id)

    
    terms = list(term_data.keys())
    for term in terms:
        if not filter_term(term, len(term_data[term])): #term_data[term][1] = page frequency
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
    workers = 15

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
