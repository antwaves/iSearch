import asyncio
import time
import re
from io import StringIO
from collections import Counter
import traceback
import os
from concurrent.futures import ProcessPoolExecutor

from queues import queue
from db import connect_to_db, get_pages, insert_terms, add_chunk


MAX_PARAMS = 15000

class page_info:
    #contains page id and content
    def __init__(self, id, content):
        self.id = id
        self.content = content

    def __repr__(self):
        return f"page id {self.id}"


class term_data:
    #contains the total occurences of a list of terms, also link said terms back to their orginal pages
    def __init__(self):
        self.pages = []
    
    def add(self, page):
        self.pages.append(page)

    def __repr__(self):
        return f"{len(self.pages)} total pages and {len(pages)} total occurring pages"

       
class index_handler:
    def __init__(self, workers):
        self.workers = workers

        self.term_finder = re.compile(r"[A-Za-z0-9_-]+")
        self.vowels = set(["a", "e", "i", "o", "u", "y"])
        with open("stopwords.txt", "r") as f:
            self.stopwords = set([word.strip() for word in f.readlines()])

        self.punctuation = "".join([".", "?", "!", ",", ":", ";", "—", "(", ")", "[", "]", "{", "}", "'", '"', "/", "*", "&", "~", "+"])
        self.translator = str.maketrans("", "", self.punctuation) #removes punctuation

        self.adding_new_pages = True

        self.page_chunks = queue()
        self.insert_chunks = queue()

        self.insert_lock = asyncio.Lock()


    async def run_indexer(self):
        print(os.cpu_count())
        session_maker = await connect_to_db(self.workers) 

        async with session_maker() as session:
            workers = [asyncio.create_task(self.term_insert_worker(session_maker)) for _ in range(self.workers // 2)]  
            workers.extend([asyncio.create_task(self.term_link_insert_worker(session_maker)) for _ in range(self.workers // 2)])
            workers.append(asyncio.create_task(self.chunk_inserter(session, batch_size=100)))

            try:
                await asyncio.gather(*workers)      
            except asyncio.exceptions.CancelledError:
                pass
        
        print("All done!")


    async def chunk_inserter(self, session, batch_size):
        async for page in get_pages(session, batch_size=150):
            await self.page_chunks.put(page)
        self.adding_new_pages = False


    async def term_insert_worker(self, session_maker):
        async with session_maker() as session:
            while self.adding_new_pages or self.page_chunks.length() > 0:
                chunk = await self.page_chunks.get()
                self.page_chunks.task_done()
                term_values = await self.process_chunk(session, chunk)

                chunk = []
                while term_values:
                    length = len(term_values)
                    chunk, term_values = term_values[:min(MAX_PARAMS, length)], term_values[min(MAX_PARAMS, length):]     
                    await self.insert_chunks.put(chunk)
            

    async def term_link_insert_worker(self, session_maker):
        iteration = 0
        async with session_maker() as session:
            try:
                while self.insert_chunks.length() != 0 or self.adding_new_pages:
                    if iteration % 30 == 0:
                        await session.commit()
                    chunk = await self.insert_chunks.get()
                    await add_chunk(session, chunk)
                    print("Added a chunk of 100 pages!")

            except asyncio.CancelledError:
                await session.commit()

            except Exception as e:
                await session.rollback()
            
            finally:
                self.insert_chunks.task_done()
            
            await session.commit()
        print('Finished!')


    async def process_chunk(self, session, chunk):
        term_pages = self.get_terms_from_pages(chunk)

        term_page_frequency = {} #key = term, value = how many pages a term appears on
        for term, pages_containing_term in term_pages.items(): 
            amount_of_pages = len(pages_containing_term)
            filter_term(term_page_frequency, term, amount_of_pages)
            
        async with self.insert_lock:
            term_ids = await insert_terms(session, term_page_frequency, MAX_PARAMS) #list containing [term, term_id] for all inserted terms 

        values = []
        for obj in term_ids:
            term, term_id = obj[0], obj[1]
            pages_containing_term = term_pages[term]

            row = [{"term_id": term_id, "page_id": page_id} for page_id in pages_containing_term]
            values.extend(row)

        return values


    def get_terms_from_page(self, content):
        content = content.encode("ascii", "ignore").decode()
        content = content.translate(self.translator)
        terms = self.term_finder.finditer(content)

        stopwords = self.stopwords
        final_terms = []
        for match in terms:
            term = match.group().lower()
   
            if term in stopwords:
                continue

            length = len(term)
            if length <= 1 or length >= 30:
                continue
            
            if length > 20:
                vowels = sum(char in self.vowels for char in term)
                if vowels > 7 and vowels + 1 < length // 2:
                    continue

                numbers = len([char for char in term if char.isdecimal()])
                if numbers > 5 and numbers + 1 < length // 2:
                    continue

            final_terms.append(term)

        return final_terms

    
    def get_terms_from_pages(self, pages):
        term_pages = {} #key = term, value = pages that contain term
        for p in pages:
            page = page_info(p[0], p[1])
            terms = set(self.get_terms_from_page(page.content))
            for term in terms:
                term_pages.setdefault(term, []).append(page.id)

        return term_pages
    

    async def log_worker(self):
        while self.insert_chunks.length() > 0:
            await asyncio.sleep(0.25)
            print(f"{self.insert_chunks.length()} chunks remaining {10 * " "}", end="\r")


def filter_term(term_page_frequency, term, amount_of_pages):
    '''Filter out terms that are both too infrequent and too long/short'''
    term_length = len(term)
    frequent_term = amount_of_pages > 20
    valid_length_term = term_length > 3 and term_length < 15

    if valid_length_term or frequent_term:
        term_page_frequency[term] = amount_of_pages


async def main():
    workers = 4

    try:
        indexer = index_handler(workers)
        await indexer.run_indexer()
    except Exception as e:
        print(traceback.format_exc())
    

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)
