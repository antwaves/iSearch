import asyncio
import time
import re
from io import StringIO
from collections import Counter
import os

from util import queue, page_info, term_data
from db import connect_to_db, get_pages, get_term_ids, add_chunk, retrieve_term_pages

MAX_PARAMS = 15000
       
class index_handler:
    def __init__(self, workers):
        self.workers = workers
        self.pages_to_index = []

        self.term_finder = re.compile(r"(?:[A-Za-z0-9_-]+)")
        self.punctuation = "".join([".", "?", "!", ",", ":", ";", "—", "(", ")", "[", "]", "{", "}", "'", '"', "/", "*", "&", "~", "+"])
        self.vowels = set(["a", "e", "i", "o", "u", "y"])
        with open("stopwords.txt", "r") as f:
            self.stopwords = set([word.strip() for word in f.readlines()])
        self.translator = str.maketrans("", "", self.punctuation) #removes punctuation

        self.terms = queue()
        self.term_links = []
        self.term_info = []
        self.term_dict = {}
        self.insert_chunks = queue()
    

    def get_terms(self, content):
        content = content.encode("ascii", "ignore").decode()
        content = content.translate(self.translator)
        terms = self.term_finder.finditer(content)

        final_terms = []
        for match in terms:
            term = match.group()
            term = term.lower()

            if term in self.stopwords:
                continue

            length = len(term)
            if length <= 1 or length >= 30:                
                continue
            
            if length > 20:
                vowels = len([char for char in term if char in self.vowels])
                if vowels > 7 and vowels + 1 < length // 2:
                    continue

                numbers = len([char for char in term if char.isdecimal()])
                if numbers > 5 and numbers + 1 < length // 2:
                    continue
            final_terms.append(term)
            
        return final_terms
    

    async def worker(self, worker_num, session_maker):
        iteration = 0
        async with session_maker() as session:
            while self.insert_chunks.length() != 0:
                if iteration % 30 == 0:
                    await session.commit()
                try:
                    chunk = await self.insert_chunks.get()
                    await add_chunk(session, chunk)
                    self.insert_chunks.task_done()

                except asyncio.CancelledError:
                    await session.commit()
                    break
                except Exception as e:
                    print(e)

            await session.commit()
            
        print('Finished!')
    

    async def log_worker(self):
        try:
            while self.insert_chunks.length() > 0:
                await asyncio.sleep(1)
                print(f"{self.insert_chunks.length()} chunks remaining {10 * " "}", end="\r")
        except asyncio.CancelledError:
            pass


    async def run_indexer(self):
        session_maker = await connect_to_db(self.workers) 
        async with session_maker() as session:
            print("Getting pages...")

            await get_pages(session, self.pages_to_index)
            print("Getting page data...")

            i = 0
            for p in iter(self.pages_to_index):
                page = page_info(p[0], p[1])

                terms = self.get_terms(page.content)
                
                term_counts = Counter(terms)
                for term, count in term_counts.items():
                    self.term_dict.setdefault(term, []).append(page.id)

                i += 1
                if i % 50 == 0:
                    print(f"Processed {i} pages {" " * 10} \r", end="")          

            for term, pages in self.term_dict.items():
                t_length, p_length = len(term), len(pages)

                if p_length <= 10:
                    continue

                if (t_length < 4 or t_length > 15) and p_length < 20:
                    continue

                self.term_info.append([term, p_length])

            #contains term, and length of list of pages
            print("Adding terms")
            self.terms = [[item[0], item[1]] for item in self.term_info]
            term_ids = await get_term_ids(session, self.term_info)

            #creates list of inserts
            print("Creating term-page links")
            values = []
            for term, term_id in iter(term_ids):
                row = [{"term_id": term_id, "page_id": p_id} for p_id in self.term_dict[term]]
                values.extend(row)
            
            #chunk inserts
            chunk = []
            while values:
                length = len(values)
                chunk, values = values[:min(MAX_PARAMS, length)], values[min(MAX_PARAMS, length):]
                await self.insert_chunks.put(chunk)
                    
        print("Adding term-page links")       
        print(f"{self.insert_chunks.length()} total chunks") 
        workers = [asyncio.create_task(self.worker(worker_num, session_maker)) for worker_num in range(self.workers)]  

        try:
            workers += await asyncio.create_task(self.log_worker())
        except TypeError:
            pass

        await asyncio.gather(*workers)      

        t = ""
        while t != "(quit)":
            t = input("Enter term: ")
            os.system("cls")
            await retrieve_term_pages(session, t)           


async def main():
    workers = 30

    indexer = index_handler(workers)
    await indexer.run_indexer()

    
if __name__ == "__main__":
    asyncio.run(main())

