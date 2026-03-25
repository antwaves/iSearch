import asyncio
import time
import re
from io import StringIO
from collections import Counter
import traceback
import os

from queues import queue
from db import connect_to_db, get_pages, get_term_ids, add_chunk



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

        self.term_finder = re.compile(r"[A-Za-z0-9_-]++")
        self.vowels = set(["a", "e", "i", "o", "u", "y"])
        with open("stopwords.txt", "r") as f:
            self.stopwords = set([word.strip() for word in f.readlines()])

        self.punctuation = "".join([".", "?", "!", ",", ":", ";", "—", "(", ")", "[", "]", "{", "}", "'", '"', "/", "*", "&", "~", "+"])
        self.translator = str.maketrans("", "", self.punctuation) #removes punctuation

        #what the fuck?
        self.terms = queue()
        self.insert_chunks = queue()


    async def run_indexer(self):
        session_maker = await connect_to_db(self.workers) 

        async with session_maker() as session:
            print("Getting pages...")

            pages = await get_pages(session) # all pages with any content in the db
            print("Getting page data...")

            t = time.time()
            term_pages = {} #key = term, value = pages that contain term
            for p in pages:
                page = page_info(p[0], p[1])
                terms = set(self.get_terms(page.content))
                for term in terms:
                    term_pages.setdefault(term, []).append(page.id)
            e = time.time()
            print(f"{e - t} seconds. {(e-t) / len(pages)} seconds per page")
            
            await asyncio.sleep(5)

            term_page_frequency = {} #key = term, value = how many pages a term appears on
            for term, pages_containing_term in term_pages.items(): 
                t_length, amount_of_pages = len(term), len(pages_containing_term)

                frequent_term = amount_of_pages > 20
                valid_length_term = t_length > 3 and t_length < 15

                if valid_length_term or frequent_term:
                    term_page_frequency[term] = amount_of_pages
                    continue


            print("Adding terms")
            term_ids = await get_term_ids(session, term_page_frequency, MAX_PARAMS) #contains term, and length of list of pages

            print("Creating a list of database inserts")
            values = []
            for obj in term_ids:
                term, term_id = obj[0], obj[1]
                row = [{"term_id": term_id, "page_id": p_id} for p_id in term_pages[term]]
                values.extend(row)
            
            chunk = []
            while values:
                length = len(values)
                chunk, values = values[:min(MAX_PARAMS, length)], values[min(MAX_PARAMS, length):]
                await self.insert_chunks.put(chunk)


        print("Adding term-page links")       
        print(f"{self.insert_chunks.length()} total chunks") 
        workers = [asyncio.create_task(self.worker(worker_num, session_maker)) for worker_num in range(self.workers)]  
        workers.append(asyncio.create_task(self.log_worker()))
        try:
            await asyncio.gather(*workers)      
        except asyncio.exceptions.CancelledError:
            pass
        
        print("All done!")


    async def worker(self, worker_num, session_maker):
        try:
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
                        await session.rollback()
                        print(traceback.format_exc())

                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()

        except Exception as e:
            print(traceback.format_exc())

                
        print('Finished!')


    def get_terms(self, content):
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
                vowels = len([char for char in term if char in self.vowels])
                if vowels > 7 and vowels + 1 < length // 2:
                    continue

                numbers = len([char for char in term if char.isdecimal()])
                if numbers > 5 and numbers + 1 < length // 2:
                    continue

            final_terms.append(term)

        return final_terms
    

    async def log_worker(self):
        while self.insert_chunks.length() > 0:
            await asyncio.sleep(0.25)
            print(f"{self.insert_chunks.length()} chunks remaining {10 * " "}", end="\r")

    
#what the fuck
async def main():
    workers = 30

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
