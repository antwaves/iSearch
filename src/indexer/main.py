import asyncio
import re
from io import StringIO
from collections import Counter
import os


from util import queue, page_info, page_term_frequency, term_data
from db import connect_to_db, get_pages, add_term, retrieve_term_pages

       
class index_handler:
    def __init__(self, workers):
        self.workers = workers
        self.index_queue = queue()

        self.term_splitter = re.compile(r" |(?<=[a-z])(?=[A-Z])|-|\n")
        self.search = re.compile(r'\d')
        self.punctuation = "".join([".", "?", "!", ",", ":", ";", "—", "(", ")", "[", "]", "{", "}", "'", '"', "/", "*", "&", "~", "+"])
        with open("stopwords.txt", "r") as f:
            self.stopwords = set([word.strip() for word in f.readlines()])
        self.translator = str.maketrans("", "", self.punctuation) #removes punctuation

        self.term_queue = queue()
        self.term_dict = {}
    

    def get_terms(self, content):
        content = content.encode("ascii", "ignore").decode()
        content = content.translate(self.translator)
        terms = self.term_splitter.split(content)
        terms = [term.replace(" ", "").lower() for term in terms if term.lower() not 
            in self.stopwords and 1 < len(term) < 50 and not bool(re.search(self.search, term))]

        return terms
    

    async def worker(self, worker_num, session_maker):
        iteration = 0

        async with session_maker() as session:
            while self.term_queue.length() != 0:
                iteration += 1
                if iteration == 50:
                    await session.commit() 

                try:
                    obj = await self.term_queue.get()
                    term, data = obj[0], obj[1]
                    if term == "human":
                        print(obj)

                    await add_term(session, term, data)
                    self.term_queue.task_done()
                except asyncio.CancelledError:
                    await session.commit()
                    break
                except Exception as e:
                    print(e)

            await session.commit()
        print('Finished!')
    

    async def log_worker(self):
        while self.term_queue.length() > 0:
            await asyncio.sleep(0.5)
            print(f"{self.term_queue.length()} terms remaining {10 * " "}")


    async def run_indexer(self):
        session_maker = await connect_to_db(self.workers) 
        async with session_maker() as session:
            print("Getting pages...")
            await get_pages(session, self.index_queue)
                
            print("Getting page data...")
            while self.index_queue.length() > 0:
                result = await self.index_queue.get()
                page = page_info(result[0], result[1])

                terms = self.get_terms(page.content)
                
                term_counts = Counter(terms)
                for term, count in term_counts.items():
                    self.term_dict.setdefault(term, term_data()).add(page_term_frequency(page.id, count))
            
            for term, data in self.term_dict.items():
                await self.term_queue.put([term, data])
            
            
            print("Sorting terms...")        
            workers = [asyncio.create_task(self.worker(worker_num, session_maker)) for worker_num in range(self.workers)]  

            try:
                workers += await asyncio.create_task(self.log_worker())
            except TypeError:
                pass

            await asyncio.gather(*workers)      

            while True:
                t = input("Enter term: ")
                os.system("cls")
                await retrieve_term_pages(session, t)           


async def main():
    workers = 30

    indexer = index_handler(workers)
    await indexer.run_indexer()


    
if __name__ == "__main__":
    asyncio.run(main())


# page url page id page content



# term term id pages 

# python python.com python-tutorial.com