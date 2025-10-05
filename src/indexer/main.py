import asyncio
import re
from collections import Counter


from util import queue, page_info, page_term_frequency, term_data
from db import connect_to_db, get_pages, add_term

       
class index_handler:
    def __init__(self, workers):
        self.workers = workers
        self.index_queue = queue()

        self.term_splitter = re.compile(r" |(?<=[a-z])(?=[A-Z])|-|\n")
        self.punctuation = {".", "?", "!", ",", ":", ";", "—", "(", ")", "[", "]", "{", "}", "'", '"', "/", "*", "&", "~", "+"}
        with open("stopwords.txt", "r") as f:
            self.stopwords = [word.strip() for word in f.readlines()]
        
        self.term_queue = queue()

        self.term_dict = {}
    

    def get_terms(self, content):
        content = content.encode("ascii", "ignore").decode()
        content = ''.join(char if char not in self.punctuation else '' for char in content)
        terms = self.term_splitter.split(content)
        terms = [term.replace(" ", "").lower() for term in terms]
        terms = [term for term in terms if term not in self.stopwords and 0 < len(term) < 50]

        return terms
    

    async def worker(self, worker_num, session_maker):
        async with session_maker() as session:
            while self.term_queue.length() != 0:
                try:
                    obj = await self.term_queue.get()
                    term, data = obj[0], obj[1]

                    print(f"{worker_num}, {term}")
                    await add_term(session, term, data)
                    self.term_queue.task_done()
                except asyncio.CancelledError:
                    break
            else:
                print('Finished!')


    async def run_indexer(self):
        session_maker = await connect_to_db(self.workers) 
        async with session_maker() as session:
            await get_pages(session, self.index_queue)
                
            while self.index_queue.length() > 0:
                result = await self.index_queue.get()
                page = page_info(result[0], result[1])

                terms = self.get_terms(page.content)
                
                term_counts = Counter(terms)
                for term, count in term_counts.items():
                    self.term_dict.setdefault(term, term_data()).add(page_term_frequency(page.id, count))
            
            for term, data in self.term_dict.items():
                await self.term_queue.put([term, data])
        
        workers = [asyncio.create_task(self.worker(worker_num, session_maker)) for worker_num in range(self.workers)]  
        await asyncio.gather(*workers)                 
        print('DONE!!!!')    


async def main():
    workers = 30

    indexer = index_handler(workers)
    await indexer.run_indexer()

    
if __name__ == "__main__":
    asyncio.run(main())