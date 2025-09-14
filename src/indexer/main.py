import asyncio
import re
from collections import Counter


from util import queue
from db import connect_to_db, get_pages 

import random

class page_info:
    def __init__(self, id, content):
        self.id = id
        self.content = content

    def __repr__(self):
        return f"page id {self.id}"


class page_term_info:
    def __init__(self, id, occurences):
        self.id = id
        self.occurences = occurences
    
    def __repr__(self):
        return f"{self.id} with {self.occurences} occurences"


class index_handler:
    def __init__(self, workers):
        self.workers = workers
        self.index_queue = queue()

        self.term_splitter = re.compile(r" |(?<=[a-z])(?=[A-Z])|-|\n")
        self.punctuation = {".", "?", "!", ",", ":", ";", "—", "(", ")", "[", "]", "{", "}", "'", '"', "/", "*", "&", "~", "+"}
        with open("stopwords.txt", "r") as f:
            self.stopwords = [word.strip() for word in f.readlines()]

        self.term_dict = {}


    async def run_indexer(self):
        session_maker = await connect_to_db(self.workers) 
        async with session_maker() as session:
            await get_pages(session, self.index_queue)
            
        while self.index_queue.length() > 0:
            result = await self.index_queue.get()
            page = page_info(result[0], result[1])

            page.content = page.content.encode("ascii", "ignore").decode()
            page.content = ''.join(char if char not in self.punctuation else '' for char in page.content)
            terms = self.term_splitter.split(page.content)
            terms = [term.lower() for term in terms]
            terms = [term for term in terms if term not in self.stopwords and 0 < len(term) < 50]
            
            term_counts = Counter(terms)
            for term, count in term_counts.items():
                self.term_dict.setdefault(term, []).append(page_term_info(page.id, count))
    

async def main():
    workers = 30

    indexer = index_handler(workers)
    await indexer.run_indexer()

    query = input("Make query: ")

    query = query.encode("ascii", "ignore").decode()
    query = ''.join(char if char not in indexer.punctuation else '' for char in query)
    terms = indexer.term_splitter.split(query)
    terms = [term.lower() for term in terms]
    terms = [term for term in terms if term not in indexer.stopwords and 0 < len(term) < 50]

    print(terms)
    
if __name__ == "__main__":
    asyncio.run(main())