import os
from urllib.parse import quote_plus

import asyncpg
from asyncpg.exceptions import DeadlockDetectedError
import asyncio
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import (Column, ForeignKey, Integer, Table, exists, select,
                        update)
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER, TEXT, insert, asyncpg
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import (Mapped, backref, declarative_base, mapped_column,
                            relationship, selectinload, sessionmaker, load_only)
from sqlalchemy.sql import func

import time

Base = declarative_base()

page_links = Table(
    "page_outlinks",
    Base.metadata,
    Column("source_page_id", INTEGER, ForeignKey('pages.page_id'), primary_key=True),
    Column("outgoing_page_id", INTEGER, ForeignKey('pages.page_id'), primary_key=True),
    schema = 'public'
)


term_links = Table(
    "term_page_links",
    Base.metadata,
    Column("term_id", INTEGER, ForeignKey("terms.term_id"), primary_key=True),
    Column("page_id", INTEGER, ForeignKey("pages.page_id"), primary_key=True),
    schema = 'public'
    )


class Page(Base):
    __tablename__ = "pages"

    page_id: Mapped[int] = mapped_column(primary_key=True, index=True, unique=True)
    page_url: Mapped[str] = mapped_column(TEXT, unique=True)
    page_content: Mapped[str] = mapped_column(TEXT, nullable=True) 

    outlinks = relationship(
        "Page",
        secondary=page_links,
        primaryjoin=page_id == page_links.c.source_page_id,
        secondaryjoin=page_id == page_links.c.outgoing_page_id,
        back_populates="inlinks",
        lazy='selectin'
        )

    inlinks = relationship(
        "Page",
        secondary=page_links,
        primaryjoin=page_id == page_links.c.outgoing_page_id,
        secondaryjoin=page_id == page_links.c.source_page_id,
        back_populates="outlinks",
        lazy='selectin'
    )
    

class Term(Base):
    __tablename__ = "terms"

    term_id: Mapped[int] = mapped_column(primary_key=True, index=True, unique=True)
    term: Mapped[str] = mapped_column(TEXT, unique=True)
    total_pages: Mapped[int] = mapped_column()
    
    pages = relationship(
        "Page",
        secondary=term_links,
        lazy='selectin'
    )


async def connect_to_db(request_pool_size):
    '''Loads database and tables, returns a session object'''
    load_dotenv()
    user = os.getenv("USER")
    password = quote_plus(os.getenv("PASSWORD"))
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    dbname = os.getenv("DBNAME")

    url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_async_engine(url, pool_size=request_pool_size)

    Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    return Session


async def run_transaction_safely(session_maker, transaction_func, args):
    ''' Runs a given database transaction while catching and retrying on deadlocks and other errors'''
    async with session_maker() as session:
        for retry in range(5):
            try:
                return await transaction_func(session, *args)
            except DBAPIError as e:
                await session.rollback()
                if isinstance(e.orig, DeadlockDetectedError):
                    sleep_time = 0.2 * (2 ** retry)
                    print(f"Deadlock detected in {transaction_func}, retry # {retry + 1}, sleeping {sleep_time}")
                    await asyncio.sleep(sleep_time)
                else:
                    print(f"Exception in {transaction_func}: {e}")
                    raise
            except Exception as e:
                await session.rollback()
                print(f"Exception in {transaction_func}: {e}")
                raise
            
        await session.close()


# ----------------- FOR CRAWLER -----------------
class db_info:
    def __init__(self, url, content, outlinks):
        self.url = url
        self.content = content
        self.outlinks = outlinks


class database_handler:
    def __init__(self, database_queue):
        self.session_maker = None
        self.database_queue = database_queue
        self.being_added_to = True

        self.current_batch = []
        self.batch_mutate_lock = asyncio.Lock()
        self.max_params = 10000
        self.batch_size = 500

        self.added = 0


    def still_running(self):
        return self.being_added_to or not self.database_queue.empty() 


    async def connect_to_db(self, request_pool_size):
        '''Loads database and tables, returns a session object'''          
        self.session_maker = await connect_to_db(request_pool_size)
    

    async def worker(self):
        while self.still_running():
            try:
                async with asyncio.timeout(25):
                    page_info = db_info(*await self.database_queue.get())


                batch_to_add = None
                async with self.batch_mutate_lock:
                    self.current_batch.append(page_info)
                
                    if len(self.current_batch) >= self.batch_size:
                        batch_to_add = self.current_batch
                        self.current_batch = []
                
                if batch_to_add:
                    t = time.perf_counter()
                    with open("test.txt", "a") as f:
                        for item in batch_to_add:
                            f.write(item.url + "\n")
                    inserts = await add_batch(self.session_maker, batch_to_add, self.max_params)
                    self.added += inserts
                    print(f"\x1b[102mTook {time.perf_counter() - t} to add {inserts} pages to db. {self.added} total added. \x1b[0m")

            except asyncio.TimeoutError:
                print("Timed out! But it's handled.")
                break

            except asyncio.CancelledError:
                print("I was cancelled!")
                break

            except Exception as e:
                print("Exception in db worker", e)                   
            

        async with self.batch_mutate_lock:
            remaining = self.current_batch
            self.current_batch = []

        if remaining:
            l = await add_batch(self.session_maker, remaining, self.max_params)  
            self.added += l
            print(f"\x1b[102m Added a final batch to db of size {l} \x1b[0m")
        

        print(self.current_batch)
        print("Db worker exited")


async def add_batch(session_maker, page_batch, max_params):  
    # initial inserts work under assumption that the batch is under max params
    t = time.perf_counter()

    url_to_id = {}
    deduped = {}
    for item in page_batch:
        deduped[item.url] = item
    page_batch = list(deduped.values())
    urls = list(deduped.keys())

    chunk = sorted([{"page_url": item.url, "page_content": item.content} for item in page_batch], key=lambda x: x['page_url'])

    in_stmt = insert(Page).on_conflict_do_update(index_elements=["page_url"], set_={"page_content": insert(Page).excluded.page_content})
    inserted_pages = await run_transaction_safely(session_maker, transaction_func=insert_pages, args=[in_stmt, chunk, urls])
    total_sucessful_inserts = len(inserted_pages)

    for item in inserted_pages:
        url_to_id[item[1]] = item[0] 

    t1 = time.perf_counter()

    # add outlinks
    outlink_to_id = {}
    outlinks = []
    for item in page_batch:
        outlinks.extend(item.outlinks)
    outlinks = {outlink for outlink in outlinks}

    outlink_inserts = sorted([{"page_url": url} for url in outlinks], key=lambda x: x['page_url'])
    outlink_insert_statement = insert(Page).on_conflict_do_nothing()
    
    outlink_ids = []
    while outlink_inserts:
        length = min(max_params, len(outlink_inserts))
        chunk, outlink_inserts = outlink_inserts[:length], outlink_inserts[length:]
        chunk_outlinks = [item['page_url'] for item in chunk]

        result_ids = await run_transaction_safely(session_maker, transaction_func=insert_pages, args=[outlink_insert_statement, chunk, chunk_outlinks])
        if result_ids:
            outlink_ids.extend(result_ids)

    for item in outlink_ids:
        outlink_to_id[item[1]] = item[0]

    t2 = time.perf_counter()

    # add page <-> outlink links
    outlink_connection_inserts = []
    for item in page_batch:
        page_id = url_to_id[item.url]
        item_outlinks = item.outlinks

        inserts = [{"source_page_id": page_id, "outgoing_page_id" : outlink_to_id[outlink]} for outlink in item_outlinks]
        outlink_connection_inserts.extend(inserts)
    
    outlink_connection_inserts = sorted(outlink_connection_inserts, key = lambda x: x["source_page_id"])
    in_stmt = insert(page_links).on_conflict_do_nothing()
    while outlink_connection_inserts:
        length = min(max_params, len(outlink_connection_inserts))
        chunk, outlink_connection_inserts = outlink_connection_inserts[:length], outlink_connection_inserts[length:]
        j = time.perf_counter()
        await run_transaction_safely(session_maker, transaction_func=insert_links, args=[in_stmt, chunk])
        print(f"Tiok {time.perf_counter() - j}")

    t3 = time.perf_counter()
    total = t3 - t

    async with session_maker() as session:
        stmt = select(func.count()).select_from(Page).where(Page.page_content != None)
        result = await session.execute(stmt)

    print(f"Took {t1 - t} for initial inserts, {t2 - t1} for outlink inserts, and {t3 - t2} for relationship inserts")
    print(f"{(t1 - t) / total} {(t2 - t1) / total} {(t3 - t2) / total}")
    print(result.all())
    
    return total_sucessful_inserts


async def insert_pages(session, stmt, chunk, urls):
    pages = await session.execute(stmt, chunk)
    await session.commit()
    result = await session.execute(select(Page.page_id, Page.page_url).where(Page.page_url.in_(urls)))
    return result.all()


async def insert_links(session, stmt, chunk):
    await session.execute(stmt, chunk, execution_options={"postgresql_executemany": True})
    await session.commit()

# ----------------- FOR INDEXER  -----------------
async def insert_terms(session, in_stmt, chunk, terms):
    ''' Inserts terms and gets their ids '''
    await session.execute(in_stmt, chunk)
    result = await session.execute(select(Term.term, Term.term_id).where(Term.term.in_(terms)))
    await session.commit()
    return result.all()


async def insert_terms_safe(session_maker, term_info, max_params):
    ''' Gets a set of terms from a batch, inserts them into the db and updates their total_pages column, and returns their id '''
    ids = []
    chunk = []

    term_values =  sorted([{"term": term, "total_pages": 0} for term in term_info.keys()], key=lambda x: x["term"])
    in_stmt = insert(Term).on_conflict_do_nothing()

    while term_values:
        length = min(max_params, len(term_values))
        chunk, term_values = term_values[:length], term_values[length:]
        terms = [v["term"] for v in chunk]
        
        result_ids = await run_transaction_safely(session_maker, transaction_func=insert_terms, args=[in_stmt, chunk, terms])
        if result_ids:
            ids.extend(result_ids)
        else:
            print("Failed to get ids")

    return ids


async def add_chunk(session, chunk):
    ''' Add a chunk of term-page links '''
    stmt = insert(term_links).on_conflict_do_nothing(index_elements=[term_links.c.term_id, term_links.c.page_id])
    await session.execute(stmt, chunk, execution_options={"postgresql_executemany": True})
    await session.commit()


async def add_chunk_safe(session_maker, chunk):
    ''' Add a chunk of term-page links safely '''
    await run_transaction_safely(session_maker, transaction_func=add_chunk, args=[chunk])


async def get_pages(session, batch_size):    
    ''' Gets all availible pages in a stream and returns them as an async generator object '''
    stmt = select(Page).where(Page.page_content != None).execution_options(yield_per=batch_size)
    result = await session.stream_scalars(stmt)
    batch = []

    async for page in result:
        batch.append([page.page_id, page.page_content])

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch

    print("Got all pages")


async def set_term_counts(session_maker):
    ''' Sets the total_page column for all terms, ran after program has finished executing '''
    print("Updating total_pages")
    async with session_maker() as session:
        count_stmt = select(func.count()).select_from(term_links).where(term_links.c.term_id == Term.term_id).scalar_subquery()
        stmt = update(Term).values(total_pages=count_stmt)

        await session.execute(stmt)
        await session.commit()
    print("Updated total pages!")
            

# ----------------- FOR QUERY ENGINE -----------------
async def count_pages(session):
    stmt = select(func.count()).select_from(Page).where(Page.page_content != None)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_total_pages_for_terms(session, terms):
    set_terms = set(terms)
    stmt = select(Term.term, Term.total_pages).where(Term.term.in_(set_terms))
    result = await session.execute(stmt)
    
    return result.all()


async def retrieve_term_pages(session, term_str):
    stmt = select(Term).where(Term.term == term_str).options(selectinload(Term.pages))
    result = await session.execute(stmt)
    term = result.scalar_one_or_none()
    
    items = []
    if term:
        for page in term.pages:
            items.append((page.page_url, page.page_content))

    return items

 