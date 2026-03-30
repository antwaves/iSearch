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

Base = declarative_base()

page_links = Table(
    "page_outlinks",
    Base.metadata,
    Column("target_page_id", INTEGER, ForeignKey('pages.page_id'), primary_key=True),
    Column("source_page_id", INTEGER, ForeignKey('pages.page_id'), primary_key=True),
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
        secondaryjoin=page_id == page_links.c.target_page_id,
        back_populates="inlinks",
        lazy='selectin'
        )

    inlinks = relationship(
        "Page",
        secondary=page_links,
        primaryjoin=page_id == page_links.c.target_page_id,
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


    def still_running(self):
        return self.being_added_to or not self.database_queue.empty()


    async def connect_to_db(self, request_pool_size):
        '''Loads database and tables, returns a session object'''          
        self.session_maker = await connect_to_db(request_pool_size)

    
    async def worker(self):
        try:
            async with self.session_maker() as session:
                while self.still_running():
                    try:
                        page_info = db_info( *await self.database_queue.get())
                        await create_page(session, page_info)

                    except asyncio.CancelledError:
                        await session.commit()
                        break

                    except Exception as e:
                        print("Exception in db worker", e)
        except Exception as e:
            print("DB worker's session threw an exception with error:", e)


# TODO: add deadlock retry and deal with too many params
async def create_page(session: AsyncSession, page_info):
    link = page_info.url
    content = page_info.content
    outlinks = sorted(set(page_info.outlinks))

    
    print("attempt ", link)
    try:
        stmt = (insert(Page).values(page_url=link, page_content=content)
            .on_conflict_do_update(index_elements=["page_url"], set_={"page_content": insert(Page).excluded.page_content})
            .returning(Page.page_id)
        )
        main_link_id = await session.execute(stmt)
        main_link_id = main_link_id.scalar_one()

        await session.commit()

        if outlinks:
            stmt = (insert(Page).values([{"page_url" : link} for link in outlinks])
                .on_conflict_do_nothing(index_elements=["page_url"]))
            await session.execute(stmt)
            await session.commit()

            stmt = select(Page.page_id).where(Page.page_url.in_(outlinks))
            page_ids = await session.execute(stmt)
            page_ids = page_ids.scalars()

            stmt = (insert(page_links).values([{"target_page_id": main_link_id, "source_page_id": out_id} for out_id in page_ids])
                    .on_conflict_do_nothing())   
            await session.execute(stmt)
            
        await session.commit()

    except Exception as e:
        print(f"Error adding {link}: {e}")
        await session.rollback()
        silent_log(e, "create_page", [link, outlinks])
        await asyncio.sleep(2)


# ----------------- FOR INDEXER  -----------------
async def insert_terms(session, in_stmt, chunk, terms):
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
        
        async with session_maker() as session:
            for retry in range(5):
                try:
                    result_ids = await insert_terms(session, in_stmt, chunk, terms)
                    break

                except DBAPIError as e:
                    session.rollback()
                    if isinstance(e.orig, DeadlockDetectedError):
                        sleep_time = 0.2 * (2 ** retry)
                        print(f"Deadlock detected in insert_terms, retry {retry+1}, sleeping {sleep_time}")
                        await asyncio.sleep(sleep_time)
                    else:
                        print(f"Exception in insert_terms {e}")
                        raise

                except Exception as e:
                    print(f"Exception in insert_terms {e}")
                    await session.rollback()
                    raise
            
            await session.close()

        if result_ids:
            ids.extend(result_ids)

    return ids


async def add_chunk(session, chunk, retry = 0):
    stmt = insert(term_links).on_conflict_do_nothing(index_elements=[term_links.c.term_id, term_links.c.page_id])
    await session.execute(stmt, chunk, execution_options={"postgresql_executemany": True})
    await session.commit()


async def add_chunk_safe(session_maker, chunk):
    async with session_maker() as session:
        for retry in range(5):
            try:
                await add_chunk(session, chunk)
                await session.close()
                return

            except DBAPIError as e:
                await session.rollback()

                if isinstance(e.orig, DeadlockDetectedError):
                    sleep_time = 0.2 * (2 ** retry)
                    print(f"Deadlock detected, retry {retry+1}, sleeping {sleep_time}")
                    await asyncio.sleep(sleep_time)
                else:
                    print(f"Exception in add_chunk {e}")
                    raise

            except Exception as e:
                print(f"Exception in add_chunk {e}")
                await session.rollback()
                raise


async def get_pages(session, batch_size):    
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
    print("Updating total_pages")
    async with session_maker() as session:
        count_stmt = select(func.count()).select_from(term_links).where(term_links.c.term_id == Term.term_id).scalar_subquery()
        stmt = update(Term).values(total_pages=count_stmt)

        await session.execute(stmt)
        await session.commit()
    print("Updated total pages!")
            

# ----------------- FOR QUERY ENGINE -----------------
async def retrieve_term_pages(session, term_str):
    stmt = select(Term).where(Term.term == term_str).options(selectinload(Term.pages))
    result = await session.execute(stmt)
    term = result.scalar_one_or_none()
    
    if term:
        for page in term.pages:
            print(page.page_url)