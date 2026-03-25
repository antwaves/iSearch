import os
from urllib.parse import quote_plus

import asyncpg
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
async def get_term_ids(session, term_info, max_params):
    ids = []
    chunk, values = [], [{"term": item[0], "total_pages": item[1]} for item in term_info.items()]

    while values:
        length = min(max_params, len(values))
        chunk, values = values[:length], values[length:]

        term_insert = insert(Term).values(chunk)
        term_insert = term_insert.on_conflict_do_update(index_elements=[Term.term], set_={"total_pages": term_insert.excluded.total_pages})
        term_insert = term_insert.returning(Term.term, Term.term_id)
        result = await session.execute(term_insert)

        ids.extend(result.all())
        await session.commit()

    return ids


async def add_chunk(session, chunk):
    try:
        chunk_insert = insert(term_links).values(chunk)
        chunk_insert = chunk_insert.on_conflict_do_nothing(index_elements=[term_links.c.term_id, term_links.c.page_id])
        await session.execute(chunk_insert)

    except Exception as e:
        print(f"Exception in add chunk")
        with open("log.txt", "a") as f:
            f.write(f"add_chunk threw an error: {e}\n")


async def retrieve_term_pages(session, term_str):
    stmt = select(Term).where(Term.term == term_str).options(selectinload(Term.pages))
    result = await session.execute(stmt)
    term = result.scalar_one_or_none()
    
    if term:
        for page in term.pages:
            print(page.page_url)


# ----------------- FOR QUERY ENGINE -----------------

async def get_pages(session):
    pages = []

    stmt = select(Page).where(Page.page_content != None)
    result = await session.scalars(stmt) 

    i = 0
    for page in result:
        i += 1
        if i % 100 == 0:
            print(f"Got {i} pages {" " * 10} \r", end="")
        pages.append([page.page_id, page.page_content])
    return pages