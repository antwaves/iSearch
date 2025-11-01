import asyncio
import os
from urllib.parse import quote_plus

import asyncpg
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import (Column, ForeignKey, Integer, Table, exists, select,
                        update)
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER, TEXT, insert, asyncpg
from sqlalchemy.exc import DBAPIError, MissingGreenlet
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import (Mapped, backref, declarative_base, mapped_column,
                            relationship, selectinload, sessionmaker, load_only)
from sqlalchemy.sql import func

import time


MAX_PARAMS = 15000
Base = declarative_base()


page_links = Table(
    "links",
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


async def get_pages(session, pages):
    stmt = select(Page).where(Page.page_content != None)
    result = await session.scalars(stmt) 

    i = 0
    for page in result:
        i += 1
        if i % 100 == 0:
            print(f"Got {i} pages {" " * 10} \r", end="")
        pages.append([page.page_id, page.page_content])


async def get_term_ids(session, term_info):
    ids = []
    chunk, values = [], [{"term": item[0], "total_pages": item[1]} for item in term_info]

    while values:
        length = len(values)
        chunk, values = values[:min(MAX_PARAMS, length)], values[min(MAX_PARAMS, length):]

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
