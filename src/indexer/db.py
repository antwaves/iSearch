import asyncio
import os
from urllib.parse import quote_plus

import asyncpg
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import (Column, ForeignKey, Integer, Table, exists, select,
                        update)
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER, TEXT, insert
from sqlalchemy.exc import DBAPIError, MissingGreenlet
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import (Mapped, backref, declarative_base, mapped_column,
                            relationship, selectinload, sessionmaker, load_only)
from sqlalchemy.sql import func


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
    term_count: Mapped[int] = mapped_column()
    
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


async def get_pages(session, queue):
    stmt = select(Page).where(Page.page_content != None)
    result = await session.scalars(stmt) 

    for page in result:
        await queue.put([page.page_id, page.page_content])


async def add_term(session, term_str: str, term_data):
    stmt = select(Term).where(Term.term == term_str).options(selectinload(Term.pages))
    result = await session.execute(stmt)
    term = result.scalar_one_or_none()

    if not term:
        term = Term(term=term_str, term_count=term_data.total_occurences)
        session.add(term)
        result = await session.execute(stmt)
        term = result.scalar_one_or_none()

    p_ids = [p.id for p in term_data.pages]
    stmt = select(Page).where(Page.page_id.in_(p_ids)).options(load_only(Page.page_id))
    result = await session.execute(stmt)
    pages = result.scalars().all()
    term.pages = pages
    

async def retrieve_term_pages(session, term_str):
    stmt = select(Term).where(Term.term == term_str).options(selectinload(Term.pages))
    result = await session.execute(stmt)
    term = result.scalar_one_or_none()
    
    if term:
        for page in term.pages:
            print(page.page_url)
