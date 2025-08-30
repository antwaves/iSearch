import datetime
import asyncio
import os
from urllib.parse import quote_plus

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from dotenv import load_dotenv
from sqlalchemy import select, Table, Column, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY, TEXT, INTEGER, insert
from sqlalchemy.orm import (Mapped, declarative_base, mapped_column,
                            sessionmaker, relationship, backref)
from sqlalchemy.sql import func
#asyncpg too


class db_info:
    def __init__(self, db_session, url, content, outlinks):
        self.db_session = db_session
        self.url = url
        self.content = content
        self.outlinks = outlinks


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
    Base = declarative_base()

    page_links = Table(
        "links",
        Base.metadata,
        Column('link_id', INTEGER, primary_key=True),
        Column("target_page_id", INTEGER, ForeignKey('pages.page_id')),
        Column("source_page_id", INTEGER, ForeignKey('pages.page_id')),
        schema = 'public'
    )

    global Page
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
         )

        inlinks = relationship(
            "Page",
            secondary=page_links,
            primaryjoin=page_id == page_links.c.target_page_id,
            secondaryjoin=page_id == page_links.c.source_page_id,
            back_populates="outlinks",
        )

        def __repr__(self) -> str:
            return f"{self.page_url}"

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    return Session


async def create_page(session, link, content, outlinks): 
    outlink_pages = []
    #try:
    with session.no_autoflush:
        for url in set(outlinks):
            result = await session.execute(select(Page).where(Page.page_url == url and Page.page_url != link))

            await asyncio.sleep(20)
            outlink_page = result.scalar_one_or_none()

            if not outlink_page:
                outlink_page = Page(page_url=url)
                session.add(outlink_page)

            outlink_pages.append(outlink_page)
        
        await session.commit()

        result = await session.execute(select(Page).where(Page.page_url == link))
        page = result.scalar_one_or_none()

        if page:
            page.page_content = content
            page.outlinks = outlink_pages
        else:
            page = Page(page_url=link, page_content=content, outlinks=outlink_pages)
            session.add(page)

        await session.commit()
        
    # except Exception as e: 
    #     await session.rollback()
    #     print(f"Page failed to be added with exception:", e)
    #     return e



async def db_worker(db_queue):
    while True:
        try:
            page_info = await db_queue.get()
            await create_page(page_info.db_session, page_info.url, page_info.content, page_info.outlinks)
        except asyncio.CancelledError:
            break
        