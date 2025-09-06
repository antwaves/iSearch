import asyncio
import datetime
import os
from urllib.parse import quote_plus

import asyncpg
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import (Column, ForeignKey, Integer, Table, exists, select,
                        update)
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER, TEXT, insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import (Mapped, backref, declarative_base, mapped_column,
                            relationship, selectinload, sessionmaker)
from sqlalchemy.sql import func


Base = declarative_base()

page_links = Table(
    "links",
    Base.metadata,
    Column("target_page_id", INTEGER, ForeignKey('pages.page_id'), primary_key=True),
    Column("source_page_id", INTEGER, ForeignKey('pages.page_id'), primary_key=True),
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

    def __repr__(self) -> str:
        return f"{page_url}, with {len(outlinks)} outlinks and {len(inlinks)} inlinks"


class db_info:
    def __init__(self, url, content, outlinks):
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

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    return Session


async def get_page(session, page_url):
    check = select(Page).where(Page.page_url == page_url)
    result = await session.execute(check)
    return result.scalar()


async def create_page(session, link, content, outlinks): 
    try:
        stmt = insert(Page).values(page_url=link, page_content=content)
        stmt = stmt.on_conflict_do_update(index_elements=['page_url'], set_=dict(page_content=content))

        await session.execute(stmt) 

        outpages = []
        outpage_objects = []

        for page_url in outlinks:
            page_object = await get_page(session, page_url)

            if not page_object:
                outpages.append({"page_url" : page_url})
            else:
                outpage_objects.append(page_object)
                outlinks.remove(page_url)

        if outpages:
            stmt = insert(Page).on_conflict_do_nothing(index_elements=['page_url'])
            await session.execute(stmt, outpages)
        
        for page_url in outlinks:
            page_object = await get_page(session, page_url)
            if page_object:
                outpage_objects.append(page_object)
    
        page = await session.scalar(select(Page).where(Page.page_url == link).options(selectinload(Page.outlinks)))
    
        if page:
            page.outlinks = outpage_objects

        await session.commit()   

    except DBAPIError as e:
        if isinstance(e.orig, asyncpg.exceptions.DeadlockDetectedError):     
            print("Ran into deadlock")
            await asyncio.sleep(0.1)

    except Exception as e: 
        print(f"Page failed to be added with exception:", e)
        try:
            await session.rollback()
        except Exception as e:
            print("Failed to rollback with exception:", e)


async def db_worker(session_maker, db_queue):
    try:
        async with session_maker() as session:
            while True:
                try:
                    page_info = await db_queue.get()
                    await create_page(session, page_info.url, page_info.content, page_info.outlinks)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    print("Exception in db worker", e)
    except Exception as e:
        print("DB worker's session threw an exception with error:", e)
