import asyncio
import os
from urllib.parse import quote_plus

import asyncpg
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import Column, ForeignKey, Integer, Table, update, select
from sqlalchemy.dialects.postgresql import INTEGER, TEXT, insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import (Mapped, declarative_base, mapped_column,
                            relationship, selectinload, sessionmaker)
from sqlalchemy.sql import func

from util import silent_log

Base = declarative_base()

page_links = Table(
    "page_outlinks",
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
        return f"{self.page_url}"


class db_info:
    def __init__(self, url, content, outlinks):
        self.url = url
        self.content = content
        self.outlinks = outlinks


class database_handler:
    def __init__(self, database_queue, log):
        self.session_maker = None
        self.database_queue = database_queue
        self.cancelled = False
        self.log = log


    async def connect_to_db(self, request_pool_size):
        '''Loads database and tables, returns a session object'''

        load_dotenv()
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        host = os.getenv("HOST")
        port = os.getenv("PORT")
        dbname = os.getenv("DBNAME")

        url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_async_engine(url, pool_size=request_pool_size)

        Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
        self.session_maker = Session

    
    async def worker(self):
        try:
            async with self.session_maker() as session:
                while not self.cancelled:
                    try:
                        page_info = await self.database_queue.get()
                        await create_page(session, page_info)

                        self.log.inc(added=True)
                        self.log.update(added=page_info.url)

                    except asyncio.CancelledError:
                        break

                    except Exception as e:
                        print("Exception in db worker", e)

        except Exception as e:
            print("DB worker's session threw an exception with error:", e)


#add deadlock retry and deal with too many params
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
