import datetime
import os
from urllib.parse import quote_plus

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import ARRAY, TEXT, insert
from sqlalchemy.orm import (Mapped, declarative_base, mapped_column,
                            sessionmaker)
from sqlalchemy.sql import func

#asyncpg too

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

    global Page

    class Page(Base):
        __tablename__ = "pages"

        page_id: Mapped[int] = mapped_column(primary_key=True, index=True, unique=True)
        page_url: Mapped[str] = mapped_column(TEXT, unique=True)
        page_content: Mapped[str] = mapped_column(TEXT, nullable=True) 
        inlinks: Mapped[list[str]] = mapped_column(ARRAY(TEXT))
        outlinks: Mapped[list[str]] = mapped_column(ARRAY(TEXT), nullable=True)

        def __repr__(self) -> str:
            return f"{self.page_url}"

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    return Session


async def create_page(session, link, content, outlinks):
    result =  await session.execute(select(Page.page_url).where(Page.outlinks.contains([link])))
    page_inlinks = [row[0] for row in result.fetchall()]

    ins = insert(Page).values(page_url=link, page_content=content, inlinks=page_inlinks, outlinks=outlinks). \
    on_conflict_do_update(index_elements=["page_url"], set_={"page_content":content, "inlinks":page_inlinks, "outlinks":outlinks})

    try:
        await session.execute(ins)
    except Exception as e: 
        await session.rollback()
        print(f"Page failed to be added with exception:", e)
        return e
    