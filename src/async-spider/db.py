import datetime
import os
from urllib.parse import quote_plus

import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import ARRAY, TEXT, insert
from sqlalchemy.orm import (Mapped, declarative_base, mapped_column,
                            sessionmaker)
from sqlalchemy.sql import func


def connect_to_db():
    '''Loads database and tables, returns a session object'''

    load_dotenv()
    user = os.getenv("USER")
    password = quote_plus(os.getenv("PASSWORD"))
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    dbname = os.getenv("DBNAME")

    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    engine = sa.create_engine(url)

    db = engine
    Sesssion = sessionmaker(bind=db)
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
        
    Base.metadata.create_all(db)
    session = Sesssion()

    return session


def create_page(session, link, content, outlinks):
    page_inlinks =  [row[0] for row in (session.query(Page.page_url).filter(sa.and_(Page.outlinks.contains([link]), Page.page_url != link)).all())]

    #print(page_inlinks)
    #existing_page = session.query(Page).filter(Page.page_url.contains(link)).first()

    ins = insert(Page).values(page_url=link, page_content=content, inlinks=page_inlinks, outlinks=outlinks). \
    on_conflict_do_update(index_elements=["page_url"], set_={"page_content":content, "inlinks":page_inlinks, "outlinks":outlinks})

    try:
        session.execute(ins)
        session.commit()
    except Exception as e: 
        session.rollback()
        session.flush()

        print(f"Page failed to be added with exception:", e)
    