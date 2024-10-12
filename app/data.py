
import datetime as dt
from typing import Optional
import os
import pathlib
import sqlite3

import requests as rq
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, ForeignKey, String, Table, insert, select, text, update
from sqlalchemy.orm  import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    selectinload,
)

from app_session import SessionKey

#region DB engines

def get_sqlite_local_db_engine():
    # return sqlite3.connect("file::memory:?cache=shared", uri=True)
    return sqlite3.connect("local.database.db")

def get_sqlite_sporacle_db_engine():
    return sqlite3.connect("database.db")

def get_sqlalchemy_sporacle_engine():
    return sqlalchemy.create_engine("sqlite:///database.db")

#endregion

#region ORM
class Base(DeclarativeBase):
    pass

bet_lists__bet_list_odds_association = Table(
    'bet_lists__bet_list_odds',
    Base.metadata,
    Column('bet_list_name', String, ForeignKey('bet_lists.bet_list_name')),
    Column('odd_key', String, ForeignKey('bet_list_odds.key'))
)

class BetListOdd(Base):

    __tablename__ = "bet_list_odds"
    key: Mapped[str] = mapped_column(primary_key=True)
    odd_match_code: Mapped[int]
    odd_name: Mapped[str]
    odd_value: Mapped[float]
    odd_threshold: Mapped[Optional[float]]

    bet_lists: Mapped["BetList"] = relationship(
        secondary=bet_lists__bet_list_odds_association, 
        back_populates='odds',
        viewonly=True
    )
    # match: Mapped["BetListMatch"] = relationship(back_populates="odds")

class BetList(Base):

    __tablename__ = "bet_lists"

    bet_list_name: Mapped[str] = mapped_column(primary_key=True)
    creation_date: Mapped[dt.datetime] = mapped_column(default=dt.datetime.now())
    modification_date: Mapped[dt.datetime] = mapped_column(
        default=dt.datetime.now(),
        onupdate=dt.datetime.now()
    )
    odds: Mapped[list["BetListOdd"]] = relationship(
        secondary=bet_lists__bet_list_odds_association, 
        back_populates='bet_lists'
    )


def get_sqlalchemy_local_db_engine():
    return sqlalchemy.create_engine("sqlite:///local.database.db")

def create_all_tables_in_db():
    engine = get_sqlalchemy_local_db_engine()
    Base.metadata.create_all(engine)

#endregion

#region DB management
def init_database():
    create_all_tables_in_db()
    copy_odds_from_db_to_local_db()

def create_or_replace_local_db():
    local_db_path = pathlib.Path('local.database.db')
    if local_db_path.exists():
        os.remove(local_db_path)
    _ = get_sqlite_local_db_engine()
    init_database()


def attach_sporacle_db_to_local_db():
    engine = get_sqlite_local_db_engine()
    with engine.connect() as con:
        con.execute(text("ATTACH DATABASE 'database.db' AS 'odds_db';"))
        con.commit()
        con.execute()

def initial_app_setup(sporacle_bytes_data):
    with open("database.db", 'wb') as f:
        f.write(sporacle_bytes_data)
    _ = get_sqlite_sporacle_db_engine()
    create_or_replace_local_db()
    SessionKey.SPORACLE_DB_DOWNLOADED.update(True)
    SessionKey.LOCAL_DB_INITIALIZED.update(True)

def add_uploaded_db_file(uploaded_db_file):
    with open("local.database.db", 'wb') as f:
        f.write(uploaded_db_file.getvalue())

def copy_odds_from_db_to_local_db():
    local_engine = get_sqlalchemy_local_db_engine()
    with local_engine.connect() as con:

        con.execute(text("ATTACH DATABASE 'database.db' AS 'odds_db';"))
        con.commit()
        con.execute(
            text(
                """
                INSERT OR REPLACE INTO bet_list_odds(
                key, odd_match_code, odd_name, odd_value, odd_threshold
                ) SELECT key, odd_match_code, odd_name, odd_value, odd_threshold
                FROM odds_db.odds;
                """
            )
        )
        con.commit()
#endregion

#region Query utils
def get_table_from_query(engine, query):
    with engine.connect() as connection:
        query = connection.execute(
            sqlalchemy.text(query)
        )
        cols = query.keys()
        df = pd.DataFrame.from_records(
            data=query.fetchall(),
            columns=cols
        )
    return df

def get_table(table_name):
    engine = get_sqlalchemy_sporacle_engine()
    with engine.connect() as connection:
        query = connection.execute(
            sqlalchemy.text(f'select * from {table_name}')
        )
        cols = query.keys()
        df = pd.DataFrame.from_records(
            data=query.fetchall(),
            columns=cols
        )
    return df

def get_localdb_table(table_name):
    engine = get_sqlalchemy_local_db_engine()
    with engine.connect() as connection:
        query = connection.execute(
            sqlalchemy.text(f'select * from {table_name}')
        )
        cols = query.keys()
        df = pd.DataFrame.from_records(
            data=query.fetchall(),
            columns=cols
        )
    return df

def get_matches_table():
    return get_table("matches")

def get_odds_table():
    return get_table("odds")

def get_competitions():
    return get_table('competitions')

def get_saved_bet_lists():
    return get_localdb_table('bet_lists')

def get_odds_from_localdb():
    return get_localdb_table('bet_list_odds')

def get_program():
    final_cols = [
        'match_date', 'competition_name', 'description', 
        # 'home_team', 'away_team',
        '1', 'X', '2', 
        '1X', 'X2', '12', 
        '- 0.5 go.', '+ 0.5 go.', 
        '- 1.5 go.', '+ 1.5 go.',
        '- 2.5 go.', '+ 2.5 go.',
        '- 3.5 go.', '+ 3.5 go.', 
        '- 4.5 go.', '+ 4.5 go.',
        '- 5.5 go.', '+ 5.5 go.',
        'odd_match_code'
    ]
    df_raw_odds = get_odds_table()
    df_odds = clean_odds(df_raw_odds)
    df_raw_matches = get_matches_table()
    df_competitions = get_competitions()
    df_matches = (
        df_raw_matches.merge(
            df_competitions,
            on="competition_code",
            how="left",
            validate="m:1",
        )
        .rename(columns={"name":"competition_name"})
    )
    return (
        df_odds.merge(
            df_matches,
            on="odd_match_code",
            how="right",
            validate="1:m"
        )
        [final_cols]
    )

def get_bet_list_names_in_db():
    engine = get_sqlalchemy_local_db_engine()
    if sqlalchemy.inspect(engine).has_table(BetList.__tablename__):
        with Session(engine) as session:
            select_statement = select(BetList.bet_list_name)
            results = session.execute(select_statement).all()
        return [r[0] for r in results]
    else:
        return []
    
def get_matches_for_odds(match_codes):
    engine = get_sqlalchemy_sporacle_engine()
    match_codes_as_str = ', '.join(
        [f"'{code}'" for code in match_codes]
    )
    query = """
    select
        matches.odd_match_code,
        matches.description,
        matches.match_date as match_datetime,
        competitions.name as competition
    from matches
    left join competitions
        on matches.competition_code = competitions.competition_code
    where matches.odd_match_code in ({match_codes})
    """.format(
        match_codes=match_codes_as_str
    )
    df_matches = get_table_from_query(
        engine=engine, 
        query=query
    )
    return df_matches


def get_odds_for_bet_list(bet_list_name):
    engine = get_sqlalchemy_local_db_engine()
    with Session(engine) as session:
        with session.begin():
            bet_list_odds_select_statement = (
                select(BetList)
                .where(BetList.bet_list_name==bet_list_name)
                .options(selectinload(BetList.odds))
            )
            bet_list_obj = session.execute(bet_list_odds_select_statement).scalars().one()
            ls_odds = [odd.__dict__ for odd in bet_list_obj.odds]
    df = pd.DataFrame(ls_odds)
    return df


def get_bet_list_df(bet_list_name):
    final_cols = [
        'match_datetime', 'competition', 'description', 
        'odd_name', 'odd_value',
        'odd_threshold', 'odd_match_code',
        'key'
    ]
    df_odds = get_odds_for_bet_list(bet_list_name)
    df_matches = get_matches_for_odds(df_odds.odd_match_code.to_list())
    return (
        df_odds.merge(
            df_matches,
            on="odd_match_code",
            how="right",
            validate="1:m"
        )
        .astype(
            {"match_datetime":"datetime64[ns, UTC]"}
        )
        [final_cols]
    )

def get_selected_matches_for_bet_list(bet_list_name):
    engine = get_sqlalchemy_local_db_engine()
    select_statement = (
        select(BetListOdd.odd_match_code)
        .where(BetListOdd.bet_lists.has(bet_list_name=bet_list_name))
    )
    with Session(engine) as session:
        results = session.execute(select_statement).all()
        return [r[0] for r in results]

def get_existing_bet_list_summary(odd_keys):    
    odd_keys_as_str = ', '.join(
        [f"'{key}'" for key in odd_keys]
    )
    engine = get_sqlalchemy_sporacle_engine()
    query = """
    select
        matches.description,
        matches.match_date as match_datetime,
        competitions.name as competition,
        odds.key as key,
        odds.odd_name as odd_name,
        odds.odd_value as odd_value,
        odds.odd_threshold as odd_threshold,
        matches.odd_match_code
    from odds
    left join matches using (odd_match_code)
    left join competitions using (competition_code)
    where odds.key in ({odd_keys})
    """.format(odd_keys=odd_keys_as_str)
    df_summary = get_table_from_query(
        engine=engine, 
        query=query
    )
    return df_summary.to_dict(orient="records")
    # on odds.odd_match_code = matches.odd_match_code
    # on matches.competition_code = competitions.competition_code
#endregion

#region Data Cleaning
def clean_odds(df):
    ordered_columns_ls = [
        'odd_match_code',
        'competition_code',
        '1', 'X', '2',
        '1X', 'X2', '12',
        '- 0.5 go.', '+ 0.5 go.', '- 1.5 go.', '+ 1.5 go.',
        '- 2.5 go.', '+ 2.5 go.', '- 3.5 go.', '+ 3.5 go.',
        '- 4.5 go.', '+ 4.5 go.', '- 5.5 go.', '+ 5.5 go.'
    ]
    return (
        df
        .pivot(
            values="odd_value",
            columns="odd_name",
            index="odd_match_code"
        )
        .reset_index(drop=False)
        .rename_axis(None, axis=1)
        .reindex(ordered_columns_ls,axis=1)
    )
#endregion

#region CUD        
def upsert_bet_list(bet_list_name, bet_list_odds):
    engine = get_sqlalchemy_local_db_engine()
    odd_keys = [odd_dict["key"] for odd_dict in bet_list_odds]
    with Session(engine) as session: 
        with session.begin():
            # Fetch existing bet list object if exists
            existing_bet_list_statement = (
                select(BetList)
                .where(BetList.bet_list_name == bet_list_name)
                
            )
            bet_list_exists = session.query(
                existing_bet_list_statement.exists()
            ).scalar()
            # Fetch corresponding odds objects
            odds_select_statement = (
                select(BetListOdd)
                .where(BetListOdd.key.in_(odd_keys))
            )
            odds_results = session.scalars(odds_select_statement).all()            
            if bet_list_exists:
                existing_bet_list = session.scalar(existing_bet_list_statement)
                existing_bet_list.odds.clear()
                existing_bet_list.odds.extend(odds_results)
                session.add(existing_bet_list)
            else:
                bet_list_obj = BetList(bet_list_name=bet_list_name)
                bet_list_obj.odds.extend(odds_results)
                session.add(bet_list_obj)
def drop_bet_list(bet_list_name):
    engine = get_sqlalchemy_local_db_engine()
    with Session(engine) as session: 
        with session.begin():
            # Fetch existing bet list object if exists
            existing_bet_list_statement = (
                select(BetList)
                .where(BetList.bet_list_name == bet_list_name)
                
            )
            bet_list = session.scalar(existing_bet_list_statement)
            session.delete(bet_list)

#endregion