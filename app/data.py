
import datetime as dt
from typing import Optional
import os
import pathlib
import sqlite3

import requests as rq
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, MetaData, event
from sqlalchemy import ForeignKey, select, text, JSON
from sqlalchemy.orm  import (
    declarative_base,
    Mapped,
    Session,
    sessionmaker,
    mapped_column,
    relationship,
)

from app_session import SessionKey

#region DB engines

IN_MEMORY_SQLALCHEMY_DB_ENGINE = create_engine('sqlite://')

def get_sqlite_local_db_engine():
    # return sqlite3.connect("file::memory:?cache=shared", uri=True)
    return sqlite3.connect("local.database.db")

def get_sqlalchemy_local_db_engine():
    return sqlalchemy.create_engine("sqlite:///local.database.db")

def get_sqlite_sporacle_db_engine():
    return sqlite3.connect("database.db")

def get_sqlalchemy_sporacle_engine():
    return sqlalchemy.create_engine("sqlite:///database.db")

def get_sqlalchemy_cross_database_engine():
    engine = create_engine('sqlite://')
    with engine.connect() as connection:
        _ = connection.execute(text("attach 'database.db' as sporacle_db;"))
        _ = connection.execute(text("attach 'local.database.db' as app_db;"))
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

@event.listens_for(IN_MEMORY_SQLALCHEMY_DB_ENGINE, "connect", insert=True)
def set_current_schema(dbapi_connection, connection_record):
    cursor_obj = dbapi_connection.cursor()
    cursor_obj.execute("attach 'database.db' as sporacle_db;")
    cursor_obj.execute("attach 'local.database.db' as app_db;")
    cursor_obj.close()


#endregion

#region ORM

Base = declarative_base()


class BetList(Base):

    __tablename__ = "bet_lists"
    __table_args__ = {"schema": "app_db"}


    bet_list_name: Mapped[str] = mapped_column(primary_key=True)
    creation_date: Mapped[dt.datetime] = mapped_column(default=dt.datetime.now())
    modification_date: Mapped[dt.datetime] = mapped_column(
        default=dt.datetime.now(),
        onupdate=dt.datetime.now()
    )
    odds: Mapped[JSON] = mapped_column(type_=JSON, nullable=False)
    earliest_match_date: Mapped[Optional[dt.datetime]]
    last_match_date: Mapped[Optional[dt.datetime]]


class Odd(Base):
    __tablename__ = "odds"
    __table_args__ = {"schema": "sporacle_db"}

    key: Mapped[str] = mapped_column(primary_key=True)
    odd_name: Mapped[str]
    odd_value: Mapped[float]
    odd_threshold: Mapped[Optional[float]] = mapped_column()
    is_winning: Mapped[Optional[bool]]

    odd_match_code: Mapped[int] = mapped_column(
        ForeignKey("sporacle_db.matches.odd_match_code")
    )

    match: Mapped["Match"] = relationship(back_populates="odds")


class Match(Base):
    __tablename__ = "matches"
    __table_args__ = {"schema": "sporacle_db"}

    odd_match_code: Mapped[int] = mapped_column(primary_key=True)
    competition_code: Mapped[int] = mapped_column(
        ForeignKey("sporacle_db.competitions.competition_code")
    )
    match_date: Mapped[dt.datetime]
    description: Mapped[str]
    sport_radar_match_code: Mapped[Optional[int]]
    home_team: Mapped[str]
    away_team: Mapped[str]
    half_time_home_goals: Mapped[Optional[int]]
    half_time_away_goals: Mapped[Optional[int]]
    full_time_home_goals: Mapped[Optional[int]]
    full_time_away_goals: Mapped[Optional[int]]

    odds: Mapped[list["Odd"]] = relationship(
        back_populates="match", cascade="all, delete-orphan"
    )

    competition: Mapped["Competition"] = relationship(back_populates="matches")


class Competition(Base):
    __tablename__ = "competitions"
    __table_args__ = {"schema": "sporacle_db"}

    competition_code: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    is_top_competition: Mapped[bool]

    matches: Mapped[list["Match"]] = relationship(
        back_populates="competition", cascade="all, delete-orphan"
    )


def create_all_tables():
    Base.metadata.create_all(IN_MEMORY_SQLALCHEMY_DB_ENGINE)


#endregion

#region DB management
def init_database():
    create_all_tables()

def create_or_replace_local_db():
    local_db_path = pathlib.Path('local.database.db')
    if local_db_path.exists():
        os.remove(local_db_path)
    _ = get_sqlite_local_db_engine()


def attach_sporacle_db_to_local_db():
    engine = get_sqlalchemy_local_db_engine()
    with engine.connect() as con:
        con.execute(text("ATTACH DATABASE 'database.db' AS 'odds_db';"))
        con.commit()

def initial_app_setup(sporacle_bytes_data):
    with open("database.db", 'wb') as f:
        f.write(sporacle_bytes_data)
    _ = get_sqlite_sporacle_db_engine()
    create_or_replace_local_db()
    create_all_tables()
    SessionKey.SPORACLE_DB_DOWNLOADED.update(True)
    SessionKey.LOCAL_DB_INITIALIZED.update(True)

def add_uploaded_db_file(uploaded_db_file):
    with open("local.database.db", 'wb') as f:
        f.write(uploaded_db_file.getvalue())

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

def get_on_going_bet_lists():
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    query = """
    SELECT * FROM app_db.bet_lists as bet_lists
    WHERE bet_lists.last_match_date > DATE('now')
    """
    df_on_going_bet_lists = get_table_from_query(
        engine=engine, 
        query=query
    )
    return df_on_going_bet_lists

def get_future_odds():
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    query = """
    SELECT
        odds.*
    FROM sporacle_db.odds as odds 
    LEFT JOIN sporacle_db.matches as matches USING (odd_match_code)
    WHERE matches.match_date > DATE('now')
    """
    df_future_odds = get_table_from_query(
        engine=engine, 
        query=query
    )
    return df_future_odds

def get_future_matches():
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    query = """
    SELECT
        matches.odd_match_code as odd_match_code,
        matches.description as description,
        matches.match_date as match_date,
        competitions.name as competition,
        matches.competition_code as competition_code
    FROM sporacle_db.matches as matches
    LEFT JOIN sporacle_db.competitions as competitions USING (competition_code)
    WHERE matches.match_date > DATE('now')
    """
    df_future_matches = get_table_from_query(
        engine=engine, 
        query=query
    )
    return (
        df_future_matches
        .astype({"match_date":"datetime64[ns, UTC]"})
    )


def get_program():
    final_cols = [
        'match_date', 'competition', 'description', 
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
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    query = """
    SELECT
        matches.odd_match_code as odd_match_code,
        matches.description as description,
        matches.match_date as match_date,
        competitions.name as competition,
        matches.competition_code as competition_code
    FROM sporacle_db.matches as matches
    LEFT JOIN sporacle_db.competitions as competitions USING (competition_code)
    WHERE matches.match_date > DATE('now')
    """
    df_future_matches = get_future_matches()
    df_raw_future_odds = get_future_odds()
    df_future_odds = clean_odds(df_raw_future_odds)
    return (
        df_future_odds.merge(
            df_future_matches,
            on="odd_match_code",
            how="right",
            validate="1:m"
        )
        [final_cols]
    )


def get_bet_list_names_in_db():
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    with Session(engine) as session:
        select_statement = select(BetList.bet_list_name)
        results = session.execute(select_statement).all()
    return [r[0] for r in results]
    
def get_matches_for_odds(match_codes):
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    match_codes_as_str = ', '.join(
        [f"'{code}'" for code in match_codes]
    )
    query = """
    select
        matches.odd_match_code,
        matches.description,
        matches.match_date as match_datetime,
        competitions.name as competition
    from sporacle_db.matches as matches
    left join sporacle_db.competitions as competitions
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
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    with Session(engine) as session:
        bet_list_statement = (
            select(BetList)
            .where(BetList.bet_list_name==bet_list_name)
        )
        bet_list = session.scalars(bet_list_statement).one()
        ls_odd_keys = [o for o in bet_list.odds]
        odds_statement = (
            select(Odd)
            .where(Odd.key.in_(ls_odd_keys))
        )
        odds = session.scalars(odds_statement).all()
        ls_odds = [odd.__dict__ for odd in odds]
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
    df_odds = get_odds_for_bet_list(bet_list_name)
    return df_odds.odd_match_code.to_list()

def get_existing_bet_list_summary(odd_keys):    
    odd_keys_as_str = ', '.join(
        [f"'{key}'" for key in odd_keys]
    )
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
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
    from sporacle_db.odds as odds
    left join sporacle_db.matches as matches using (odd_match_code)
    left join sporacle_db.competitions as competitions using (competition_code)
    where odds.key in ({odd_keys})
    """.format(odd_keys=odd_keys_as_str)
    df_summary = get_table_from_query(
        engine=engine, 
        query=query
    )
    return df_summary.to_dict(orient="records")
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
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    odd_keys = [odd_dict["key"] for odd_dict in bet_list_odds]
    odd_dates = [odd_dict["match_datetime"] for odd_dict in bet_list_odds]
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
            if bet_list_exists:
                existing_bet_list = session.scalar(existing_bet_list_statement)
                existing_bet_list.odds = odd_keys
                existing_bet_list.earliest_match_date = min(odd_dates)
                existing_bet_list.last_match_date = max(odd_dates)
                session.add(existing_bet_list)
            else:
                bet_list_obj = BetList(
                    bet_list_name=bet_list_name, 
                    odds=odd_keys,
                    earliest_match_date=min(odd_dates),
                    last_match_date=max(odd_dates)
                )
                session.add(bet_list_obj)

def drop_bet_list(bet_list_name):
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    with Session(engine) as session: 
        with session.begin():
            # Fetch existing bet list object if exists
            existing_bet_list_statement = (
                select(BetList)
                .where(BetList.bet_list_name == bet_list_name)
                
            )
            bet_list = session.scalar(existing_bet_list_statement)
            session.delete(bet_list)

def get_bet_lists_wide_df():
    engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
    query = """
    with extracted_odd_keys as (
        SELECT
            bet_lists.bet_list_name, 
            odd_keys.value as key
        FROM app_db.bet_lists as bet_lists
        JOIN json_each(bet_lists.odds) as odd_keys
        WHERE bet_lists.last_match_date < DATE('now')
    ),

    bet_list_summary as (
        SELECT
            extracted_odd_keys.bet_list_name as bet_list_name,
            matches.description as description,
            matches.match_date as match_date,
            competitions.name as competition,
            (
                matches.half_time_home_goals || '-' || matches.half_time_away_goals
            ) as half_time_score,
            (
                matches.full_time_home_goals || '-' || matches.full_time_away_goals
            ) as full_time_score,
            odds.odd_name as odd_name,
            odds.odd_value as odd_value,
            odds.is_winning as is_winning,
            extracted_odd_keys.key as key,
            odds.odd_match_code as odd_match_code,
            matches.competition_code as competition_code
        FROM extracted_odd_keys
        LEFT JOIN sporacle_db.odds as odds USING (key)
        LEFT JOIN sporacle_db.matches as matches USING (odd_match_code)
        LEFT JOIN sporacle_db.competitions as competitions USING (competition_code)
    )

    SELECT
        bet_list_name,
        description,
        match_date,
        competition,
        half_time_score,
        full_time_score,
        odd_name,
        odd_value,
        is_winning
    FROM bet_list_summary
    """
    df_summary = get_table_from_query(
        engine=engine, 
        query=query
    )
    return df_summary

def get_winning_and_losing_bet_lists(df_summary: pd.DataFrame) -> dict[str, bool]:
    df_win_lose_bet_lists = (
        df_summary
        .groupby('bet_list_name')
        [['is_winning']]
        .all()
        .reset_index()
    )
    dc_win_lost_bet_lists = {
        dc_["bet_list_name"]:dc_["is_winning"]
        for dc_ in df_win_lose_bet_lists.to_dict(orient="records")
    }
    return dc_win_lost_bet_lists

def get_winning_and_losing_bet_lists_count(
    dc_win_lost_bet_lists: dict[str, bool]
) -> dict[str, int]:
    win_bet_lists_count = sum(
        [1 for x,y in dc_win_lost_bet_lists.items() if y == True ]
    )
    lose_bet_list_count = sum(
        [1 for x,y in dc_win_lost_bet_lists.items() if y == False ]
    )
    return {
        "winning":win_bet_lists_count, "losing":lose_bet_list_count
    }

#endregion

