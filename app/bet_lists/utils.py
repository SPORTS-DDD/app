from functools import reduce

import streamlit as st
import datetime as dt
import pandas as pd
from app_session import (
    SessionKey, 
    update_bet_list_odd_in_session,
    CREATE_UPDATE_BET_LIST_ACTION
)
from data import (
    get_program,
    get_bet_list_names_in_db,
    get_odds_table,
    upsert_bet_list,
    get_existing_bet_list_summary
)

def get_odds_for_match(match, df_odds):
    return (
        df_odds
        .query(
            "odd_match_code == @match.odd_match_code"
        )
    )

def init_new_bet_list_in_session(df_program_matches):
    selected_matches = (
        df_program_matches
        .query(
            'select_match == True'
        )
    )
    if not SessionKey.NEW_BET_LIST_MATCHES.is_in_session():
        SessionKey.NEW_BET_LIST_MATCHES.update({})
    dc_match_codes = {
        match_.odd_match_code:None for match_ in selected_matches.itertuples()
        if match_.odd_match_code not in SessionKey.NEW_BET_LIST_MATCHES.get()
    }
    SessionKey.NEW_BET_LIST_MATCHES.update(dc_match_codes)

def create_or_update_bet_list():
    if (
        SessionKey.CREATE_UPDATE_BET_LIST_ACTION.is_in_session() and
        SessionKey.CREATE_UPDATE_BET_LIST_ACTION.get() == CREATE_UPDATE_BET_LIST_ACTION.UPDATE and
        SessionKey.UPDATE_BET_LIST_NAME.is_in_session()
    ):
        action = SessionKey.CREATE_UPDATE_BET_LIST_ACTION.get()
        bet_list_to_cu = SessionKey.UPDATE_BET_LIST_NAME.get()
    else:
        action = CREATE_UPDATE_BET_LIST_ACTION.CREATE
        bet_list_to_cu = st.text_input("Name of the bet list")
        existing_bet_lists_in_db = get_bet_list_names_in_db()
        if bet_list_to_cu in existing_bet_lists_in_db:
            st.error(f'A bet list named {bet_list_to_cu} already exists in the database.', icon="❗")
        if not bet_list_to_cu:
            st.warning('First create a name for your bet list')
        SessionKey.CREATE_UPDATE_BET_LIST_ACTION.update(action)
    return bet_list_to_cu

def display_df_program(ls_selected=None):
    df_program = get_program()
    if ls_selected:
        data = (
            df_program
            .assign(
                select_match = lambda df_: df_["odd_match_code"].isin(ls_selected)
            )
        )
    else:
        data = (
            df_program
            .assign(select_match=None)
        )
    return st.data_editor(
        data=data,
        column_config={
            "select_match": st.column_config.CheckboxColumn(
                "Select",
                help="Check to add match to bet list",
                default=False
            ),
            "match_date": st.column_config.DateColumn(
                "Date",
                format="DD-MM-YYYY HH:mm",
                disabled=True
            )
        },
        disabled=[
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
        ],
        column_order=['select_match', *df_program.columns.to_list()],
        hide_index=True
    )

@st.fragment
def display_new_bet_list_form(bet_list_name, df_program_matches, df_previous_bet_list_odds):
    LS_ORDERED_ODDS = [
        '1', 'X', '2', 
        '1X', 'X2', '12', 
        '- 0.5 go.', '+ 0.5 go.', 
        '- 1.5 go.', '+ 1.5 go.',
        '- 2.5 go.', '+ 2.5 go.',
        '- 3.5 go.', '+ 3.5 go.', 
        '- 4.5 go.', '+ 4.5 go.',
        '- 5.5 go.', '+ 5.5 go.',
    ]
    df_odds = get_odds_table()
    selected_matches = (
        df_program_matches
        .query(
            'select_match == True'
        )
    )
    if selected_matches.shape[0] < 3:
        st.warning('A bet list should contain at least 3 odds')
    for match in selected_matches.itertuples():
        df_raw_match_odds = get_odds_for_match(match, df_odds)
        df_match_odds = (
            df_raw_match_odds
            .pivot(
                values="odd_value",
                columns="odd_name",
                index="odd_match_code"
            )
            .reset_index(drop=False)
            .rename_axis(None, axis=1)
            .drop(columns='odd_match_code')
        )
        if (
            df_previous_bet_list_odds is not None and
            match.odd_match_code in df_previous_bet_list_odds.odd_match_code.to_list()
        ):
            df_match_odds = display_previous_odd(match, df_match_odds, df_previous_bet_list_odds)
        with st.container(border=True):
            st.markdown(f'##### {match.description}')
            match_info_col1, match_info_col2 = st.columns(2)
            match_info_col1.write(match.competition)
            match_info_col2.write(
                (
                    match.match_date.strftime(r'%d/%m/%Y %H:%M')
                )
            )
            dataframe_columns = [col for col in LS_ORDERED_ODDS if col in df_match_odds.columns]
            selection = st.dataframe(
                data=df_match_odds[dataframe_columns],
                on_select="rerun",
                selection_mode=["single-column"]
            )
            
            if len(selection.selection.columns) == 1:
                odd_name = selection.selection.columns[0]
                odd_full_data = (
                    df_raw_match_odds
                    .query('odd_name == @odd_name')
                    .to_dict(orient="records")
                    [0]
                )
                odd_dict = {
                    "match_description":match.description,
                    "match_datetime":match.match_date,
                    "competition":match.competition,
                } | odd_full_data
                update_bet_list_odd_in_session(match, odd_dict)
    st.subheader('Summary')
    bet_amount = st.number_input(
        'Bet amount',
        min_value=10,
        value=10,
        step=5
    )
    new_bet_list_matches = SessionKey.NEW_BET_LIST_MATCHES.get().values()
    if SessionKey.CREATE_UPDATE_BET_LIST_ACTION.get() == CREATE_UPDATE_BET_LIST_ACTION.CREATE:
        display_summary(new_bet_list_matches, bet_amount)
    elif SessionKey.CREATE_UPDATE_BET_LIST_ACTION.get() == CREATE_UPDATE_BET_LIST_ACTION.UPDATE:
        new_bet_list_tab, old_bet_list_tab = st.tabs(["New Bet List", "Old Bet List"])
        with new_bet_list_tab:
            display_summary(new_bet_list_matches, bet_amount)
        with old_bet_list_tab:
            odd_keys = df_previous_bet_list_odds.key.to_list()
            existing_bet_list_matches = get_existing_bet_list_summary(odd_keys)
            display_summary(existing_bet_list_matches, bet_amount)
            


    form_submited = st.button('Save bet list')

    if form_submited:
        bet_list_odds = [
            {"odd_match_code":match_code} | odd_dict 
            for match_code, odd_dict in SessionKey.NEW_BET_LIST_MATCHES.get().items()
        ]
        upsert_bet_list(bet_list_name, bet_list_odds)
        st.switch_page('bet_lists/search_bet_lists.py')

    

def color_previous_odd(dat, c='silver'):
    return [f'background-color: {c}' for i in dat]

def display_previous_odd(match, df_match_odds, df_previous_bet_list_odds):
    highlighted_col = (
        df_previous_bet_list_odds
        .query('odd_match_code == @match.odd_match_code')
        .iloc[0]
        .odd_name
    )
    return (
        df_match_odds
        .style.apply(color_previous_odd, axis=0, subset=[highlighted_col])
        .format(precision=2)
    )



def display_summary(bet_list_matches, bet_amount):
    metric_col1, metric_col2 = st.columns(2)
    new_bet_list_summary = [
        odd_dict if odd_dict else {} \
            # for odd_dict in st.session_state.new_bet_list_matches.values()
            for odd_dict in bet_list_matches
    ]
    if new_bet_list_summary:
        odd_values_list = [
            odd_dict["odd_value"] for odd_dict in new_bet_list_summary if odd_dict
        ]
        
        odds_product = reduce(lambda x, y: x * y, odd_values_list) if odd_values_list else 0
        metric_col1.metric(
            'Σ Odds', 
            f"{odds_product:.2f}"
        )
        potential_gains = int(bet_amount * odds_product)
        metric_col1.metric('Potential gains', potential_gains)
        metric_col2.metric(
            r'# Matches', 
            len(odd_values_list) if odd_values_list else 0 
        )
        match_dates = [
            odd_dict["match_datetime"] for odd_dict in new_bet_list_summary \
            if odd_dict
        ]
        if match_dates:
            date_col1, date_col2 = metric_col2.columns(2)
            date_col1.markdown('**Earliest match**')
            date_col1.write(min(match_dates))
            date_col2.markdown('**Latest match**')
            date_col2.write(max(match_dates))
        def summary_is_valid(new_bet_list_summary):
            for odd_dict in new_bet_list_summary:
                if odd_dict == {}:
                    return False
            return True
        if summary_is_valid(new_bet_list_summary):
            st.dataframe(
                pd.DataFrame(new_bet_list_summary),
                column_config={
                    "odd_match_code": None,
                    # "odd_label": None,
                    "odd_threshold": None,
                    "key": None
                }
            )
        else:
            st.warning('An odd choice is required for every selected match')