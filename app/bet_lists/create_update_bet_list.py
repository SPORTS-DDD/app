import streamlit as st
from app_session import (
    SessionKey, 
    CREATE_UPDATE_BET_LIST_ACTION, 
)
from data import (
    get_selected_matches_for_bet_list,
    get_odds_for_bet_list,
)

from bet_lists.utils import (
    create_or_update_bet_list,
    display_df_program, 
    display_new_bet_list_form,
    init_new_bet_list_in_session, 
)

bet_list_name = create_or_update_bet_list()
     
SessionKey.NU_BET_LIST_MATCHES.update({})

if bet_list_name:
    if SessionKey.CREATE_UPDATE_BET_LIST_ACTION.get() == CREATE_UPDATE_BET_LIST_ACTION.UPDATE:
        ls_selected_matches = get_selected_matches_for_bet_list(bet_list_name)
        df_previous_bet_list_odds = get_odds_for_bet_list(bet_list_name)
        st.header(f'Update {bet_list_name}')
    else:
        st.header(f'Create {bet_list_name}')
        ls_selected_matches = None
        df_previous_bet_list_odds = None

    st.subheader('Match Selection')

    with st.expander('Select the matches you want to add to your bet list'):  
        df_program_matches = display_df_program(ls_selected_matches)

    init_new_bet_list_in_session(df_program_matches)
    st.subheader('Bets Selection')
    display_new_bet_list_form(bet_list_name, df_program_matches, df_previous_bet_list_odds)

