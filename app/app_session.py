from enum import StrEnum
import streamlit as st

class SessionKey(StrEnum):
    BET_LISTS = 'bet_lists'
    SELECTED_BET_LISTS = 'selected_bet_lists'
    SPORACLE_DB_DOWNLOADED = 'sporacle_db_is_downloaded'
    LOCAL_DB_UPLOADED = 'local_db_is_uploaded'
    LOCAL_DB_INITIALIZED = 'local_db_is_initialized'
    NEW_BET_LIST_MATCHES = 'new_bet_list_matches'
    CREATE_UPDATE_BET_LIST_ACTION = "create_update_bet_list_action"
    UPDATE_BET_LIST_NAME = "update_bet_list_name"
    NU_BET_LIST_MATCHES = 'new_updated_bet_list_matches'

    def is_in_session(self):
        if self in st.session_state:
            return True
        else:
            False
    
    def get(self):
        return st.session_state.get(self)
    
    def update(self, value):
        st.session_state[self] = value

    def delete(self):
        del st.session_state[self]


class CREATE_UPDATE_BET_LIST_ACTION(StrEnum):
    CREATE = 'create'
    UPDATE = 'update'


    

def setup_bet_lists_in_session():
    if SessionKey.BET_LISTS not in st.session_state:
        SessionKey.BET_LISTS.update({})

    if SessionKey.SELECTED_BET_LISTS not in st.session_state:
        SessionKey.SELECTED_BET_LISTS.update([])

    if SessionKey.CREATE_UPDATE_BET_LIST_ACTION not in st.session_state:
        SessionKey.CREATE_UPDATE_BET_LIST_ACTION.update(None)
    

def local_db_is_initialized():
    if (
        selected_bet_list_key:=SessionKey.LOCAL_DB_INITIALIZED
        not in st.session_state
    ):
        is_initialized = False
    else:
        is_initialized = True
    return is_initialized

def update_bet_list_odd_in_session(match_, odd_dict):
    nblm_key = SessionKey.NEW_BET_LIST_MATCHES
    odd_match_code = match_.odd_match_code
    st.session_state[nblm_key][odd_match_code] = odd_dict

