from app_session import SessionKey, setup_bet_lists_in_session
import streamlit as st
import data
from data import IN_MEMORY_SQLALCHEMY_DB_ENGINE
import pyodide

import requests as rq
import pandas as pd



#region Functions
async def download_file_as_bytes(url):
    # response = await pyodide.http.pyfetch(url, redirect='follow')
    response = await pyodide.http.pyfetch(url)
    data_in_bytes = await response.bytes()
    return data_in_bytes


@st.dialog('Upload local DB')
def display_upload_db_dialog():
    uploaded_file = st.file_uploader('Import a local DB file')
    if uploaded_file is not None:
        data.add_uploaded_db_file(uploaded_file)
        st.success('Database successfully imported. Please close this dialog.')
        

@st.dialog('Download local DB')
def display_download_db_dialog():
    with open('local.database.db', 'rb') as f:
        data = f.read()
    st.download_button(
        label='Download local DB', 
        data=data,
        file_name="local.database.db"
    )

@st.fragment
def display_query_form():
    with st.expander('Query databases'):
        selected_db = st.selectbox(
            label='What database do you want to query ?', 
            options=["Sporacle", "Local DB", "Cross-Database"]
        )
        if selected_db == "Sporacle":
            engine = data.get_sqlalchemy_sporacle_engine()
        elif selected_db == "Local DB":
            engine = data.get_sqlalchemy_local_db_engine()
        elif selected_db == "Cross-Database":
            engine = IN_MEMORY_SQLALCHEMY_DB_ENGINE
        else:
            raise NotImplementedError("The database should be either Sporacle or Local DB")
        query = st.text_area(f"Query to {selected_db}")
        if query:
            df = data.get_table_from_query(engine, query)
            st.write(df)
#endregion


#region UI
st.header('Home')
if (
    not SessionKey.LOCAL_DB_INITIALIZED.is_in_session() and
    not SessionKey.SPORACLE_DB_DOWNLOADED.is_in_session()
):
    
    URL = 'https://raw.githubusercontent.com/SPORTS-DDD/sporacle_app/refs/heads/database/data/database.db'
    sporacle_bytes_data = await download_file_as_bytes(URL) #noqa
    data.initial_app_setup(sporacle_bytes_data)
    st.success('Database successfully initialized')

setup_bet_lists_in_session()


with st.expander('Database Management'):
    create_col, upload_col, download_col = st.columns(3)
    if create_col.button('Create new empty local DB'):
        data.create_or_replace_local_db()
        
    if upload_col.button('Upload existing DB'):
        display_upload_db_dialog()

    if SessionKey.LOCAL_DB_INITIALIZED.is_in_session():
        if download_col.button('Download local DB'):
            display_download_db_dialog()


display_query_form()