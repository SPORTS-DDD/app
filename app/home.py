from app_session import SessionKey, setup_bet_lists_in_session
import streamlit as st
import data
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
        st.success('Database successfully imported')

@st.dialog('Download local DB')
def display_download_db_dialog():
    with open('local.database.db') as f:
        data = f.read()
    st.dowload_button(
        label='Download local DB', 
        data=data,
        file_name="local.database.db"
    )
#endregion


#region UI
st.header('Home')
if (
    not SessionKey.LOCAL_DB_INITIALIZED.is_in_session() and
    not SessionKey.SPORACLE_DB_DOWNLOADED.is_in_session()
):
    
    URL = 'https://raw.githubusercontent.com/SPORTS-DDD/sporacle_app/refs/heads/test_commit_db_file/database.db'
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

st.subheader('Sporacle DB')
with st.expander('Matches'):
    engine = data.get_sqlalchemy_sporacle_engine()
    df = data.get_table_from_query(engine, "SELECT * FROM matches;")
    st.write(df)