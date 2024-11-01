import streamlit as st
from data import (
    get_on_going_bet_lists, 
    get_bet_list_df,
    drop_bet_list
)
from app_session import SessionKey, CREATE_UPDATE_BET_LIST_ACTION

@st.dialog('Confirm bet list deletion')
def drop_bet_list_dialog(bet_list_name):
    st.write(f'Are you sure you want to drop {bet_list_name} ?')
    yes_col, no_col = st.columns(2)
    if yes_col.button('Yes', type="primary"):
        drop_bet_list(bet_list_name)
        st.rerun()
    if no_col.button('No'):
        st.rerun()

df_on_going_bet_lists = get_on_going_bet_lists()

selected_bet_list_name = st.selectbox(
    label="Select a bet list",
    options=df_on_going_bet_lists.bet_list_name.to_list(),
    index=None
)
if selected_bet_list_name:

    update_col, delete_col = st.columns(2)
    if update_col.button('Update'):
        SessionKey.CREATE_UPDATE_BET_LIST_ACTION.update(
            CREATE_UPDATE_BET_LIST_ACTION.UPDATE
        )
        SessionKey.UPDATE_BET_LIST_NAME.update(selected_bet_list_name)
        st.switch_page('bet_lists/create_update_bet_list.py')
    if delete_col.button('Delete'):
        drop_bet_list_dialog(selected_bet_list_name)


    df_bet_list = get_bet_list_df(selected_bet_list_name)
    match_count = df_bet_list.odd_match_code.count()
    min_date = df_bet_list.match_datetime.min()
    max_date = df_bet_list.match_datetime.max()
    odds_product = df_bet_list[["odd_value"]].astype(float).product(axis=0).values[0]
    datetime_format = r"%d/%m/%Y %H:%M"
    bet_list_name_col, odds_product_col = st.columns(2, gap="large")
    odds_product_col.write(f'#### Σ Odds: :green[{odds_product:.2f}]')
    st.markdown(
        f'**{match_count}** matches from **{min_date:{datetime_format}}** to **{max_date:{datetime_format}}**'
    )
    with st.expander(f'{selected_bet_list_name} matches & odds'):
        st.dataframe(
            df_bet_list,
            column_config={
                "odd_match_code": None,
                # "odd_label": None,
                "odd_threshold": None,
                "key": None
            }
        )