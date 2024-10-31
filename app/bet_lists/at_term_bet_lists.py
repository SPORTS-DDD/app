import streamlit as st
from data import (
    get_bet_lists_wide_df,
    get_winning_and_losing_bet_lists,
    get_winning_and_losing_bet_lists_count
)

def highlight_losing_odds(row):
    if row.is_winning == False:
        return ['font-weight: bold; color: red' for r in row]
    else:
        return ['' for r in row]


st.header('At-term bet lists')

df_at_term_bet_lists = get_bet_lists_wide_df()
dc_win_lost_bet_lists = get_winning_and_losing_bet_lists(df_at_term_bet_lists)
dc_win_lose_bet_lists_count = get_winning_and_losing_bet_lists_count(dc_win_lost_bet_lists)
win_col, lose_col = st.columns(2)
win_col.metric(label=r"# Winning bet lists", value=dc_win_lose_bet_lists_count["winning"])
lose_col.metric(label=r"# Losing bet lists", value=dc_win_lose_bet_lists_count["losing"])
win_bl_tab, lose_bl_tab = st.tabs(["🟩 &nbsp; &nbsp; Winning Bet Lists", "🟥 &nbsp; &nbsp; Losing Bet Lists"])
with win_bl_tab:
    winning_bet_lists = [
        bl_name 
        for bl_name, is_winning in dc_win_lost_bet_lists.items()
        if is_winning == True
    ]
    for bet_list_name in winning_bet_lists:
        df_bet_list = (
            df_at_term_bet_lists
            .query('bet_list_name == @bet_list_name')
            .drop(columns=["bet_list_name", "is_winning"])
        )
        total_odds = f'{df_bet_list.odd_value.product():.2f}'
        match_count = df_bet_list.description.count()
        with st.expander(
            f"**{bet_list_name}**  \n"
            f"∑ Odds: :green[{total_odds}] &nbsp; &nbsp; &nbsp; \# Matches: :green[{match_count}]"
        ):
            st.dataframe(
                df_bet_list,
                column_config={
                    "match_date":st.column_config.DateColumn('date',format="YYYY-MM-DD HH:mm"),
                    "odd_value":st.column_config.NumberColumn('odd value', format="%.2f"),
                }
            )
with lose_bl_tab:
    losing_bet_lists = [
        bl_name 
        for bl_name, is_winning in dc_win_lost_bet_lists.items()
        if is_winning == False
    ]
    for bet_list_name in losing_bet_lists:
        df_bet_list = (
            df_at_term_bet_lists
            .query('bet_list_name == @bet_list_name')
            .drop(columns=["bet_list_name"])
        )
        total_odds = f'{df_bet_list.odd_value.product():.2f}'
        match_count = df_bet_list.description.count()
        losing_match_count = df_bet_list.query('is_winning == False').description.count()
        with st.expander(
            f"**{bet_list_name}**  \n"
            f"∑ Odds: :red[{total_odds}] &nbsp; &nbsp; &nbsp; \# Losing Matches: :red[{losing_match_count}/{match_count}]"
        ):
            
            styled_df_bet_list = (
                df_bet_list
                .style.apply(highlight_losing_odds, axis=1)
                .format(precision=2)
            )
            st.dataframe(
                styled_df_bet_list,
                column_config={
                    "match_date":st.column_config.DateColumn('date',format="YYYY-MM-DD HH:mm"),
                    "odd_value":st.column_config.NumberColumn('odd value', format="%.2f"),
                    "is_winning":st.column_config.CheckboxColumn('is winning', width="small")
                }
            )