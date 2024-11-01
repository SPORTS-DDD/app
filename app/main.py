import data
import streamlit as st
#region Functions



#endregion

#region UI
st.title('Sporacle App')

home_page = st.Page("home.py", title="Home", icon=":material/home:")
at_term_bet_lists = st.Page("bet_lists/at_term_bet_lists.py", title="At-term bet lists", icon=":material/fact_check:")
search_bet_lists = st.Page("bet_lists/search_bet_lists.py", title="Search on-going bet lists", icon=":material/search:")
# bet_lists_page = st.Page("bet_lists/bet_lists.py", title="Bet Lists", icon=":material/table:")
create_update_bet_list_page = st.Page("bet_lists/create_update_bet_list.py", title="Create/Update bet list", icon=":material/edit_note:")
# new_bet_list_page = st.Page("bet_lists/new_bet_list.py", title="Create new bet list", icon=":material/format_list_numbered_rtl:")
# saved_bet_lists_page = st.Page("bet_lists/saved_bet_lists.py", title="Saved bet lists", icon=":material/database:")
# update_bet_list_page = st.Page('bet_lists/update_bet_list.py', title="Update a bet list", icon=":material/edit_note:")

pg = st.navigation(
    {
        "Main":[home_page],
        "Bets":[
            # bet_lists_page, 
            search_bet_lists,
            create_update_bet_list_page,
            at_term_bet_lists,
            # new_bet_list_page, 
            # saved_bet_lists_page, 
            # update_bet_list_page
        ],
    }
)

# if "bets_lists" not in st.session_state:
#     st.session_state["bets_lists"] = {}

#endregion

pg.run()
