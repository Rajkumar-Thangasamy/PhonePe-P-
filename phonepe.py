import streamlit as st
from streamlit_option_menu import option_menu

st.set_page_config(layout="wide")

st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")

with st.sidebar:

    select = option_menu("MAIN MENU",["HOME","DATA EXPLORATION","TOP CHARTS"])

if select == "HOME":
    pass

elif select == "DATA EXPLORATION":

    tab_1, tab_2, tab_3 = st.tabs(["AGGREGATED ANALYSIS", "MAP ANALYSIS", "TOP ANALYSIS"]) 

    with tab_1:

        method_1 = st.radio("SELECT THE METHOD",["AGGREGATED INSURANCE","AGGREGATED TRANSACTION","AGGREGATED USER"])

        if method_1 == "AGGREGATED INSURANCE":
            pass

        elif method_1 == "AGGREGATED TRANSACTION":
            pass

        elif method_1 == "AGGREGATED USER":
            pass

    with tab_2:

        method_2 = st.radio("SELECT THE METHOD",["MAP INSURANCE","MAP TRANSACTION","MAP USER"])

        if method_2 == "MAP INSURANCE":
            pass

        elif method_2 == "MAP TRANSACTION":
            pass

        elif method_2 == "MAP USER":
            pass

    with tab_3:

        method_3 = st.radio("SELECT THE METHOD",["TOP INSURANCE","TOP TRANSACTION","TOP USER"])

        if method_3 == "TOP INSURANCE":
            pass

        elif method_3 == "TOP TRANSACTION":
            pass

        elif method_3 == "TOP USER":
            pass

elif select == "TOP CHARTS":
    pass
