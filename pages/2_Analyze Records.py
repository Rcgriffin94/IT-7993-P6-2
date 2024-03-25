import streamlit as st
from streamlit_extras.app_logo import add_logo

st.set_page_config(
        layout="wide",
        page_title='KSU RSS Feed Parser • Analyze Records',
        page_icon='📰'
    )
add_logo('ksu_logo_resized.png')

st.title('Refresh Sources')

st.image('under_construction.png')