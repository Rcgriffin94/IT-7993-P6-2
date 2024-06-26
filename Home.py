import streamlit as st
from streamlit_extras.app_logo import add_logo

st.set_page_config(
    layout="wide", 
    page_title='KSU RSS Feed Parser • Home',
    page_icon='📰'
)

add_logo('ksu_logo_resized.png')

sideb = st.sidebar
learn_more = sideb.link_button('Click me to view project details. KSU Login required.', url='https://kennesawedu.sharepoint.com/:u:/r/sites/Team-IT7993P62/SitePages/Home.aspx?csf=1&web=1&share=EUU5-gklx6BAiIoOu3ZyYc0ByoNeT6Aa13RfY2VuW3wKbA&e=3fVxmy')

st.title('Welcome to our RSS Financial News Feed Parser!')
st.title('KSU Project ID: GW1-P6-2')

st.write('Disclaimer: This tool is currently in beta and is not designed to be ran by multiple users at once due to API constraints.')

st.write('This application will provide you with an AI sentiment analysis of recent financial news from across the internet. From here you can view the analysis using "Analyze Records" and filter by keyword and time frame. While a number of sources from across the internet are already in place, you have the ability to add additional sources to create a more thourough analysis - simply upload additional sources through the "Refresh Sources" option. Happy analysis!')
st.subheader('By: Raissa Divinagracia, Ryan Griffin, Justin Mayercik, Joshua McMillion, and Jacob Sweat')
st.write('Link to presentation recording is https://www.youtube.com/watch?v=BJgf5XnsWP4&t=1s')
st.video('https://www.youtube.com/watch?v=BJgf5XnsWP4&t=1s')
