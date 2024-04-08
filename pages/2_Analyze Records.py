import streamlit as st
from streamlit_extras.app_logo import add_logo
from env_config.configdev import MONGODB_DEV_USERNAME,MONGODB_DEV_PASSWORD
from env_config.configprod import MONGODB_USERNAME, MONGODB_PASSWORD
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
import sys
import pandas as pd
from fuzzywuzzy import fuzz
import numpy as np
import matplotlib.pyplot as plt


def search_headlines(keyword, headline):
    match_score = fuzz.partial_ratio(keyword.lower(), headline.lower())
    return match_score

def connectToMongo(env):

    if env == 'dev':
        # MongoDB Atlas connection details
        databaseName = 'it7993p62DEV'
        resultsCollectionName = 'rss_results_dev'
        URLCollectionName = 'rss_urls_dev'

        connectionString = f'mongodb+srv://{MONGODB_DEV_USERNAME}:{MONGODB_DEV_PASSWORD}@it7993p62dev.jmhnxyt.mongodb.net/?retryWrites=true&w=majority'

    elif env == 'prod':
        # MongoDB Atlas connection details
        databaseName = 'it7993p62'
        resultsCollectionName = 'rss_results_prod'
        URLCollectionName = 'rss_urls_prod'

        connectionString = f'mongodb+srv://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@it7993p62.btkxu9s.mongodb.net/?retryWrites=true&w=majority'

    # Connect to MongoDB Atlas
    client = MongoClient(connectionString, tlsCAFile=certifi.where(), server_api=ServerApi('1'))

    # send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
    except Exception as e:
        print(e)

    return client, databaseName, resultsCollectionName, URLCollectionName

st.set_page_config(
        layout="wide",
        page_title='KSU RSS Feed Parser • Analyze Records',
        page_icon='📰'
    )
add_logo('ksu_logo_resized.png')

st.title('Analyze Sources')

env_options = ['- select an environment -', 'prod', 'dev']
env_selection = st.selectbox('Select an Environment', env_options, index=0)
keyword = st.text_input('Keyword search', value=None).lower()
search_accuracy_options = ['Low', 'Medium', 'High']
search_accuracy =  st.select_slider(label='Select a search accuracy value',options=search_accuracy_options)
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input('Start Date', value=None)
with col2:
    end_date = st.date_input('End Date', value=None)

if st.button('Run'):
    if start_date is None and end_date is None and keyword in [None, ''] and env_selection == '- select an environment -':
        st.error('Please fill out all fields')
        sys.exit()
    elif start_date is None and end_date is None and keyword in [None, '']:
        st.error('Please enter a start date, end date, keyword, and environment selection')
        sys.exit()
    elif start_date is None and end_date is None and env_selection == '- select an environment -':
        st.error('Please enter a start date, end date, and environment selection')
        sys.exit()
    elif start_date is None and keyword in [None, ''] and env_selection == '- select an environment -':
        st.error('Please enter a start date, keyword, and environment selection')
        sys.exit()
    elif end_date is None and keyword in [None, ''] and env_selection == '- select an environment -':
        st.error('Please enter an end date, keyword, and environment selection')
        sys.exit()
    elif start_date is None and end_date is None:
        st.error('Please enter a start date and end date')
        sys.exit()
    elif start_date is None and keyword in [None, '']:
        st.error('Please enter a start date and keyword')
        sys.exit()
    elif start_date is None and env_selection == '- select an environment -':
        st.error('Please enter a start date and environment selection')
        sys.exit()
    elif end_date is None and keyword in [None, '']:
        st.error('Please enter an end date and keyword')
        sys.exit()
    elif end_date is None and env_selection == '- select an environment -':
        st.error('Please enter an end date and environment selection')
        sys.exit()
    elif keyword in [None, ''] and env_selection == '- select an environment -':
        st.error('Please enter a keyword and environment selection')
        sys.exit()
    elif start_date is None:
        st.error('Please enter a start date')
        sys.exit()
    elif end_date is None:
        st.error('Please enter an end date')
        sys.exit()
    elif keyword in [None, '']:
        st.error('Please enter a keyword')
        sys.exit()
    elif env_selection == '- select an environment -':
        st.error('Please enter an environment selection')
        sys.exit()

    client, databaseName, resultsCollectionName, URLCollectionName = connectToMongo(env_selection)
    db = client[databaseName]
    resultsCollection = db[resultsCollectionName]
    existingURLS = list(resultsCollection.find())
    existingURLSDF = pd.DataFrame(existingURLS)

    existingURLSDF['Date Published'] = pd.to_datetime(existingURLSDF['Date Published'])
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    filtered_df = existingURLSDF[(existingURLSDF['Date Published'] >= start_date) & (existingURLSDF['Date Published'] <= end_date)]


    if search_accuracy == 'Low':
        threshold = 25
    if search_accuracy == 'Medium':
        threshold = 50
    if search_accuracy == 'High':
        threshold = 75

    filtered_df['Match_Scores'] = filtered_df['Headline'].apply(lambda headline: search_headlines(keyword, headline))
    filtered_df = filtered_df[filtered_df['Match_Scores'] >= threshold]
    filtered_df = filtered_df.sort_values(by='Match_Scores', ascending=False)

    st.title(f'There were {len(filtered_df)} records found')

    # Bar chart
    st.subheader('Headline Sentiment Analysis Counts')
    sentiment_counts = filtered_df['Sentiment'].value_counts()
    st.bar_chart(sentiment_counts)

    # Line Chart
    filtered_df['Date Published'] = pd.to_datetime(filtered_df['Date Published'])
    grouped_df = filtered_df.groupby(['Date Published', 'Sentiment']).size().unstack(fill_value=0)
    grouped_df.reset_index(inplace=True)
    grouped_df['Date Published'] = grouped_df['Date Published'].dt.strftime('%Y-%m-%d')
    st.subheader('Headline Sentiment Analysis over time by article publication date')
    chart_data = grouped_df.set_index('Date Published')
    st.line_chart(chart_data)

    st.subheader('Raw Data')
    st.dataframe(filtered_df)
