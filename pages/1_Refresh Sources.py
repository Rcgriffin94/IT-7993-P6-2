import streamlit as st
from streamlit_extras.app_logo import add_logo
import pandas as pd
import feedparser
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
from env_config.configdev import MONGODB_DEV_USERNAME,MONGODB_DEV_PASSWORD
from env_config.configprod import MONGODB_USERNAME, MONGODB_PASSWORD
import time
import sys
import requests


st.set_page_config(
    layout="wide", 
    page_title='KSU RSS Feed Parser • Refresh Records',
    page_icon='📰'
)

add_logo('ksu_logo_resized.png')

sideb = st.sidebar
learn_more = sideb.link_button('Click me to view project details. KSU Login required.', url='https://kennesawedu.sharepoint.com/:u:/r/sites/Team-IT7993P62/SitePages/Home.aspx?csf=1&web=1&share=EUU5-gklx6BAiIoOu3ZyYc0ByoNeT6Aa13RfY2VuW3wKbA&e=3fVxmy')

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
def get_sentiment(headline):
	API_URL = "https://api-inference.huggingface.co/models/hakonmh/sentiment-xdistil-uncased"
	headers = {"Authorization": f"Bearer {'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'}"}
	json_payload = {'inputs': headline}
	response = requests.post(API_URL, headers=headers, json=json_payload)
	output = response.json()
	max_score_label = max(output[0], key=lambda x: x['score'])['label'].upper()
	return max_score_label

st.title('Refresh Sources')
env_options = ['- select an environment -', 'prod', 'dev']
env_selection = st.selectbox('Select an Environment', env_options, index=0)

uploaded_selection = st.radio('Do you want to upload new sources?', ['Yes', 'No'], horizontal=True, index=1)

timeout = 5
if uploaded_selection == 'Yes':
    uploaded_file = st.file_uploader("Choose a file", type='CSV')
    if uploaded_file is not None:
        csvRecordsDF = pd.read_csv(uploaded_file)

    if st.button('Run'):
        if env_selection == '- select an environment -' and uploaded_file is None:
            st.error('Please select an environment AND upload the source CSV then try again')
            sys.exit()
        elif env_selection == '- select an environment -':
            st.error('Please select an environment and then try again')
            sys.exit()
        elif uploaded_file is None:
            st.error('Please upload the source CSV file and try again')
            sys.exit()

        with st.status('Upload new sources'):
            client, databaseName, resultsCollectionName, URLCollectionName = connectToMongo(env_selection)
            st.write('Connected to MongoDB')
            time.sleep(timeout)
            db = client[databaseName]
            URLCollection = db[URLCollectionName]
            resultsCollection = db[resultsCollectionName]

            # Pulling existing urls from MongoDB and comparing to records from the CSV to find new records
            st.write('Pulled records from MongoDB')
            time.sleep(timeout)
            URLCollectionRecords = list(URLCollection.find())
            URLCollectionRecordsDF = pd.DataFrame(URLCollectionRecords)
            st.write('Comparing the records in MongoDB to the uploaded CSV')
            time.sleep(timeout)
            newRecordsDF = csvRecordsDF[~csvRecordsDF.isin(URLCollectionRecordsDF.to_dict('list')).all(1)]
            newRecordsCount = len(newRecordsDF)

            time.sleep(timeout)

            try:
                records = newRecordsDF.to_dict(orient='records')
                URLCollection.insert_many(records)
                st.write(f'Reviewed sources.csv and added {newRecordsCount} new sources')

            except TypeError:
                st.write('Reviewed sources.csv and added 0 new sources')

        with st.status('Extract news articles from RSS feeds'):

            URLCollectionRecords = list(URLCollection.find())
            URLCollectionRecordsDF = pd.DataFrame(URLCollectionRecords)

            count = 1

            data = {
                'RSS Feed Name': [], 
                'Headline': [], 
                'Link': [],
                'Date Published': [],
                'Date Extracted': [],
                'Sentiment': []
            }
            
            for row in range(len(URLCollectionRecordsDF)):
                url = URLCollectionRecordsDF.loc[row, 'URL']
                dateFormat = URLCollectionRecordsDF.loc[row, 'dateFormat']
                siteName = URLCollectionRecordsDF.loc[row, 'siteName']


                st.write(f'Extracted from {siteName} - {len(URLCollectionRecordsDF) - count} sources remaining')

                feed = feedparser.parse(url)
                if feed.status == 200:
                    for entry in feed.entries:

                        data['RSS Feed Name'].append(siteName)
                        data['Headline'].append(entry.title)
                        data['Link'].append(entry.link)
                        data['Sentiment'].append(get_sentiment(entry.title))
                        current_date = datetime.now()
                        formatted_date = current_date.strftime('%m/%d/%Y')
                        data['Date Extracted'].append(formatted_date)

                        try: 

                            if dateFormat == "dow, dd month yyyy hh:mm:ss tmz":
                                date_str = str(entry.published)
                                date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                                formatted_date = date_obj.strftime("%m/%d/%Y")
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss tmz':
                                date_object = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %Z')
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss -0000':
                                unformattedDate = entry.published
                                location = unformattedDate.find('-')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss +0000':
                                unformattedDate = entry.published
                                location = unformattedDate.find('+')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss EDT':
                                unformattedDate = entry.published
                                location = unformattedDate.find('EDT')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss EST':
                                unformattedDate = entry.published
                                location = unformattedDate.find('EST')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                        except ValueError:
                            data['Date Published'].append(entry.published)

                else:
                    st.write(" Failed to fetch the feed. Status code:", feed.status)

                count = count + 1

            time.sleep(timeout)
            st.write('Complete')
            
        
        with st.status('Remove duplicate records'):
            st.write('Review the current pull to itself and drop duplicates')
            resultsdf = pd.DataFrame(data)
            # If the new results contain duplicates URL then keep the first record and drop the rest
            resultsdf =  resultsdf.drop_duplicates(subset=['Link'], keep='first')

            st.write('Review the current pull to MongoDB and drop duplicates')
            # Pulling exisiting records from MongoDB to compare to new records
            existingURLS = list(resultsCollection.find())
            existingURLSDF = pd.DataFrame(existingURLS)

            time.sleep(timeout)

            if len(existingURLSDF) == 0:
                # If Mongo is empty then add all new records
                resultsDict = resultsdf.to_dict(orient='records')
                st.write(f'Saving {len(resultsdf)} records to MongoDB')
                time.sleep(timeout)
                resultsCollection.insert_many(resultsDict)
                st.write('Save complete!')

            else:
                # if mongo is not empty
                mask = ~resultsdf['Link'].isin(existingURLSDF['Link'])
                newRecords = resultsdf[mask]

                if len(newRecords) != 0:
                    # If new records are found then remove duplicates and then add
                    st.write(f'Saving {len(newRecords)} records to MongoDB')
                    time.sleep(timeout)
                    newRecordsDict = newRecords.to_dict(orient='records')
                    resultsCollection.insert_many(newRecordsDict)
                    st.write('Save complete')
                
                else:
                    st.write('No new records found')

        st.success('Refresh complete!')
else:
    if st.button('Run'):
        if env_selection == '- select an environment -':
            st.error('Please select an environment and then try again')
            sys.exit()

        with st.status('Extract news articles from RSS feeds'):
            client, databaseName, resultsCollectionName, URLCollectionName = connectToMongo(env_selection)

            st.write('Connected to MongoDB')
            time.sleep(timeout)
            db = client[databaseName]
            URLCollection = db[URLCollectionName]
            resultsCollection = db[resultsCollectionName]

            URLCollectionRecords = list(URLCollection.find())
            URLCollectionRecordsDF = pd.DataFrame(URLCollectionRecords)

            count = 1

            data = {
                'RSS Feed Name': [], 
                'Headline': [], 
                'Link': [],
                'Date Published': [],
                'Date Extracted': [],
                'Sentiment': []
            }
            
            for row in range(len(URLCollectionRecordsDF)):
                url = URLCollectionRecordsDF.loc[row, 'URL']
                dateFormat = URLCollectionRecordsDF.loc[row, 'dateFormat']
                siteName = URLCollectionRecordsDF.loc[row, 'siteName']
                


                st.write(f'Extracted from {siteName} - {len(URLCollectionRecordsDF) - count} sources remaining')

                feed = feedparser.parse(url)
                if feed.status == 200:
                    for entry in feed.entries:

                        data['RSS Feed Name'].append(siteName)
                        data['Headline'].append(entry.title)
                        data['Link'].append(entry.link)
                        data['Sentiment'].append(get_sentiment(entry.title))
                        current_date = datetime.now()
                        formatted_date = current_date.strftime('%m/%d/%Y')
                        data['Date Extracted'].append(formatted_date)

                        try: 

                            if dateFormat == "dow, dd month yyyy hh:mm:ss tmz":
                                date_str = str(entry.published)
                                date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                                formatted_date = date_obj.strftime("%m/%d/%Y")
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss tmz':
                                date_object = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %Z')
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss -0000':
                                unformattedDate = entry.published
                                location = unformattedDate.find('-')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss +0000':
                                unformattedDate = entry.published
                                location = unformattedDate.find('+')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss EDT':
                                unformattedDate = entry.published
                                location = unformattedDate.find('EDT')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                            if dateFormat == 'dow, dd mm yyyy hh:mm:ss EST':
                                unformattedDate = entry.published
                                location = unformattedDate.find('EST')
                                stripLen = len(unformattedDate) - location
                                unformattedDate = unformattedDate[:stripLen]
                                formattedDate = date_object.strftime('%m/%d/%Y')
                                data['Date Published'].append(formattedDate)

                        except ValueError:
                            data['Date Published'].append(entry.published)

                else:
                    st.write(" Failed to fetch the feed. Status code:", feed.status)

                count = count + 1

            time.sleep(timeout)
            st.write('Complete')
            
        
        with st.status('Remove duplicate records'):
            st.write('Review the current pull to itself and drop duplicates')
            resultsdf = pd.DataFrame(data)
            # If the new results contain duplicates URL then keep the first record and drop the rest
            resultsdf =  resultsdf.drop_duplicates(subset=['Link'], keep='first')

            st.write('Review the current pull to MongoDB and drop duplicates')
            # Pulling exisiting records from MongoDB to compare to new records
            existingURLS = list(resultsCollection.find())
            existingURLSDF = pd.DataFrame(existingURLS)

            time.sleep(timeout)

            if len(existingURLSDF) == 0:
                # If Mongo is empty then add all new records
                resultsDict = resultsdf.to_dict(orient='records')
                st.write(f'Saving {len(resultsdf)} records to MongoDB')
                time.sleep(timeout)
                resultsCollection.insert_many(resultsDict)
                st.write('Save complete!')

            else:
                # if mongo is not empty
                mask = ~resultsdf['Link'].isin(existingURLSDF['Link'])
                newRecords = resultsdf[mask]

                if len(newRecords) != 0:
                    # If new records are found then remove duplicates and then add
                    st.write(f'Saving {len(newRecords)} records to MongoDB')
                    time.sleep(timeout)
                    newRecordsDict = newRecords.to_dict(orient='records')
                    resultsCollection.insert_many(newRecordsDict)
                    st.write('Save complete')
                
                else:
                    st.write('No new records found')

        st.success('Refresh complete!')
        