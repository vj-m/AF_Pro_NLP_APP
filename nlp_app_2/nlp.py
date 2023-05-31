import pandas as pd
import sentence_transformers
from sentence_transformers import SentenceTransformer, util
import torch
import numpy
import re
import time
import spacy
import mysql.connector
from collections import Counter
from string import punctuation

class Data_ETL:

    def __init__(self):

        self.data_df = pd.read_excel('AF_Pro_Videos_May30_2023.xlsx', sheet_name='Sheet1')
        #print('Init - ', self.data_df.head(2))
        # Get the encoder Transformer Model
        self.transformer = SentenceTransformer('all-MiniLM-L6-v2')
        self.df = self.load_transform_data(self.data_df)
        #print('after load transform -', self.df.head(2))
        self.df = self.initial_embedder(self.df)
        #print('after embedding -', self.df.head(2))
        self.nlp = spacy.load("en_core_web_sm")

    #id, description ,name, summary, video_thubnail_id
    def load_transform_data(self, data_df):
        data_df.drop(columns=[
        'version',
        'creator_id',
        'date_created',
        'date_deleted',
        'deleted',
        'file_id',
        'last_updated',
        'visibility',
        'duration',
        'sport_id',
        'sport_position_id',
        'video_category_id'
        ], axis = 1, inplace = True)

        # Create new columns for Embedding and Similarity Score
        data_df['embedding'] = 'nan'
        data_df['score'] = 0
        data_df['support'] = ''
        return data_df


    def initial_embedder(self, data_df):
        st = time.time()
        for index, row in data_df.iterrows():

            if row['support'] == 'nan':
                text = str(row['name']) + ' '+ str(row['summary']) + ' ' + str(row['description'])
                embedding = self.transformer.encode(text, convert_to_tensor=True)
                # print(embedding)
                data_df.at[index, 'embedding'] = embedding
            else:
                text = str(row['name']) + ' '+ str(row['summary']) + ' ' + str(row['description']) + ' ' + str(row['support'])
                embedding = self.transformer.encode(text, convert_to_tensor=True)
                # print(embedding)
                data_df.at[index, 'embedding'] = embedding

        print('Time For Embedding - ', time.time() - st)
        return data_df


    def search(self, query, N = 3):
        st = time.time()
        query_embedding = self.transformer.encode(query, convert_to_tensor=True)
        for index, row in self.df.iterrows():
            embedding = self.df.at[index, 'embedding']
            # print(util.cos_sim(query_embedding, embedding)[0].numpy()[0])
            self.df.at[index, 'score'] = util.cos_sim(query_embedding, embedding)[0].numpy()[0]

        final_df = self.df.sort_values(by=['score'], ascending=False)
        IDs = []

        for index, row in final_df.head(N).iterrows():
            IDs.append(row['id'])

        print('Time for search - ', time.time() - st)
        print('video ids  -', IDs)

        return {
            'query' : query,
            'video_ids' : IDs
        }

    def search_pro(self, query, N = 3):
        st = time.time()
        query_embedding = self.transformer.encode(query, convert_to_tensor=True)
        for index, row in self.df.iterrows():
            embedding = self.df.at[index, 'embedding']
            # print(util.cos_sim(query_embedding, embedding)[0].numpy()[0])
            self.df.at[index, 'score'] = util.cos_sim(query_embedding, embedding)[0].numpy()[0]

        final_df = self.df.sort_values(by=['score'], ascending=False)
        output = []

        for index, row in final_df.head(N).iterrows():
            # add thumbnail path when available
            output.append((row['id'], row['score'], int(row['video_thubnail_id'])))

        print('Time for search - ', time.time() - st)
        print('output  -', output)

        return {
            'query' : query,
            'output' : output
        }

    def get_hotwords(self, text):
        result = []
        pos_tag = ['PROPN', 'ADJ', 'NOUN']
        doc = self.nlp(text.lower())
        for token in doc:
            if (token.text in self.nlp.Defaults.stop_words or token.text in punctuation):
                continue
            if (token.pos_ in pos_tag):
                result.append(token.text)
        return result


    def get_optimized_query_keywords(self, query, N = 20):
        top_N = []
        output = set(self.get_hotwords(query))
        most_common_list = Counter(output).most_common(N)
        for item in most_common_list:
            top_N.append(item[0])
        return top_N


    def add_support(self, id, query):
        try:
            value = self.df.loc[self.df['id'] == id, 'support'].values[0]
        except Exception as E:
            print('Error in add support - ', E)

        if value == '':
            tokens = self.get_optimized_query_keywords(query)
            value = " ".join(tokens)
            self.df.loc[self.df['id'] == id, 'support'] = value
        else:
            support_list = value.split()
            support_list.extend(self.get_optimized_query_keywords(query))
            new_value = list(set(support_list))
            value = " ".join(new_value)
            self.df.loc[self.df['id'] == id, 'support'] = value
        self.df = self.initial_embedder(self.df)
        print('Updated Support Value -', self.df.loc[self.df['id'] == id, 'support'].values[0])