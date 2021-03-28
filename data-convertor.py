import pandas as pd
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='username', password='password')

cluster = Cluster(['host'], port=9042, auth_provider=auth_provider)
session = cluster.connect('keyspace')

c_size = 100_000

for data_chunk in pd.read_csv('../data/data.tsv', sep='\t', chunksize=c_size, error_bad_lines=False, encoding='utf8'):

    df = data_chunk.groupby(['product_id','product_title','review_date'])['review_id'].count().reset_index()
    df.rename(columns = {'review_id':'reviews_amount'}, inplace = True)

    for index, row in df.iterrows():
        print('top_N_products_by_date record ', index)

        #top_N_products_by_date
        try:
            session.execute(
                """
                INSERT INTO top_N_products_by_date (review_date, product_id, reviews_amount, product_title)
                VALUES (%s, %s, %s, %s)
                IF NOT EXISTS
                """, 
                (row['review_date'], row['product_id'], row['reviews_amount'], row['product_title'])
            )
            print(row['review_date'], ' ', index)
        except cassandra.InvalidRequest:
            pass

    print('Chunk Q4 loaded')
    
    df = data_chunk.groupby(['customer_id', 'review_date'])['review_id'].count().reset_index()
    df.rename(columns = {'review_id':'reviews_amount'}, inplace = True)

    for index, row in df.iterrows():
        print('top_N_customers_by_date record ', index)

        #top_N_customers_by_date
        try:
            session.execute(
                """
                INSERT INTO top_N_customers_by_date (review_date, customer_id, reviews_amount)
                VALUES (%s, %s, %s)
                IF NOT EXISTS
                """, 
                (row['review_date'], row['customer_id'], row['reviews_amount'])
            )
            print(row['review_date'], ' ', index)
        except cassandra.InvalidRequest:
            pass

    print('Chunk Q5 loaded')
    
    df = data_chunk[data_chunk['star_rating'] < 3]
    df = df.groupby(['customer_id', 'review_date'])['review_id'].count().reset_index()
    df.rename(columns = {'review_id':'bad_reviews_amount'}, inplace = True)

    for index, row in df.iterrows():
        print('top_N_haters_by_date record ', index)

        #top_N_haters_by_date
        try:
            session.execute(
                """
                INSERT INTO top_N_haters_by_date (review_date, customer_id, bad_reviews_amount)
                VALUES (%s, %s, %s)
                IF NOT EXISTS
                """, 
                (row['review_date'], row['customer_id'], row['bad_reviews_amount'])
            )
            print(row['review_date'], ' ', index)
        except cassandra.InvalidRequest:
            pass

    print('Chunk Q7 loaded')
    
    df = data_chunk[data_chunk['star_rating'] < 3]
    df = df.groupby(['customer_id', 'review_date'])['review_id'].count().reset_index()
    df.rename(columns = {'review_id':'good_reviews_amount'}, inplace = True)

    for index, row in df.iterrows():
        print('top_N_backers_by_date record ', index)

        #top_N_backers_by_date
        try:
            session.execute(
                """
                INSERT INTO top_N_backers_by_date (review_date, customer_id, good_reviews_amount)
                VALUES (%s, %s, %s)
                IF NOT EXISTS
                """, 
                (row['review_date'], row['customer_id'], row['good_reviews_amount'])
            )
            print(row['review_date'], ' ', index)
        except cassandra.InvalidRequest:
            pass

    print('Chunk Q8 loaded')

    for index, row in data_chunk.iterrows():
        print('all_reviews_by_product_id record ', index)

        #all_reviews_by_product_id
        try:
            session.execute(
                """
                INSERT INTO all_reviews_by_product_id  (product_id, customer_id, review_id, review_date, review_headline, review_body, verified_purchase, star_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                IF NOT EXISTS
                """, 
                (row['product_id'], row['customer_id'], row['review_id'], row['review_date'], row['review_headline'], row['review_body'], row['verified_purchase'], row['star_rating'])
            )
            print(row['product_id'], ' ', index)
        except cassandra.InvalidRequest:
            pass

        print('all_reviews_by_customer_id record ', index)

        #all_reviews_by_customer_id
        try:
            session.execute(
                """
                INSERT INTO all_reviews_by_customer_id  (product_id, customer_id, review_id, review_date, review_headline, review_body, verified_purchase, star_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                IF NOT EXISTS
                """, 
                (row['product_id'], row['customer_id'], row['review_id'], row['review_date'], row['review_headline'], row['review_body'], row['verified_purchase'], row['star_rating'])
            )
            print(row['customer_id'], ' ', index)
        except cassandra.InvalidRequest:
            pass

    print('Chunk Q1+Q2+Q3 loaded')

# Recreate Q2 with PRIMARY KEY((product_is, star_rating)) to avoid filtering
# Q1, Q2
#query = "CREATE TABLE all_reviews_by_product_id (product_id VARCHAR, customer_id INT, review_date DATE, review_headline VARCHAR, review_body TEXT, verified_purchase VARCHAR, star_rating SMALLINT, PRIMARY KEY ((product_id), review_date)) WITH CLUSTERING ORDER BY (review_date DESC)"
#session.execute(query)

#Q3
#query = "CREATE TABLE all_reviews_by_customer_id (customer_id INT, product_id VARCHAR, review_id VARCHAR, review_date DATE, review_headline VARCHAR, review_body TEXT, verified_purchase VARCHAR, star_rating SMALLINT, PRIMARY KEY ((customer_id), review_date)) WITH CLUSTERING ORDER BY (review_date DESC)"

#Q4
#query = "CREATE TABLE top_N_products_by_date (review_date DATE, product_id VARCHAR, reviews_amount INT, product_title VARCHAR, PRIMARY KEY ((review_date), reviews_amount)) WITH CLUSTERING ORDER BY (reviews_amount DESC)"

#Q5
#query = "CREATE TABLE top_N_customers_by_date (review_date DATE, customer_id INT, reviews_amount INT, PRIMARY KEY ((reviews_date), reviews_amount)) WITH CLUSTERING ORDER BY (reviews_amount DESC)"

#Q7
#query = "CREATE TABLE top_N_haters_by_date (review_date DATE, customer_id INT, bad_reviews_amount INT, PRIMARY KEY ((review_date), bad_reviews_amount)) WITH CLUSTERING ORDER BY (bad_reviews_amount DESC)"

#Q8
#query = "CREATE TABLE top_N_backers_by_date (review_date DATE, customer_id INT, good_reviews_amount INT, PRIMARY KEY ((review_date), good_reviews_amount)) WITH CLUSTERING ORDER BY (good_reviews_amount DESC)"