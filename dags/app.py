#dag - directed acyclic graph

#tasks : 1) fetch bol.com data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies
import json
from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#1) fetch bol.com data (extract) 2) clean data (transform), Not really needed in this case




def get_bol_data_books(ti):
        
    URL = "https://www.bol.com/nl/nl/s/?searchtext=data+engineering+books"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    results = soup.find("main",{"id": "mainContent"})
    book_group = results.find("ul", {"id": "js_items_content"})

    books = book_group.find_all("li", {"class":"product-item--row"})

    collected_books = []

    for book in books:
        #price
        product_price = book.find("div",{"class":"product-prices"})
        product_price = product_price.find("div",{"class":"price-block__highlight"})

        price = product_price.find("span",{"data-test":"price"}).text.strip()
        price = price.split("\n")
        no_price = price[0].strip()
        dec_price = price[1].strip()
        if dec_price == "-":
            dec_price=0
        price = f"{no_price}.{dec_price}"
        price = float(price)
    
        product_info = book.find("div", {"class": "product-item__info"})
        product_creator = product_info.find("ul", {"class": "product-creator"})
        creators = product_creator.find_all("a", {"data-test": "party-link"})

        #authors
        authors = []

        if len(creators) > 1:
            # Append all authors from the list
            for i in range(len(creators)):
                authors.append(creators[i].text)
        elif len(creators) == 1:
            authors.append(creators[0].text)

        # Join the list into a string for printing
        authors_str = ", ".join(authors)
        # print(authors_str)

        #title
        product_title = product_info.find("div",{"class":"product-title--inline"})
        title = product_title.find("a", {"class":"product-title"}).text

        collected_books.append(
            {
                "Title": title,
                "Author": authors_str,
                "Price": price
            }
        )
    
    df = pd.DataFrame(collected_books)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    # Push the DataFrame to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))





#3) create and store data in table on postgres (load)
    
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")


    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price)
    VALUES (%s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_bol.com_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Bol.com and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT
    );
    """,
    dag=dag,
)


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_bol_data_books,
    dag=dag,
)


insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

create_table_task >> fetch_book_data_task >> insert_book_data_task


