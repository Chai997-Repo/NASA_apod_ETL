from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


# Define the DAF
with DAG(
    dag_id = 'nasa-apod-postgres',
    start_date = days_ago(1), # it tell run every day within this dag
    schedule = '@daily',
    catchup = False

) as dag:
    
    # Step-1: Create an table if it doesn't exists
    @task
    def create_table():
        # Initialize the Postgres Hook
        postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data(
        id SERIAL PRIMARY KEY,
        title VARCHAR(225),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(50)
        );

        """

        ## Executing the table create query
        postgres_hook.run(create_table_query)

    ## Step-2: Extract the NASA API DATA(APOD DATA) - Astronomy picture of the day (Extract pipeline)
    ## url - https://api.nasa.gov/planetary/apod?api_key=JhDrRnWGh8bqpCgRKN0pnm8pEXcNajR69ed9dGI8

    extract_apod = SimpleHttpOperator(
        task_id = 'extract_apod',
        http_conn_id = 'nasa_api',  # once airflow starts running there is an something to put connections we need to match that connection id with this conn_id we are defining here
        endpoint = 'planetary/apod',  #NASA API endpoint for APOD Image
        method = 'GET',
        data = {"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"},  #conn.nasa_api - the connection we create in airflow #api_key- is coming from airflow connection string and we are using the API KEY
        response_filter = lambda response:response.json(), # we will convert all our response into JSON
        

    )

    ## Step-3: Transform the data (Picking only the information that is necessary for our analysis)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data
    
    ## Step-4 : Load the data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the PostgresHook to load the data 
        postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        ## Now we are Inserting the data into postgres table we created using INsert query

        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES(%s, %s, %s, %s, %s)

        """
        ## Execute the SQL Insert query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    ## Step-5 : Task Dependencies
    create_table() >> extract_apod  # we need to ensure something  db to store the extracted data 
    api_response = extract_apod.output # there will be another varible inside extract_apod which is from SimpleHttpOperator
    transformed_data = transform_apod_data(api_response) # Transform Step
    transformed_data >> load_data_to_postgres(transformed_data) # Load Step, # Fixed: Pass the actual transformed_data variable

