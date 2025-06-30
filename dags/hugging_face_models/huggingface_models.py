from datetime import datetime, timedelta

from airflow.sdk import dag, task

from airflow.providers.postgres.hooks.postgres import PostgresHook # Import PostgresHook for database connections
from huggingface_hub import list_models  # Import Huggingface Hub API to list models


default_args = {
    'owner': 'fayad',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
        default_args=default_args,
        dag_id='huggingface_models_etl',
        description='A simple ETL process to fetch and process model data from Huggingface Hub',
        schedule=timedelta(minutes=1),
        start_date=datetime(2025, 6, 2, 0, 0, 0),
        catchup=True,
)
def huggingface_models_dag():
    @task()
    def fetch_huggingface_models():
        print("Fetching models from Huggingface Hub...")
        try:
            models = list_models(sort='lastModified', direction=-1, limit=50)  # Fetch top 10 models sorted by lastModified
            model_data = [
                {
                    'id': m.id,
                    'downloads': m.downloads,
                    'author': m.author or None,
                    'tags': m.tags or [],
                    'likes': m.likes or 0,
                    'pipeline_tag': m.pipeline_tag or None,
                    'lastModified': m.lastModified,
                }
                for m in models
            ]
            return model_data
        except Exception as e:
            print(f"Error fetching models: {e}")
            return []

    @task()
    def process_models(fetched_data):
        print("Processing fetched models...")
        try:
            seen = set()
            if not fetched_data:
                print("No data fetched to process.")
                return []

            processed_data = []
            for model in fetched_data:
                if model['id'] in seen:
                    print(f"Duplicate model found: {model['id']}, skipping.")
                    continue
                seen.add(model['id'])
                processed_model = {
                    'id': model['id'],
                    'downloads': model['downloads'] or 0,
                    'author': model['author'] or 'Unknown',
                    'tags': ', '.join(model['tags']) or '',
                    'likes': model['likes'] or 0,
                    'pipeline_tag': model['pipeline_tag'] or 'Unknown',
                    'lastModified': model['lastModified'].isoformat() if model['lastModified'] else None,
                }
                processed_data.append(processed_model)
            return processed_data
        except Exception as e:
            print(f"Error processing models: {e}")
            return []

    @task()
    def store_models(processed_data):

        print("Storing processed models in the database...")
        try:
            if not processed_data:
                print("No data to store.")
                return

            pg_hook = PostgresHook(postgres_conn_id='postgres_home_connection_node2')  # Replace with your connection ID
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS huggingface_models (
                    id TEXT PRIMARY KEY,
                    downloads INTEGER,
                    author TEXT,
                    tags TEXT,
                    likes INTEGER,
                    pipeline_tag TEXT,
                    lastModified TIMESTAMP
                );
                """
            )

            for model in processed_data:
                cursor.execute(
                    """
                    INSERT INTO huggingface_models (id, downloads, author, tags, likes, pipeline_tag, lastModified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        downloads = EXCLUDED.downloads,
                        author = EXCLUDED.author,
                        tags = EXCLUDED.tags,
                        likes = EXCLUDED.likes,
                        pipeline_tag = EXCLUDED.pipeline_tag,
                        lastModified = EXCLUDED.lastModified;
                    """,
                    (
                        model['id'],
                        model['downloads'],
                        model['author'],
                        model['tags'],
                        model['likes'],
                        model['pipeline_tag'],
                        model['lastModified']
                    )
                )
            conn.commit()
            cursor.close()
            print("Models stored successfully.")
        except Exception as e:
            print(f"Error storing models: {e}")

    fetched_data = fetch_huggingface_models()
    processed_data = process_models(fetched_data)
    store_models(processed_data)

dag = huggingface_models_dag()