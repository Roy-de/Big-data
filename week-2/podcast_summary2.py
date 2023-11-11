from datetime import datetime

from airflow.decorators import dag, task
import pendulum
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

import requests


@dag(
    dag_id='podcast_summary2',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 11, 10),
    catchup=False
)
def podcast_summary2():
    def get_access_token():
        url = "https://accounts.spotify.com/api/token"

        payload_token = 'grant_type=client_credentials&client_id=6c76fd3b93054d029a03ead7c8af209b&client_secret' \
                        '=328033b94c5442afa08cdbf2c01f9b5d'
        headers_token = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': '__Host-device_id=AQCO6JiTcibHHoOE'
                      '-gJkEVd8WOIrQDPveZGS22sVfZaGqQw5QkCT52RJ_PKRZDoMCJWpklW8K2JkZJFVbsfXWT-gAdKM_PXPFw0; '
                      '__Host-sp_csrf_sid=2aebf09ec16b3a18e6aa1d3f487f8875f88d64f668888da65443647305d416db; sp_tr=false'
        }

        response_token = requests.post(url, headers=headers_token, data=payload_token)
        return response_token.json().get('access_token')

    SPOTIFY_URL = "https://api.spotify.com/v1/shows/6olvQhNhQwMbGG26t3rVgM"
    payload = {}
    headers = {
        'Authorization': 'Bearer ' + get_access_token()}

    create_database = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="podcasts",
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes(
            episode_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            duration_ms INTEGER,
            explicit INTEGER, -- Use 0 for false, 1 for true
            external_url TEXT,
            html_description TEXT,
            release_date DATE,
            release_date_precision TEXT,
            type TEXT,
            uri TEXT,
            is_externally_hosted INTEGER, -- Use 0 for false, 1 for true
            is_playable INTEGER, -- Use 0 for false, 1 for true
            language TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CHECK (release_date_precision IN ('year', 'month', 'day'))
        )
        """
    )

    @task()
    def get_episodes():
        """
        We will be getting the shxtsNGigs podcast from spotify
        Let's first get the url to use
        :return: episodes
        """
        response = requests.get(url=SPOTIFY_URL, headers=headers, data=payload)
        episodes = response.json().get('episodes')
        print(f"episodes are of {type(episodes)}")
        print(episodes)
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)

    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored = hook.get_pandas_df("SELECT * FROM episodes")
        new_episodes = []
        for episode in episodes['items']:
            new_episodes.append((
                episode.get('name'),
                episode.get('description'),
                episode.get('duration_ms'),
                episode.get('explicit'),
                episode.get('external_urls').get('spotify'),
                episode.get('html_description'),
                episode.get('release_date'),
                episode.get('release_date_precision'),
                episode.get('type'),
                episode.get('uri'),
                episode.get('is_externally_hosted'),
                episode.get('is_playable'),
                episode.get('language'),
                datetime.utcnow(),  # created_at
                datetime.utcnow()  # updated_at
            ))

        if new_episodes:
            hook.insert_rows(table="episodes", rows=new_episodes,
                             target_fields=["name", "description", "duration_ms", "explicit", "external_url",
                                            "html_description", "release_date", "release_date_precision", "type", "uri",
                                            "is_externally_hosted", "is_playable", "language", "created_at",
                                            "updated_at"])

    load_episodes(podcast_episodes)


dag = podcast_summary2()
