import json
import logging
import time
from datetime import datetime

from elasticsearch import Elasticsearch
from utils import backoff
from state import State, JsonFileStorage


logger = logging.getLogger('ESLoader')


class EsSaver:
    def __init__(self, host: list, state_key='my_key'):
        self.client = Elasticsearch(host)
        self.movies_list = []
        self.key = state_key

    @backoff()
    def create_index(self, file_path) -> None:
        with open(file_path, 'r') as file:
            f = json.load(file)
        if self.client.indices.exists(index="movies"):
            logger.warning(f'{datetime.now()}\n\nindex movies already exist:')

        self.client.index(index='movies', body=f)
        
    @backoff()  # перенес по отдельности: для подключения и загрузки, что бы backoff отрабатывал в нужном месте
    def load_data(self) -> None:
        self.client.bulk(body='\n'.join(self.movies_list) + '\n', index='movies', refresh=True)

    def load(self, query) -> None:
        while query:
            rows = iter(query)
            for row in rows:
                self.movies_list.extend(
                    [
                        json.dumps(
                            {
                                'index': {
                                    '_index': 'movies',
                                    '_id': row['id']
                                }
                            }
                        ),
                        json.dumps(row),
                    ]
                )
                if len(self.movies_list) == 50:
                    self.load_data()
                    self.movies_list.clear()
            self.load_data()
            break
        State(JsonFileStorage('states/PostgresData.txt')).set_state(str(self.key), value=str(datetime.now()))
