import json
import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from utils import backoff


logger = logging.getLogger('ESLoader')


class EsSaver:
    def __init__(self, host: list, state_key='my_key'):
        self.client = Elasticsearch(host)
        self.movies_list = []
        self.key = state_key

    @backoff()
    def create_index(self, file_path, name_index) -> None:
        with open(file_path, 'r') as file:
            f = json.load(file)
        if self.client.indices.exists(index=name_index):
            logger.warning(f'{datetime.now()}\n\nindex movies already exist:')

        self.client.index(index=name_index, body=f)

    @backoff()
    def load_data(self, name_index) -> None:
        self.client.bulk(body='\n'.join(self.movies_list) + '\n', index=name_index, refresh=True)

    def load(self, query, name_index) -> None:
        while query:
            rows = iter(query)
            for row in rows:
                self.movies_list.extend(
                    [
                        json.dumps(
                            {
                                'index': {
                                    '_index': name_index,
                                    '_id': row['id']
                                }
                            }
                        ),
                        json.dumps(row),
                    ]
                )
                if len(self.movies_list) == 50:
                    self.load_data(name_index)
                    self.movies_list.clear()
            self.load_data(name_index)
            break
