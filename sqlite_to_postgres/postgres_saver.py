from psycopg2.extensions import connection as _connection


class PostgresSaver:
    """Класс для сохранения данных в бд
    Принимает словарь из sqllite_loader.py со значениями из dataclasses
    проходит циклом, каждые 100 записей добавляет в postgres. Пришлось сделать дублирование кода,
    что бы добавлять последние записи.
    В postgres добавляются данные из методов, соответствующих названиям таблиц
    """

    def __init__(self, pg_conn: _connection):
        self.conn = pg_conn
        self.cursor = self.conn.cursor()
        self.counter = 0

    def mogrify_create(self, data) -> list:
        ss = " ,%s"
        args = ','.join(
            self.cursor.mogrify(
                f"(%s{ss * (len(data[0].__dict__) - 1)})",
                list(value for _, value in item.__dict__.items())
            ).decode() for item in data
        )
        return args

    def save_all_data(self, data: dict) -> bool:
        for table in data:
            batch = 100
            for i in range(0, len(data[table]) + batch, batch):
                data_save = data[table][i: i + batch]
                if data_save:
                    self.cursor.execute(f"""
                        INSERT INTO content.{table} ({', '.join(i for i in data[table][0].__annotations__)})
                        VALUES {self.mogrify_create(data_save)}
                        ON CONFLICT (id) DO NOTHING
                        """)
                    self.conn.commit()
        return True
