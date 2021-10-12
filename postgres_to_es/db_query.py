load_person_q = f'''SELECT DISTINCT id
                    FROM content.person
                    GROUP BY id
                    '''

load_person_role = f'''SELECT p.id, p.full_name, p.birth_date,
                    ARRAY_AGG(DISTINCT pfw.role) AS role,
                    ARRAY_AGG(DISTINCT pfw.film_work_id) AS film_ids
                    FROM content.person as p
                    LEFT JOIN content.person_film_work as pfw ON p.id = pfw.person_id
                    GROUP BY p.id
                    '''


load_film_id = f'''SELECT DISTINCT fw.id
                    FROM content.film_work as fw
                    LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
                    WHERE pfw.person_id IN (%s)
                    GROUP BY fw.id
                    '''

big_request = """ARRAY_AGG(DISTINCT jsonb_build_object('name', g.name, 'id', g.id)) AS genre,
ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'director') AS director,
ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers,
ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor') AS actors_names,
ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') AS writers_names"""

full_load = f'''SELECT DISTINCT fw.id, fw.title, fw.description, fw.rating, fw.type,
                          fw.updated_at, {big_request}
                            FROM content.film_work as fw
                            LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
                            LEFT JOIN content.person as p ON p.id = pfw.person_id
                            LEFT JOIN content.genre_film_work as gfw ON gfw.film_work_id = fw.id
                            LEFT JOIN content.genre as g ON g.id = gfw.genre_id
                            WHERE fw.id IN (%s)
                            GROUP BY fw.id
                            ORDER BY fw.updated_at;'''

query_all_genre = f'''SELECT id, name, description
                 FROM content.genre
                 ORDER BY created_at;'''
