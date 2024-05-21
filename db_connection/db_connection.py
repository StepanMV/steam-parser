import psycopg2
from psycopg2 import sql
import json

class DBConnection:
    def __init__(self, host: str, port: int, db_name: str, user: str, password: str):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = psycopg2.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port)
        
        self.translation_data = {
            'genres': self.get_genres(),
            'tags': self.get_tags(),
            'publishers': self.get_publishers(),
            'developers': self.get_developers()
        }
        
        self.game_data = {
            'genres': self.get_game_genres(),
            'tags': self.get_game_tags(),
            'publishers': self.get_game_publishers(),
            'developers': self.get_game_developers()
        }


    def get_game_info(self, steam_id: int) -> list[dict]:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("""
                SELECT 
                    g.steam_id,
                    g.title, 
                    g.link,
                    g.available, 
                    g.release_date, 
                    g.supports_win, 
                    g.supports_linux, 
                    g.supports_mac, 
                    g.positive_reviews, 
                    g.total_reviews,
                    array_agg(DISTINCT gnr.genre_name) AS genres,
                    array_agg(DISTINCT tg.tag_name) AS tags,
                    array_agg(DISTINCT dev.developer_name) AS developers,
                    array_agg(DISTINCT pub.publisher_name) AS publishers,
                    ph.price_w_discount AS last_price
                FROM games g
                LEFT JOIN game_genres gg ON g.game_id = gg.game_id
                LEFT JOIN genres gnr ON gg.genre_id = gnr.genre_id
                LEFT JOIN game_tags gt ON g.game_id = gt.game_id
                LEFT JOIN tags tg ON gt.tag_id = tg.tag_id
                LEFT JOIN game_developers gd ON g.game_id = gd.game_id
                LEFT JOIN developers dev ON gd.developer_id = dev.developer_id
                LEFT JOIN game_publishers gp ON g.game_id = gp.game_id
                LEFT JOIN publishers pub ON gp.publisher_id = pub.publisher_id
                LEFT JOIN LATERAL (
                    SELECT ph1.price_w_discount, ph1.date_time
                    FROM price_history ph1
                    WHERE ph1.game_id = g.game_id
                    ORDER BY ph1.date_time DESC
                    LIMIT 1
                ) ph ON true
                WHERE g.steam_id = %s
                GROUP BY 
                    g.game_id, 
                    g.title, 
                    g.available, 
                    g.release_date, 
                    g.supports_win, 
                    g.supports_linux, 
                    g.supports_mac, 
                    g.positive_reviews, 
                    g.total_reviews, 
                    ph.price_w_discount;
            """)
            
            # Executing the query
            cursor.execute(query, (steam_id,))
            
            # Fetching all results
            result = cursor.fetchone()
            if result is None:
                return None
            
            # Getting column names for better readability
            colnames = [desc[0] for desc in cursor.description]
            
            # Converting the results to a list of dictionaries
            games_info = dict(zip(colnames, result))

            return games_info
        
        except Exception as e:
            print(f"SQL Error on get_games_info: {e}")
        
        finally:
            cursor.close()
    
    def get_game_prices(self, game_id: int) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("""
                SELECT
                    price_wo_discount,
                    price_w_discount,
                    date_time
                FROM price_history
                WHERE game_id = %s
                ORDER BY date_time;
            """)
            
            # Executing the query
            cursor.execute(query, (game_id,))
            
            # Fetching all results
            results = cursor.fetchall()
            
            # Getting column names for better readability
            colnames = [desc[0] for desc in cursor.description]
            
            # Converting the results to a list of dictionaries
            dict_results = {colname: [] for colname in colnames}
            for row in results:
                for i, colname in enumerate(colnames):
                    dict_results[colname].append(row[i])
            

            return dict_results
        
        except Exception as e:
            print(f"SQL Error on get_game_prices: {e}")
        
        finally:
            cursor.close()
    

    def search_games(self, query: str = None, min_price: int = None,
                 max_price: int = None, min_year: int = None, max_year: int = None,
                 genres: list[str] = None, tags: list[str] = None, publishers: list[str] = None, developers: list[str] = None,
                 score: int = None, sort: str = 'score', sort_direction: str = 'DESC') -> list[dict]:
        cursor = self.conn.cursor()
        try:
            # Base query
            base_query = sql.SQL("""
                SELECT 
                    g.steam_id,
                    g.title, 
                    g.link,
                    g.available, 
                    g.release_date, 
                    g.supports_win, 
                    g.supports_linux, 
                    g.supports_mac, 
                    g.positive_reviews, 
                    g.total_reviews,
                    g.positive_reviews::float / g.total_reviews * 100 AS score,
                    array_agg(DISTINCT gnr.genre_name) AS genres,
                    array_agg(DISTINCT tg.tag_name) AS tags,
                    array_agg(DISTINCT dev.developer_name) AS developers,
                    array_agg(DISTINCT pub.publisher_name) AS publishers,
                    ph.price_w_discount AS last_price
                FROM games g
                LEFT JOIN game_genres gg ON g.game_id = gg.game_id
                LEFT JOIN genres gnr ON gg.genre_id = gnr.genre_id
                LEFT JOIN game_tags gt ON g.game_id = gt.game_id
                LEFT JOIN tags tg ON gt.tag_id = tg.tag_id
                LEFT JOIN game_developers gd ON g.game_id = gd.game_id
                LEFT JOIN developers dev ON gd.developer_id = dev.developer_id
                LEFT JOIN game_publishers gp ON g.game_id = gp.game_id
                LEFT JOIN publishers pub ON gp.publisher_id = pub.publisher_id
                LEFT JOIN LATERAL (
                    SELECT ph1.price_w_discount, ph1.date_time
                    FROM price_history ph1
                    WHERE ph1.game_id = g.game_id
                    ORDER BY ph1.date_time DESC
                    LIMIT 1
                ) ph ON true
            """)

            # Filters
            filters = []
            params = []

            if query:
                filters.append("g.title ILIKE %s")
                params.append(f"%{query}%")

            if min_price is not None:
                filters.append("ph.price_w_discount >= %s")
                params.append(min_price)
            if max_price is not None:
                filters.append("ph.price_w_discount <= %s")
                params.append(max_price)
            if min_year is not None:
                filters.append("EXTRACT(YEAR FROM g.release_date) >= %s")
                params.append(min_year)
            if max_year is not None:
                filters.append("EXTRACT(YEAR FROM g.release_date) <= %s")
                params.append(max_year)
            if score is not None:
                filters.append("g.positive_reviews::float / g.total_reviews * 100 >= %s AND g.total_reviews > 10")
                params.append(score)

            # Subqueries for genres, tags, developers, publishers
            if genres:
                genre_ids_query = """
                    SELECT gg.game_id
                    FROM game_genres gg
                    JOIN genres gnr ON gg.genre_id = gnr.genre_id
                    WHERE gnr.genre_name ILIKE ANY(%s)
                    GROUP BY gg.game_id
                """
                filters.append("g.game_id IN (" + genre_ids_query + ")")
                params.append(genres)
            
            if tags:
                tag_ids_query = """
                    SELECT gt.game_id
                    FROM game_tags gt
                    JOIN tags tg ON gt.tag_id = tg.tag_id
                    WHERE tg.tag_name ILIKE ANY(%s)
                    GROUP BY gt.game_id
                """
                filters.append("g.game_id IN (" + tag_ids_query + ")")
                params.append(tags)
            
            if developers:
                developer_ids_query = """
                    SELECT gd.game_id
                    FROM game_developers gd
                    JOIN developers dev ON gd.developer_id = dev.developer_id
                    WHERE dev.developer_name ILIKE ANY(%s)
                    GROUP BY gd.game_id
                """
                filters.append("g.game_id IN (" + developer_ids_query + ")")
                params.append(developers)
            
            if publishers:
                publisher_ids_query = """
                    SELECT gp.game_id
                    FROM game_publishers gp
                    JOIN publishers pub ON gp.publisher_id = pub.publisher_id
                    WHERE pub.publisher_name ILIKE ANY(%s)
                    GROUP BY gp.game_id
                """
                filters.append("g.game_id IN (" + publisher_ids_query + ")")
                params.append(publishers)

            where_clause = "WHERE " + " AND ".join(filters) if filters else ""

            # Adding sort column and direction to the query
            final_query = base_query + sql.SQL(where_clause) + sql.SQL("""
                GROUP BY 
                    g.game_id, 
                    g.title, 
                    g.available, 
                    g.release_date, 
                    g.supports_win, 
                    g.supports_linux, 
                    g.supports_mac, 
                    g.positive_reviews, 
                    g.total_reviews, 
                    ph.price_w_discount
                ORDER BY {sort} {sort_direction};
            """).format(
                sort=sql.Identifier(sort),
                sort_direction=sql.SQL(sort_direction.upper())
            )

            cursor.execute(final_query, params)
            results = cursor.fetchall()

            colnames = [desc[0] for desc in cursor.description]
            games_info = [dict(zip(colnames, result)) for result in results]
            return games_info
        except Exception as e:
            print(f"SQL Error on search_games: {e}")
            return []
        finally:
            cursor.close()


    def get_game_ids(self) -> list[int]:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT game_id FROM games")
            cursor.execute(query)
            results = cursor.fetchall()
            return [game_id for game_id, in results]
        except Exception as e:
            print(f"SQL Error on get_game_ids: {e}")
        finally:
            cursor.close()
    
    def get_genres(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT genre_id, genre_name FROM genres")
            cursor.execute(query)
            results = cursor.fetchall()
            return {genre_name: genre_id for genre_id, genre_name in results}
        except Exception as e:
            print(f"SQL Error on get_genres: {e}")
            return {}
        finally:
            cursor.close()
    
    def get_tags(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT tag_id, tag_name FROM tags")
            cursor.execute(query)
            results = cursor.fetchall()
            return {tag_name: tag_id for tag_id, tag_name in results}
        except Exception as e:
            print(f"SQL Error on get_tags: {e}")
            return {}
        finally:
            cursor.close()
    
    def get_publishers(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT publisher_id, publisher_name FROM publishers")
            cursor.execute(query)
            results = cursor.fetchall()
            return {publisher_name: publisher_id for publisher_id, publisher_name in results}
        except Exception as e:
            print(f"SQL Error on get_publishers: {e}")
            return {}
        finally:
            cursor.close()
            
    def get_developers(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT developer_id, developer_name FROM developers")
            cursor.execute(query)
            results = cursor.fetchall()
            return {developer_name: developer_id for developer_id, developer_name in results}
        except Exception as e:
            print(f"SQL Error on get_developers: {e}")
            return {}
        finally:
            cursor.close()
    
    def get_game_genres(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT game_id, array_agg(genre_id) FROM game_genres GROUP BY game_id")
            cursor.execute(query)
            results = cursor.fetchall()
            return {game_id: genre_ids for game_id, genre_ids in results}
        except Exception as e:
            print(f"SQL Error on get_game_genres: {e}")
            return {}
        finally:
            cursor.close()
    
    def get_game_tags(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT game_id, array_agg(tag_id) FROM game_tags GROUP BY game_id")
            cursor.execute(query)
            results = cursor.fetchall()
            return {game_id: tag_ids for game_id, tag_ids in results}
        except Exception as e:
            print(f"SQL Error on get_game_tags: {e}")
            return {}
        finally:
            cursor.close()
    
    def get_game_publishers(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT game_id, array_agg(publisher_id) FROM game_publishers GROUP BY game_id")
            cursor.execute(query)
            results = cursor.fetchall()
            return {game_id: publisher_ids for game_id, publisher_ids in results}
        except Exception as e:
            print(f"SQL Error on get_game_publishers: {e}")
            return {}
        finally:
            cursor.close()
    
    def get_game_developers(self) -> dict:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("SELECT game_id, array_agg(developer_id) FROM game_developers GROUP BY game_id")
            cursor.execute(query)
            results = cursor.fetchall()
            return {game_id: developer_ids for game_id, developer_ids in results}
        except Exception as e:
            print(f"SQL Error on get_game_developers: {e}")
            return {}
        finally:
            cursor.close()
            
    def add_developers(self, developers: list[str]) -> None:
        cursor = self.conn.cursor()
        try:
            values = [(developer,) for developer in developers]
            query = sql.SQL("INSERT INTO developers (developer_name) VALUES {}").format(
                sql.SQL(',').join(map(sql.Literal, values))
            )
            cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(f"SQL Error on add_developers: {e}")
        finally:
            cursor.close()
    
    def add_publishers(self, publishers: list[str]) -> None:
        cursor = self.conn.cursor()
        try:
            values = [(publisher,) for publisher in publishers]
            query = sql.SQL("INSERT INTO publishers (publisher_name) VALUES {}").format(
                sql.SQL(',').join(map(sql.Literal, values))
            )
            cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(f"SQL Error on add_publishers: {e}")
        finally:
            cursor.close()
    
    def add_genres(self, genres: list[str]) -> None:
        cursor = self.conn.cursor()
        try:
            values = [(genre,) for genre in genres]
            query = sql.SQL("INSERT INTO genres (genre_name) VALUES {}").format(
                sql.SQL(',').join(map(sql.Literal, values))
            )
            cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(f"SQL Error on add_genres: {e}")
        finally:
            cursor.close()
    
    def add_tags(self, tags: list[str]) -> None:
        cursor = self.conn.cursor()
        try:
            values = [(tag,) for tag in tags]
            query = sql.SQL("INSERT INTO tags (tag_name) VALUES {}").format(
                sql.SQL(',').join(map(sql.Literal, values))
            )
            cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(f"SQL Error on add_tags: {e}")
        finally:
            cursor.close()
            
    def update_translation_data(self, game_info: dict) -> None:
        new_genres = list(set(game_info['genres']) - set(self.translation_data['genres']))
        new_tags = list(set(game_info['tags']) - set(self.translation_data['tags']))
        new_developers = list(set(game_info['developers']) - set(self.translation_data['developers']))
        new_publishers = list(set(game_info['publishers']) - set(self.translation_data['publishers']))
        
        if new_genres:
            self.add_genres(new_genres)
        if new_tags:
            self.add_tags(new_tags)
        if new_developers:
            self.add_developers(new_developers)
        if new_publishers:
            self.add_publishers(new_publishers)
            
        if new_genres or new_tags or new_developers or new_publishers:
            new_translation_data = {
                'genres': self.get_genres(),
                'tags': self.get_tags(),
                'publishers': self.get_publishers(),
                'developers': self.get_developers()
            }
            if new_translation_data["genres"]:
                self.translation_data = new_translation_data
    
    def update_game_data(self, game_id: int, game_data: dict) -> None:
        self.game_data['genres'][game_id] = game_data['genres']
        self.game_data['tags'][game_id] = game_data['tags']
        self.game_data['publishers'][game_id] = game_data['publishers']
        self.game_data['developers'][game_id] = game_data['developers']
    
    def _process_game_genres(self, game_id: int, old_genres: list[int], new_genres: list[int]) -> None:
        cursor = self.conn.cursor()
        try:
            # Get the genres that need to be added
            genres_to_add = list(set(new_genres) - set(old_genres))
            # Get the genres that need to be removed
            genres_to_remove = list(set(old_genres) - set(new_genres))
            
            # Add the genres
            if genres_to_add:
                values = [(game_id, genre_id) for genre_id in genres_to_add]
                query = sql.SQL("INSERT INTO game_genres (game_id, genre_id) VALUES {}").format(
                    sql.SQL(',').join(map(sql.Literal, values))
                )
                cursor.execute(query)
            
            # Remove the genres
            if genres_to_remove:
                query = sql.SQL("DELETE FROM game_genres WHERE game_id = %s AND genre_id IN %s")
                cursor.execute(query, (game_id, tuple(genres_to_remove)))
                
            if genres_to_add or genres_to_remove:
                self.conn.commit()
            
        except Exception as e:
            print(f"SQL Error on process_game_genes: {e}")
            print(game_id, old_genres, new_genres)
        finally:
            cursor.close()
    
    def _process_game_tags(self, game_id: int, old_tags: list[int], new_tags: list[int]) -> None:
        cursor = self.conn.cursor()
        try:
            # Get the tags that need to be added
            tags_to_add = list(set(new_tags) - set(old_tags))
            # Get the tags that need to be removed
            tags_to_remove = list(set(old_tags) - set(new_tags))
            
            # Add the tags
            if tags_to_add:
                values = [(game_id, tag_id) for tag_id in tags_to_add]
                query = sql.SQL("INSERT INTO game_tags (game_id, tag_id) VALUES {}").format(
                    sql.SQL(',').join(map(sql.Literal, values))
                )
                cursor.execute(query)
            
            # Remove the tags
            if tags_to_remove:
                query = sql.SQL("DELETE FROM game_tags WHERE game_id = %s AND tag_id IN %s")
                cursor.execute(query, (game_id, tuple(tags_to_remove)))
                
            if tags_to_add or tags_to_remove:
                self.conn.commit()
            
        except Exception as e:
            print(f"SQL Error on process_game_tags: {e}")
        finally:
            cursor.close()
    
    def _process_game_publishers(self, game_id: int, old_publishers: list[int], new_publishers: list[int]) -> None:
        cursor = self.conn.cursor()
        try:
            # Get the publishers that need to be added
            publishers_to_add = list(set(new_publishers) - set(old_publishers))
            # Get the publishers that need to be removed
            publishers_to_remove = list(set(old_publishers) - set(new_publishers))
            
            # Add the publishers
            if publishers_to_add:
                values = [(game_id, publisher_id) for publisher_id in publishers_to_add]
                query = sql.SQL("INSERT INTO game_publishers (game_id, publisher_id) VALUES {}").format(
                    sql.SQL(',').join(map(sql.Literal, values))
                )
                cursor.execute(query)
            
            # Remove the publishers
            if publishers_to_remove:
                query = sql.SQL("DELETE FROM game_publishers WHERE game_id = %s AND publisher_id IN %s")
                cursor.execute(query, (game_id, tuple(publishers_to_remove)))
                
            if publishers_to_add or publishers_to_remove:
                self.conn.commit()
            
        except Exception as e:
            print(f"SQL Error on process_game_publishers: {e}")
        finally:
            cursor.close()
    
    def _process_game_developers(self, game_id: int, old_developers: list[int], new_developers: list[int]) -> None:
        cursor = self.conn.cursor()
        try:
            # Get the developers that need to be added
            developers_to_add = list(set(new_developers) - set(old_developers))
            # Get the developers that need to be removed
            developers_to_remove = list(set(old_developers) - set(new_developers))
            
            # Add the developers
            if developers_to_add:
                values = [(game_id, developer_id) for developer_id in developers_to_add]
                query = sql.SQL("INSERT INTO game_developers (game_id, developer_id) VALUES {}").format(
                    sql.SQL(',').join(map(sql.Literal, values))
                )
                cursor.execute(query)
            
            # Remove the developers
            if developers_to_remove:
                query = sql.SQL("DELETE FROM game_developers WHERE game_id = %s AND developer_id IN %s")
                cursor.execute(query, (game_id, tuple(developers_to_remove)))

            if developers_to_add or developers_to_remove:
                self.conn.commit()
            
        except Exception as e:
            print(f"SQL Error on process_game_developers: {e}")
        finally:
            cursor.close()
    
    def _process_game_price(self, game_id: int, price_wo_discount: float, price_w_discount: float) -> None:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("""
                SELECT price_wo_discount, price_w_discount
                FROM price_history
                WHERE game_id = %s
                ORDER BY date_time DESC
                LIMIT 1;
            """)
            cursor.execute(query, (game_id,))
            result = cursor.fetchone()
            if result is None or float(result[0]) != price_wo_discount or float(result[1]) != price_w_discount:
                query = sql.SQL("""
                    INSERT INTO price_history (game_id, price_wo_discount, price_w_discount, date_time)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP);
                """)
                cursor.execute(query, (game_id, price_wo_discount, price_w_discount))
                self.conn.commit()
        except Exception as e:
            print(f"SQL Error on process_game_price: {e}")
        finally:
            cursor.close()


    def add_or_update_game_info(self, game_info: dict) -> int:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("""
                INSERT INTO games (steam_id, title, link, available, release_date, supports_win, supports_linux, supports_mac, positive_reviews, total_reviews)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (steam_id) DO UPDATE
                SET title = EXCLUDED.title,
                    link = EXCLUDED.link,
                    available = EXCLUDED.available,
                    release_date = EXCLUDED.release_date,
                    supports_win = EXCLUDED.supports_win,
                    supports_linux = EXCLUDED.supports_linux,
                    supports_mac = EXCLUDED.supports_mac,
                    positive_reviews = EXCLUDED.positive_reviews,
                    total_reviews = EXCLUDED.total_reviews
                RETURNING game_id;
            """)
            
            cursor.execute(query, (game_info["steam_id"], game_info["title"], game_info["link"], game_info["available"], game_info["release_date"],
                                game_info["supports_win"], game_info["supports_linux"], game_info["supports_mac"],
                                game_info["positive_reviews"], game_info["total_reviews"]))
            game_id = cursor.fetchone()[0]

            self.conn.commit()

            old_genres = self.game_data["genres"].get(game_id, [])
            self._process_game_genres(game_id, old_genres, game_info["genres"])
            old_tags = self.game_data["tags"].get(game_id, [])
            self._process_game_tags(game_id, old_tags, game_info["tags"])
            old_publishers = self.game_data["publishers"].get(game_id, [])
            self._process_game_publishers(game_id, old_publishers, game_info["publishers"])
            old_developers = self.game_data["developers"].get(game_id, [])
            self._process_game_developers(game_id, old_developers, game_info["developers"])
            self._process_game_price(game_id, game_info["price_wo_discount"], game_info["price_w_discount"])
            return game_id

        except Exception as e:
            print(f"SQL Error on add_or_update_game_info: {e}")
            return None
        finally:
            cursor.close()

    
    def set_unavailable_games(self, game_ids: list[int]) -> None:
        cursor = self.conn.cursor()
        try:
            query = sql.SQL("""
                UPDATE games
                SET available = FALSE
                WHERE game_id IN %s;
            """)
            cursor.execute(query, (tuple(game_ids),))
            self.conn.commit()
        except Exception as e:
            print(f"SQL Error on set_unavailable_games: {e}")
        finally:
            cursor.close()
