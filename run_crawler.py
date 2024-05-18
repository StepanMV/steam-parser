from steam_crawler import SteamCrawler
from db_connection import DBConnection
from datetime import datetime
import asyncio

def steam_date_to_postgres_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%d %b, %Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        pass

    try:
        date_obj = datetime.strptime(date_str, "%b %Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        pass

    try:
        quarter_str, year = date_str.split()
        quarter_start_month = {
            'Q1': 1,
            'Q2': 4,
            'Q3': 7,
            'Q4': 10
        }
        start_month = quarter_start_month[quarter_str]
        date_obj = datetime.strptime(f"{year}-{start_month:02d}-01", "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%d")
    except (ValueError, KeyError):
        pass

    return None


def sanitize_data(data: dict, translation_data: dict) -> dict:
    genre_ids = [translation_data['genres'][genre] for genre in data['genres']]
    tag_ids = [translation_data['tags'][tag] for tag in data['tags']]
    developer_ids = [translation_data['developers'][developer] for developer in data['developers']]
    publisher_ids = [translation_data['publishers'][publisher] for publisher in data['publishers']]
    data['genres'] = genre_ids
    data['tags'] = tag_ids
    data['developers'] = developer_ids
    data['publishers'] = publisher_ids
    data['available'] = True
    if data['release_date']:
        data['release_date'] = steam_date_to_postgres_date(data['release_date'])
    return data


async def main():
    loop = asyncio.get_event_loop()
    steam_crawler = SteamCrawler()
    db_connection = DBConnection("localhost", 5432, "steam", "twinkboy42", "0847")
    game_ids = set(db_connection.get_game_ids())
    processed_game_ids = set()
    loop.create_task(steam_crawler.run())
    while steam_crawler.games_processed < steam_crawler.total_games:
        if steam_crawler.datastream:
            data = steam_crawler.datastream.pop(0)
            if data['game_id'] in processed_game_ids:
                continue
            db_connection.update_translation_data(data)
            data = sanitize_data(data, db_connection.translation_data)
            db_connection.add_or_update_game_info(data)
            db_connection.update_game_data(data)
            processed_game_ids.add(data['game_id'])
        else:
            print(steam_crawler.games_processed, steam_crawler.total_games)
            await asyncio.sleep(1)
    unavailable_game_ids = list(game_ids - processed_game_ids)
    db_connection.set_unavailable_games(unavailable_game_ids)
    db_connection.conn.close()


if __name__ == "__main__":
    asyncio.run(main())