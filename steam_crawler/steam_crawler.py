from bs4 import BeautifulSoup
import aiohttp
import asyncio
import io
import json

class SteamCrawler:

    def __init__(self):
        self.search_url = "https://store.steampowered.com/search/?filter=topsellers"
        self.scroll_url = "https://store.steampowered.com/search/results/?query=&start=NUM&count=50&filter=topsellers&infinite=true"
        self.agecheck_url = "https://store.steampowered.com/agecheckset/app/NUM/"
        self.datastream = []
        self.games_processed = 0
        self.total_games = 1

    async def _fetch_url_content(self, url: str) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.text()
                if response.status != 200:
                    raise Exception(f"Failed to fetch content from {url}")
                return content
    
    async def _fetch_game_content(self, url: str) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.text()
                if response.status != 200:
                    raise Exception(f"Failed to fetch content from {url}")
                # check if age check is required (div with class "age_gate")
                soup = BeautifulSoup(content, 'html.parser')
                agecheck_div = soup.find("div", {"class": "age_gate"})
                if not agecheck_div:
                    return content
                agecheck_url = self.agecheck_url.replace("NUM", url.split('/')[-3])
                session_id = session.cookie_jar.filter_cookies("https://store.steampowered.com")["sessionid"].value
                data = {"ageDay": "13", "ageMonth": "January", "ageYear": "1995", "sessionid": session_id}
                async with session.post(agecheck_url, data=data) as response:
                    content = await response.text()
                    if response.status != 200:
                        raise Exception(f"Failed to fetch content from {url}")
                    async with session.get(url) as response:
                        content = await response.text()
                        if response.status != 200:
                            raise Exception(f"Failed to fetch content from {url}")
                        return content


    async def fetch_game_pages(self, urls: list[str]) -> list[str]:
        tasks = []
        for url in urls:
            tasks.append(self._fetch_game_content(url))
        return await asyncio.gather(*tasks)
    
    async def fetch_search_page(self) -> str:
        page = await self._fetch_url_content(self.search_url)
        return page
    
    async def fetch_scroll_page(self, num: int) -> str:
        sub_content = await self._fetch_url_content(self.scroll_url.replace("NUM", str(num * 50)))
        sub_content = json.loads(sub_content)
        if not sub_content["success"]:
            raise Exception("Failed to fetch additional content")
        sub_content = sub_content["results_html"]
        
        sub_content_eval = io.StringIO()
        print(sub_content, file=sub_content_eval)
        sub_content = sub_content_eval.getvalue()
        sub_content_eval.close()
        return sub_content
    
    def append_search_page(self, search_page: str, scroll_page: str) -> str:
        return search_page.replace("<!-- End List Items -->", scroll_page)
    
    def get_game_urls(self, page: str) -> list[str]:
        soup = BeautifulSoup(page, 'html.parser')
        links = soup.select("a.search_result_row")
        return [link['href'] for link in links if 'href' in link.attrs]

    def get_game_ids(self, urls: list[str]) -> list[int]:
        return [int(url.split('/')[-3]) for url in urls]

    def _get_game_info_main(self, search_page: str, appid: int) -> dict:
        soup = BeautifulSoup(search_page, 'html.parser')
        game_info = {}
        a_tag = soup.find('a', {'data-ds-itemkey': f'App_{appid}'})
        title_span = a_tag.find('span', class_='title')
        game_info['title'] = title_span.text
        win_span = a_tag.find('span', class_='platform_img win')
        mac_span = a_tag.find('span', class_='platform_img mac')
        linux_span = a_tag.find('span', class_='platform_img linux')
        game_info['supports_win'] = bool(win_span)
        game_info['supports_mac'] = bool(mac_span)
        game_info['supports_linux'] = bool(linux_span)
        original_price_div = a_tag.find('div', class_='discount_original_price')
        discount_price_div = a_tag.find('div', class_='discount_final_price')
        if original_price_div:
            price_wo_discount = float(original_price_div.text[:-1].replace(',', '.')) if original_price_div.text != 'Free' else 0
            price_w_discount = float(discount_price_div.text[:-1].replace(',', '.')) if discount_price_div.text != 'Free' else 0
            game_info['price_wo_discount'] = price_wo_discount
            game_info['price_w_discount'] = price_w_discount
        else:
            price_w_discount = float(discount_price_div.text[:-1].replace(',', '.')) if discount_price_div.text != 'Free' else 0
            game_info['price_wo_discount'] = price_w_discount
            game_info['price_w_discount'] = price_w_discount
        return game_info
    
    def _get_game_info_detail(self, page: str) -> dict:
        soup = BeautifulSoup(page, 'html.parser')
        game_info = {}
        glance_block = soup.find('div', class_='glance_ctn')
        release_date = glance_block.find('div', class_='date')
        game_info['release_date'] = release_date.text if release_date else None

        developers_list_div = glance_block.find('div', class_='dev_row')
        developers = [developer.text for developer in developers_list_div.find_all('a')] if developers_list_div else []
        game_info['developers'] = developers

        publishers_list_div = developers_list_div.find_next('div', class_='dev_row')
        publishers = [publisher.text for publisher in publishers_list_div.find_all('a')] if publishers_list_div else []
        game_info['publishers'] = publishers

        tags_div = glance_block.find('div', class_='glance_tags_ctn')
        tags = [tag.text.strip() for tag in tags_div.find_all('a')] if tags_div else []
        game_info['tags'] = tags

        genres_block = soup.find('div', id='genresAndManufacturer').find('span')
        genres = [genre.text for genre in genres_block.find_all('a')] if genres_block else []
        game_info['genres'] = genres
        
        reviews_div = soup.find('div', id='reviews_filter_options')
        if not reviews_div:
            game_info['total_reviews'] = None
            game_info['positive_reviews'] = None
            return game_info
        
        total_reviews_span = reviews_div.find('span', class_='user_reviews_count')
        total_reviews = int(total_reviews_span.text[1:-1].replace(',', ''))
        game_info['total_reviews'] = total_reviews

        positive_reviews_span = total_reviews_span.find_next('span', class_='user_reviews_count')
        positive_reviews = int(positive_reviews_span.text[1:-1].replace(',', ''))
        game_info['positive_reviews'] = positive_reviews
        return game_info
    
    def get_game_info(self, search_page: str, game_page: str, appid: int) -> dict:
        game_info_main = self._get_game_info_main(search_page, appid)
        game_info_detail = self._get_game_info_detail(game_page)
        return {**game_info_main, **game_info_detail, "steam_id": appid}
    
    async def get_games_info(self, i: int) -> list[dict]:
        scroll_page = await self.fetch_scroll_page(i)
        game_urls = self.get_game_urls(scroll_page)
        game_ids = self.get_game_ids(game_urls)
        game_pages = await self.fetch_game_pages(game_urls)
        games_info = []
        for game_id, game_page, game_url in zip(game_ids, game_pages, game_urls):
            try:
                game_info = self.get_game_info(scroll_page, game_page, game_id)
                game_info['link'] = game_url
                games_info.append(game_info)
            except AttributeError as e:
                print(f"Failed to fetch game info for game {game_info['title']}")
            finally:
                self.games_processed += 1
        return games_info

    async def run(self):
        sub_content = await self._fetch_url_content(self.scroll_url.replace("NUM", "0"))
        sub_content = json.loads(sub_content)
        if not sub_content["success"]:
            raise Exception("Failed to start a crawler")
        self.total_games = sub_content["total_count"]
        for i in range(0, self.total_games // 50 + 1):
            games_info = await self.get_games_info(i)
            self.datastream.extend(games_info)
