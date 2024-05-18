from flask import Flask, jsonify, request
from db_connection import DBConnection

app = Flask(__name__)
db_connection = DBConnection("localhost", 5432, "steam", "twinkboy42", "twinkboy42")

@app.route('/api/v1/search', methods=['GET'])
def search(self):
    query = request.args.get('query')
    score = request.args.get('score')
    genres = request.args.getlist('genres')
    tags = request.args.getlist('tags')
    developers = request.args.getlist('developers')
    publishers = request.args.getlist('publishers')
    min_price = request.args.get('min_price')
    max_price = request.args.get('max_price')
    min_year = request.args.get('min_year')
    max_year = request.args.get('max_year')
    sort = request.args.get('sort')
    sort_order = request.args.get('sort_order')
    res = db_connection.search_games(query=query, score=score, genres=genres, tags=tags,
                                            developers=developers, publishers=publishers, min_price=min_price,
                                            max_price=max_price, min_year=min_year, max_year=max_year,
                                            sort=sort, sort_order=sort_order)
    return jsonify(res)


@app.route('/api/v1/games/<id>', methods=['GET'])
def get_game(self, id):
    res = db_connection.get_game_info(id)
    return jsonify(res)


@app.route('/api/v1/prices/<id>', methods=['GET'])
def get_prices(self, id):
    res = db_connection.get_game_prices(id)
    return jsonify(res)


if __name__ == '__main__':
    app.run()
