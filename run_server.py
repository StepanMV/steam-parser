from flask import Flask, jsonify, request
from db_connection import DBConnection

app = Flask(__name__)
db_connection = DBConnection("localhost", 5432, "steam", "twinkboy42", "twinkboy42")

@app.route('/api/v1/search', methods=['GET'])
def search():
    query = request.args.get('query')
    min_price = request.args.get('min_price', type=float)
    max_price = request.args.get('max_price', type=float)
    min_year = request.args.get('min_year', type=int)
    max_year = request.args.get('max_year', type=int)
    genres = request.args.getlist('genres[]')
    tags = request.args.getlist('tags[]')
    print(tags)
    publishers = request.args.getlist('publishers[]')
    developers = request.args.getlist('developers[]')
    score = request.args.get('score', type=int)
    sort = request.args.get('sort') if request.args.get('sort') else 'score'
    sort_direction = request.args.get('sort_direction') if request.args.get('sort_direction') else 'DESC'

    res = db_connection.search_games(query=query, score=score, genres=genres, tags=tags,
                                     developers=developers, publishers=publishers, min_price=min_price,
                                     max_price=max_price, min_year=min_year, max_year=max_year,
                                     sort=sort, sort_direction=sort_direction)
    return jsonify(res)


@app.route('/api/v1/games/<id>', methods=['GET'])
def get_game(id):
    res = db_connection.get_game_info(id)
    return jsonify(res)


@app.route('/api/v1/prices/<id>', methods=['GET'])
def get_prices(id):
    res = db_connection.get_game_prices(id)
    return jsonify(res)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
