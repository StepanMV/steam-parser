CREATE TABLE publishers (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name VARCHAR(255) UNIQUE
);

CREATE TABLE developers (
    developer_id SERIAL PRIMARY KEY,
    developer_name VARCHAR(255) UNIQUE
);

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(255) UNIQUE
);

CREATE TABLE tags (
    tag_id SERIAL PRIMARY KEY,
    tag_name VARCHAR(255) UNIQUE
);

CREATE TABLE games (
    game_id SERIAL PRIMARY KEY,
    available BOOL,
    title TEXT,
    release_date DATE,
    supports_win BOOL,
    supports_linux BOOL,
    supports_mac BOOL,
    positive_reviews INT,
    total_reviews INT
);

CREATE INDEX idx_title ON games(title);
CREATE INDEX idx_available ON games(available);
CREATE INDEX idx_supports_win ON games(supports_win);
CREATE INDEX idx_supports_linux ON games(supports_linux);
CREATE INDEX idx_supports_mac ON games(supports_mac);

CREATE TABLE game_genres (
    game_id INT,
    genre_id INT,
    PRIMARY KEY (game_id, genre_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE INDEX idx_game_genres ON game_genres(game_id, genre_id);

CREATE TABLE game_tags (
    game_id INT,
    tag_id INT,
    PRIMARY KEY (game_id, tag_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

CREATE INDEX idx_game_tags ON game_tags(game_id, tag_id);

CREATE TABLE game_developers (
    game_id INT,
    developer_id INT,
    PRIMARY KEY (game_id, developer_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (developer_id) REFERENCES developers(developer_id)
);

CREATE INDEX idx_game_developers ON game_developers(game_id, developer_id);

CREATE TABLE game_publishers (
    game_id INT,
    publisher_id INT,
    PRIMARY KEY (game_id, publisher_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id)
);

CREATE INDEX idx_game_publishers ON game_publishers(game_id, publisher_id);

CREATE TABLE price_history (
    price_id SERIAL PRIMARY KEY,
    game_id INT,
    price_wo_discount DECIMAL(10, 2),
    price_w_discount DECIMAL(10, 2),
    date_time TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX idx_game_id ON price_history(game_id);
CREATE INDEX idx_date_time ON price_history(date_time);
