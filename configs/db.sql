CREATE SCHEMA IF NOT EXISTS NBA AUTHORIZATION stats;

CREATE TABLE IF NOT EXISTS NBA.game_stats(
      game_date DATE NULL
    , player VARCHAR(50) NULL
    , team VARCHAR(5) NULL
    , opponent VARCHAR(5) NULL
    , minutes_played TIME NULL
    , field_goals INTEGER NULL
    , field_goal_attempts INTEGER NULL
    , three_points INTEGER NULL
    , three_point_attempts INTEGER NULL
    , free_throws INTEGER NULL
    , free_throw_attempts INTEGER NULL
    , offensive_rebounds INTEGER NOT NULL
    , defensive_rebounds INTEGER NOT NULL
    , assists INTEGER NULL
    , steels INTEGER NULL
    , blocks INTEGER NULL
    , turn_overs INTEGER NULL
    , personal_fouls INTEGER NULL
    , points_scored INTEGER NULL
    , plus_minus INTEGER NULL
    , game_rating_score NUMERIC(4,4) DEFAULT 0.0
    , home_game BOOLEAN DEFAULT FALSE
    , games_played INTEGER DEFAULT 0
    , PRIMARY KEY (game_date, player)
);
CREATE INDEX IF NOT EXISTS team_index ON NBA.game_stats(team);
CREATE INDEX IF NOT EXISTS opponent_index ON NBA.game_stats(opponent);

CREATE TABLE IF NOT EXISTS NBA.team(
      player VARCHAR(50) NOT NULL
    , playable BOOLEAN DEFAULT TRUE
    , enroll_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    , release_date TIMESTAMP NULL
);
CREATE INDEX IF NOT EXISTS team_player_index on NBA.team(player);

CREATE TABLE IF NOT EXISTS NBA.running_player_averages(
      game_date DATE NOT NULL
    , player VARCHAR(50) NOT NULL
    , minutes_played FLOAT NOT NULL
    , field_goals FLOAT NOT NULL
    , three_points FLOAT NOT NULL
    , free_throws FLOAT NOT NULL
    , offensive_rebounds FLOAT NOT NULL
    , defensive_rebounds FLOAT NOT NULL
    , total_rebounds FLOAT NOT NULL
    , assists FLOAT NOT NULL
    , steels FLOAT NOT NULL
    , blocks FLOAT NOT NULL
    , turn_overs FLOAT NOT NULL
    , points_scored FLOAT NOT NULL
    , plus_minus FLOAT NOT NULL
    , overall_efficiency FLOAT NOT NULL
    , period_length INT NOT NULL
    , UNIQUE(game_date, player, period_length)
);
CREATE INDEX IF NOT EXISTS averages_player_index on NBA.running_player_averages(player);
CREATE INDEX IF NOT EXISTS averages_game_date_index on NBA.running_player_averages(game_date);

CREATE TABLE IF NOT EXISTS NBA.seasons (
    season VARCHAR(20) PRIMARY KEY NOT NULL,
    season_start DATE NOT NULL,
    season_end DATE NOT NULL,
    playoff_start DATE NOT NULL,
    playoff_end DATE NOT NULL
);