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
    , game_rating_score NUMERIC(3,1) DEFAULT 0.0
    , home_game BOOLEAN DEFAULT FALSE
    , season VARCHAR(20)
    , PRIMARY KEY (game_date, player)
);
CREATE INDEX IF NOT EXISTS team_index ON NBA.game_stats(team);
CREATE INDEX IF NOT EXISTS opponent_index ON NBA.game_stats(opponent);

CREATE TABLE IF NOT EXISTS NBA.seasons (
    season VARCHAR(20) PRIMARY KEY NOT NULL,
    season_start DATE NOT NULL,
    season_end DATE NOT NULL,
    playoff_start DATE NOT NULL,
    playoff_end DATE NOT NULL
);

INSERT INTO NBA.seasons VALUES
    ('2020-21', '2020-12-22', '2021-05-18', '2021-05-22', '2021-06-20'),
    ('2021-22', '2021-10-19', '2022-04-10', '2022-04-12', '2022-06-16'),
    ('2022-23', '2022-10-18', '2023-04-09', '2023-04-11', '2023-06-12');


DO $$
BEGIN
	CREATE TYPE NBA.average_periods AS ENUM ('one_week', 'three_week', 'nine_week');
EXCEPTION
	WHEN duplicate_object THEN null;
END;
$$;

DO $$
DECLARE
	rec record;
BEGIN
	FOR rec IN
		SELECT t.name AS name FROM UNNEST(enum_range(NULL::NBA.average_periods)) WITH ORDINALITY AS t(name,idx)
	LOOP
		EXECUTE
			'CREATE TABLE IF NOT EXISTS NBA.running_player_averages_'||rec.name||'(
			  game_date DATE NOT NULL
			, season VARCHAR(20) NOT NULL
			, week_id INT NOT NULL
			, player VARCHAR(50) NOT NULL
			, minutes_played NUMERIC(6,4) NOT NULL
			, field_goals NUMERIC(6,4) NOT NULL
			, three_points NUMERIC(6,4) NOT NULL
			, free_throws NUMERIC(6,4) NOT NULL
			, offensive_rebounds NUMERIC(6,4) NOT NULL
			, defensive_rebounds NUMERIC(6,4) NOT NULL
			, total_rebounds NUMERIC(6,4) NOT NULL
			, assists NUMERIC(6,4) NOT NULL
			, steels NUMERIC(6,4) NOT NULL
			, blocks NUMERIC(6,4) NOT NULL
			, turn_overs NUMERIC(6,4) NOT NULL
			, points_scored NUMERIC(6,4) NOT NULL
			, plus_minus NUMERIC(6,4) NOT NULL
			, overall_efficiency NUMERIC(6,4) NOT NULL
			, games_played INT NOT NULL
			, game_rating_score NUMERIC(6,4) NOT NULL
			, minutes_per_game NUMERIC(6,4) NOT NULL
			, center_player_stats NUMERIC(6,4) NOT NULL
			, guard_player_stats NUMERIC(6,4) NOT NULL
			, forward_player_stats NUMERIC(6,4) NOT NULL
			, created_timestamp timestamp default current_timestamp
			, UNIQUE(game_date, player, season)
		);
		CREATE INDEX IF NOT EXISTS averages_player_index_'||rec.name||' on NBA.running_player_averages_'||rec.name||'(player);
		CREATE INDEX IF NOT EXISTS averages_game_date_index_'||rec.name||' on NBA.running_player_averages_'||rec.name||'(game_date);
		CREATE INDEX IF NOT EXISTS averages_season_index_'||rec.name||' on NBA.running_player_averages_'||rec.name||'(season);';
	END LOOP;
END
$$;
