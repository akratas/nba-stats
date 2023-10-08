from datetime import date, datetime
from enum import Enum
from typing import List, Dict, Tuple, Set

from caritas.core.depot.abc.api import SQLBasedExternalDepot
from caritas.core.depot.data_stores import PostgresDepotSQLBased


class NBATypes(Enum):
    STRING = 1
    INTEGER = 2
    DECIMAL = 3
    TIME = 4
    BOOLEAN = 5


def init_db() -> SQLBasedExternalDepot:
    db_configs: Dict[str, any] = {'caritas.db.host': 'localhost', 'caritas.db.port': 5432, 'caritas.db.name': 'nba',
                                  'caritas.db.user': 'stats', 'caritas.db.password': 'nba_stats',
                                  'caritas.db.pool.min': 2, 'caritas.db.pool.max': 5}
    return PostgresDepotSQLBased(db_configs)


class NBADataSink:
    """Will store to the DB the NBA data"""

    _GAME_DATE_FIELD_NAME: str = 'game_date'

    _game_dates_query: str = 'SELECT DISTINCT game_date FROM NBA.game_stats ORDER BY game_date ASC;'

    _seasons_query: str = 'SELECT * FROM NBA.seasons;'

    def __init__(self):
        self.depot: SQLBasedExternalDepot = init_db()
        self.header_mappings: Dict[str, Tuple[str, NBATypes]] = NBADataSink._init_header_mappings()
        self.element_attribute_mappings: Dict[
            str, Tuple[str, NBATypes]] = NBADataSink._init_headers_by_attribute_mappings()
        self.saved_dates: Set[date] = self.load_dates()
        self.season_dates: Dict[str, Tuple[date, date]] = self.load_seasons()
        self.season_loading: str = ''

    def has_date_been_loaded(self, game_date: date) -> bool:
        return game_date in self.saved_dates

    @staticmethod
    def _init_header_mappings() -> Dict[str, Tuple[str, NBATypes]]:
        """
        Maps the incoming header value to a column value in the DB
        :return:
        """
        return {
            'Player': ('player', NBATypes.STRING),
            'Tm': ('team', NBATypes.STRING),
            'game_location': ('home_game', NBATypes.BOOLEAN),
            'Opp': ('opponent', NBATypes.STRING),
            'MP': ('minutes_played', NBATypes.TIME),
            'FG': ('field_goals', NBATypes.INTEGER),
            'FGA': ('field_goal_attempts', NBATypes.INTEGER),
            '3P': ('three_points', NBATypes.INTEGER),
            '3PA': ('three_point_attempts', NBATypes.INTEGER),
            'FT': ('free_throws', NBATypes.INTEGER),
            'FTA': ('free_throw_attempts', NBATypes.INTEGER),
            'ORB': ('offensive_rebounds', NBATypes.INTEGER),
            'DRB': ('defensive_rebounds', NBATypes.INTEGER),
            'AST': ('assists', NBATypes.INTEGER),
            'STL': ('steels', NBATypes.INTEGER),
            'BLK': ('blocks', NBATypes.INTEGER),
            'TOV': ('turn_overs', NBATypes.INTEGER),
            'PF': ('personal_fouls', NBATypes.INTEGER),
            'PTS': ('points_scored', NBATypes.INTEGER),
            '+/-': ('plus_minus', NBATypes.INTEGER),
            'GmSc': ('game_rating_score', NBATypes.DECIMAL),
        }

    @staticmethod
    def _init_headers_by_attribute_mappings() -> Dict[str, Tuple[str, NBATypes]]:
        """
        Maps the data row's element id to the db's column name and type.  These values are tied to the attribute
        *data-stat* found in every row of interest
        :return: mapping
        """
        return {
            'player': ('player', NBATypes.STRING),
            'team_id': ('team', NBATypes.STRING),
            'game_location': ('home_game', NBATypes.BOOLEAN),
            'opp_id': ('opponent', NBATypes.STRING),
            'mp': ('minutes_played', NBATypes.TIME),
            'fg': ('field_goals', NBATypes.INTEGER),
            'fga': ('field_goal_attempts', NBATypes.INTEGER),
            'fg3': ('three_points', NBATypes.INTEGER),
            'fg3a': ('three_point_attempts', NBATypes.INTEGER),
            'ft': ('free_throws', NBATypes.INTEGER),
            'fta': ('free_throw_attempts', NBATypes.INTEGER),
            'orb': ('offensive_rebounds', NBATypes.INTEGER),
            'drb': ('defensive_rebounds', NBATypes.INTEGER),
            'ast': ('assists', NBATypes.INTEGER),
            'stl': ('steels', NBATypes.INTEGER),
            'blk': ('blocks', NBATypes.INTEGER),
            'tov': ('turn_overs', NBATypes.INTEGER),
            'pf': ('personal_fouls', NBATypes.INTEGER),
            'pts': ('points_scored', NBATypes.INTEGER),
            'plus_minus': ('plus_minus', NBATypes.INTEGER),
            'game_score': ('game_rating_score', NBATypes.DECIMAL),
        }

    def store_records(self, game_date: date, rows: List[Dict[str, str]], uses_attributes_mapping: bool = False) -> None:
        """
        Stores the incoming records into the Database.
        :param game_date: The game date being loaded.
        :param rows: The rows scrapped form the site.
        :param uses_attributes_mapping: True if you loaded the data using the data-stat attribute to determine col name
        :return: None
        """
        if game_date in self.saved_dates:
            print(f'Date {game_date} already loaded, ignoring.')
            return
        try:
            params: List[Dict[str, any]] = []
            mappings: Dict[str, Tuple[
                str, NBATypes]] = self.element_attribute_mappings if uses_attributes_mapping else self.header_mappings
            for row in rows:
                inbound: Dict[str, any] = {NBADataSink._GAME_DATE_FIELD_NAME: game_date}
                try:
                    for key in mappings.keys():
                        value: any = row[key]
                        config: Tuple[str, NBATypes] = mappings[key]
                        inbound[config[0]] = NBADataSink.convert(value, config[1])
                    inbound['season'] = self.season_loading
                    params.append(inbound)
                except Exception as e:
                    print(f'ERROR: {e} ROW: {row}')
            if len(params) > 0:
                self.depot.do_batch_query_single_dict('NBA.game_stats', True, params)
        except Exception as e:
            print(f'ERROR: {e}')

    @staticmethod
    def convert(value: str, data_type: NBATypes) -> any:
        if value is None:
            return None

        is_blank: bool = len(value.strip()) == 0

        if NBATypes.STRING is data_type:
            return str(value) if not is_blank else ''

        if NBATypes.INTEGER is data_type:
            return int(value) if not is_blank else 0

        if NBATypes.DECIMAL is data_type:
            return float(value) if not is_blank else 0.0

        if NBATypes.TIME is data_type:
            time_split: List[str] = value.split(':') if not is_blank else ['0', '0']
            minute: int = int(time_split[0])
            if minute < 60:
                return datetime.now().time().replace(hour=0, minute=minute, second=int(time_split[1]), microsecond=0)
            return datetime.now().time().replace(hour=int(minute / 60), minute=0, second=int(time_split[1]),
                                                 microsecond=0)
        if NBATypes.BOOLEAN is data_type:
            return bool(value) if not is_blank else False

    def load_dates(self) -> Set[date]:
        dates: Set[date] = set()
        result: List[Dict[str, any]] = self.depot.do_query_many_dict(NBADataSink._game_dates_query)
        for item in result:
            dates.add(item[NBADataSink._GAME_DATE_FIELD_NAME])
        return dates

    def load_seasons(self):
        seasons: Dict[str, Tuple[date, date]] = {}
        result: List[Dict[str, any]] = self.depot.do_query_many_dict(NBADataSink._seasons_query)
        for item in result:
            seasons[item['season']] = (item['season_start'], item['season_end'])
        return seasons

    def season(self, season):
        if season not in self.season_dates:
            raise Exception(f'incorrect season key {season}, not in db: {self.season_dates}')
        self.season_loading = season
