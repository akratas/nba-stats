import math
import sys
from argparse import ArgumentParser, Namespace
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Dict, List, Tuple

from caritas.core.depot.abc.api import SQLBasedExternalDepot

from nba.analyzer.averages import StatsLoader
from nba.collector.depot import init_db

_avg_table_name = 'running_player_averages_'
_insert_query: str = 'INSERT INTO NBA.running_player_averages_{time} AS avgs ({headers}) ' \
                     'VALUES (' \
                     '%(game_date)s' \
                     ', %(season)s' \
                     ', %(week_id)s' \
                     ', %(player)s' \
                     ', %(minutes_played)s' \
                     ', %(field_goals)s' \
                     ', %(three_points)s' \
                     ', %(free_throws)s' \
                     ', %(offensive_rebounds)s' \
                     ', %(defensive_rebounds)s' \
                     ', %(total_rebounds)s' \
                     ', %(assists)s' \
                     ', %(steels)s' \
                     ', %(blocks)s' \
                     ', %(turn_overs)s' \
                     ', %(points_scored)s' \
                     ', %(plus_minus)s' \
                     ', %(overall_efficiency)s' \
                     ', %(season)s' \
                     ', %(games_played)s' \
                     ', %(minutes_per_game)s' \
                     ', %(center_player_stats)s' \
                     ', %(guard_player_stats)s' \
                     ', %(forward_player_stats)s' \
                     ')' \
                     'ON CONFLICT (game_date, player, season)' \
                     'DO UPDATE SET' \
                     '  minutes_played = %(minutes_played)s' \
                     ', field_goals = %(field_goals)s' \
                     ', three_points = %(three_points)s' \
                     ', free_throws = %(free_throws)s' \
                     ', offensive_rebounds = %(offensive_rebounds)s' \
                     ', defensive_rebounds = %(defensive_rebounds)s' \
                     ', total_rebounds = %(total_rebounds)s' \
                     ', assists = %(assists)s' \
                     ', steels = %(steels)s' \
                     ', blocks = %(blocks)s' \
                     ', turn_overs = %(turn_overs)s' \
                     ', points_scored = %(points_scored)s' \
                     ', overall_efficiency = %(overall_efficiency)s' \
                     ', plus_minus = %(plus_minus)s' \
                     ', season = %(season)s' \
                     ', games_played = %(games_played)s' \
                     ', minutes_per_game = %(minutes_per_game)s' \
                     ', center_player_stats = %(center_player_stats)s' \
                     ', guard_player_stats = %(guard_player_stats)s' \
                     ', forward_player_stats = %(forward_player_stats)s' \
                     'WHERE avgs.game_date = %(game_date)s AND avgs.player = %(player)s ' \
                     'AND season = %{season}s'


class AveragePeriods(Enum):
    ONE_WEEK = 1 * 7
    THREE_WEEK = 3 * 7
    NINE_WEEK = 9 * 7


class DynamicAveragesCalculator:
    """
    Will create the dynamic averages entries for the collected game data.
    """
    log10: float = math.log(10)
    # Based on formula from https://www.investopedia.com/articles/forex/09/mcginley-dynamic-indicator.asp
    constant_weight: int = 6

    @staticmethod
    def normalize(value: float) -> float:
        return math.asinh(value / 2) / DynamicAveragesCalculator.log10

    @staticmethod
    def calculate_moving_average(cache: Dict[AveragePeriods, Dict[str, List[Dict[str, any]]]],
                                 rows: List[Dict[str, any]], loader: StatsLoader, period: AveragePeriods,
                                 week: int) -> None:
        player_totals = cache.get(period)
        for row in rows:
            player: str = row.get('player')
            stat: Dict[str, any] = {'player': player, 'game_date': row.get('game_date'), 'season': row.get('season'),
                                    'week_id': week}

            t: time = row.get('minutes_played')
            stat['minutes_played'] = DynamicAveragesCalculator.normalize(
                (t.hour * 3600 + t.minute * 60 + t.second) * 1.0)
            stat['field_goals'] = DynamicAveragesCalculator.normalize(
                row.get('field_goals') / row.get('field_goal_attempts'))
            stat['three_points'] = DynamicAveragesCalculator.normalize(
                row.get('three_points') / row.get('three_point_attempts'))
            stat['free_throws'] = DynamicAveragesCalculator.normalize(
                row.get('free_throws') / row.get('free_throw_attempts'))
            stat['total_rebounds'] = DynamicAveragesCalculator.normalize(
                row.get('offensive_rebounds') + row.get('defensive_rebounds'))

            stat['field_goal_attempts'] = DynamicAveragesCalculator.normalize(row.get('field_goal_attempts'))
            stat['free_throw_attempts'] = DynamicAveragesCalculator.normalize(row.get('free_throw_attempts'))
            stat['free_throws_made'] = DynamicAveragesCalculator.normalize(row.get('free_throws'))
            stat['three_points_made'] = DynamicAveragesCalculator.normalize(row.get('three_points'))
            stat['three_point_attempts'] = DynamicAveragesCalculator.normalize(row.get('three_point_attempts'))
            stat['offensive_rebounds'] = DynamicAveragesCalculator.normalize(row.get('offensive_rebounds'))
            stat['defensive_rebounds'] = DynamicAveragesCalculator.normalize(row.get('defensive_rebounds'))

            stat['assists'] = DynamicAveragesCalculator.normalize(row.get('assists'))
            stat['steels'] = DynamicAveragesCalculator.normalize(row.get('steels'))
            stat['blocks'] = DynamicAveragesCalculator.normalize(row.get('blocks'))
            stat['turn_overs'] = DynamicAveragesCalculator.normalize(row.get('turn_overs'))
            stat['personal_fouls'] = DynamicAveragesCalculator.normalize(row.get('personal_fouls'))
            stat['points_scored'] = DynamicAveragesCalculator.normalize(row.get('points_scored'))
            stat['plus_minus'] = DynamicAveragesCalculator.normalize(row.get('plus_minus'))
            stat['overall_efficiency'] = DynamicAveragesCalculator.normalize(
                DynamicAveragesCalculator.calc_overall(stat))
            running_totals: List[Dict[str, any]] = player_totals.get(player)

    @staticmethod
    def calculate_averages(previous_stats: Dict[str, Dict[str, any]], rows: List[Dict[str, any]],
                           columns: List[str], period: AveragePeriods) -> List[Dict[str, any]]:
        multiplier: int = period.value
        averages: List[Dict[str, any]] = []
        for row in rows:
            player: str = row.get('player')

            stat: Dict[str, any] = previous_stats.get(player)
            if not stat:
                stat = {'player': player}
                previous_stats[player] = stat

            stat['game_date'] = row.get('game_date')

            for column in columns:
                if 'overall_efficiency' == column:
                    current = DynamicAveragesCalculator.calc_overall(row) * 1.0
                elif 'minutes_played' == column:
                    t: time = row.get(column)
                    current: float = (t.hour * 3600 + t.minute * 60 + t.second) * 1.0
                else:
                    current: float = row.get(column) * 1.0
                if column not in stat:
                    stat[column] = current
                else:
                    stat[column] = DynamicAveragesCalculator.calc_mcginley_avg(stat[column], current, multiplier)

            snap: Dict[str, any] = {}
            snap.update(stat)
            averages.append(snap)
        return averages

    @staticmethod
    def calc_mcginley_avg(prev_average: float, current_value: float, multiplier: int) -> float:
        denom: float = prev_average if prev_average != 0.0 else 1
        num: float = current_value if current_value != 0.0 else 0
        denom = num / denom if num != 0.0 and denom != 0.0 else 1
        return prev_average + (current_value - prev_average) / \
            (DynamicAveragesCalculator.constant_weight * multiplier / denom)

    @staticmethod
    def calc_overall(row: Dict[str, any]) -> float:
        val: float = row.get('points_scored')
        val = val + 0.4 * row.get('field_goals')
        val = val + 0.4 * row.get('three_points')
        val = val + 0.7 * row.get('offensive_rebounds')
        val = val + 0.3 * row.get('defensive_rebounds') + row.get('steels')
        val = val + 0.7 * row.get('assists')
        val = val + 0.7 * row.get('blocks')
        val = val - 0.7 * row.get('field_goal_attempts')
        val = val - 0.4 * (row.get('free_throw_attempts') - row.get('free_throws_made'))
        val = val - 0.4 * (row.get('three_point_attempts') - row.get('three_points_made'))
        val = val - 0.4 * row.get('personal_fouls')
        val = val + .7 * row.get('minutes_played')
        return val - row.get('turn_overs')

    @staticmethod
    def calc_center_stats(row: Dict[str, any]) -> float:
        return .8 * (row.get('defensive_rebounds') + row.get('offensive_rebounds')) + row.get('blocks') + \
            .6 * row.get('field_goals') + row.get('minutes_played')

    @staticmethod
    def calc_guard_stats(row: Dict[str, any]) -> float:
        return .8 * (row.get('assists') + row.get('steels')) + row.get('three_points') * row.get('points_scored') + \
            row.get('minutes_played') - row.get('turn_overs')

    @staticmethod
    def calc_forward_stats(row: Dict[str, any]) -> float:
        pass


class StatsLoader:
    """
    Takes in parameters and loads stats for calculation purposes.
    """

    _game_date_query: str = 'SELECT * FROM NBA.game_stats g WHERE g.game_date BETWEEN \'{from_date}\' AND \'{to_date}\''
    _season_query: str = 'SELECT s.season_start, s.season_end FROM NBA.seasons s WHERE s.season = \'{season}\''
    _headers: str = 'SELECT column_name FROM information_schema.columns WHERE table_schema = \'nba\' ' \
                    'AND table_name = \'{avg_tbl_name}\''
    _numeric_cols_names: str = 'SELECT column_name FROM information_schema.columns WHERE table_schema = \'nba\' ' \
                               'AND table_name = \'{avg_tbl_name}\' AND data_type = \'double precision\';'
    _averages_player: str = 'SELECT player, minutes_played, field_goals, three_points, free_throws, offensive_rebounds' \
                            ', defensive_rebounds, total_rebounds, assists, steels, blocks, turn_overs, points_scored' \
                            ', overall_efficiency FROM NBA.{avg_tbl_name} a ' \
                            'WHERE a.season={season} AND a.week_id={week_id}'
    _games_played: str = 'SELECT player, season, COUNT(season) AS games_played' \
                         'FROM nba.game_stats ' \
                         'WHERE game_date BETWEEN \'{from_date}\' AND \'{to_date}\' AND season=\'{season}\' ' \
                         'AND player=\'{player}\' GROUP BY player, season ORDER BY season DESC, player;'

    def __init__(self):
        self.depot: SQLBasedExternalDepot = init_db()

    def load_by_season(self, season_dates: Tuple[date, date]) -> List[Dict[str, any]]:
        return self.load_by_dates(season_dates[0], season_dates[1])

    def load_by_dates(self, from_date: date, to_date: date) -> List[Dict[str, any]]:
        print(f'Loading data by dates from {from_date} to {to_date}.')
        return self.depot.do_query_many_dict(StatsLoader._game_date_query.format(from_date=from_date, to_date=to_date))

    def load_games_played(self, player: str, season: str, from_date: date, to_date: date) -> Dict[str, any]:
        return self.depot.do_batch_query_single_dict(
            StatsLoader._games_played.format(from_date=from_date, to_date=to_date, season=season, player=player), False)

    def get_averages_headers(self) -> str:
        rows: List[Dict[str, any]] = self.depot.do_query_many_dict(StatsLoader._headers)
        size: int = len(rows) - 1
        pointer: int = 0
        headers: str = ''
        for row in rows:
            headers = headers + (row.get('column_name') + (',' if pointer < size else ''))
            pointer += 1
        return headers

    def get_averages_column_names(self) -> List[str]:
        rows: List[Dict[str, any]] = self.depot.do_query_many_dict(StatsLoader._numeric_cols_names)
        columns: List[str] = []
        for row in rows:
            columns.append(row.get('column_name'))
        return columns

    def load_previous_player_stats(self, period: AveragePeriods) -> Dict[str, Dict[str, any]]:
        rows: List[Dict[str, any]] = \
            self.depot.do_query_many_dict(
                StatsLoader._averages_player.format(avg_tbl_name=f'{_avg_table_name}{period.name}',
                                                    period=period.value,
                                                    second_period=period.value))
        stats: Dict[str, Dict[str, any]] = {}
        for row in rows:
            player: str = row.get('player')
            stat: Dict[str, any] = {}
            stats[player] = stat
            stat.update(row)

        return stats


class AnalyticsController:
    """
    Guides the steps to load and calculate all analytics.
    """
    _seasons_query: str = 'SELECT * FROM NBA.seasons;'

    def __init__(self):
        self.depot: SQLBasedExternalDepot = init_db()
        self.loader: StatsLoader = StatsLoader()
        self.season_dates: Dict[str, Tuple[date, date]] = self.load_seasons()

    def calc_for_season(self, season: str) -> None:
        if season not in self.season_dates:
            raise ValueError(f'season {season} not found')
        # self.analyze_and_store(self.loader.load_by_season(self.season_dates.get(season)))
        dates: Tuple[date, date] = self.season_dates.get(season)
        current_date: date = dates[0]
        next_date: date = current_date + timedelta(days=1)
        week_date: date = current_date + timedelta(days=AveragePeriods.ONE_WEEK.value)
        three_date: date = current_date + timedelta(days=AveragePeriods.THREE_WEEK.value)
        nine_date: date = current_date + timedelta(days=AveragePeriods.NINE_WEEK.value)
        cache: Dict[AveragePeriods, Dict[str, List[Dict[str, any]]]] = {}
        while current_date <= dates[1]:
            self.calc_for_dates()
            current_date = next_date

    def calc_for_dates(self, from_date: date, to_date: date) -> None:
        self.analyze_and_store(self.loader.load_by_dates(from_date, to_date))

    def analyze_and_store(self, rows: List[Dict[str, any]], start_week: int, number_of_weeks: int) -> None:
        columns: List[str] = self.loader.get_averages_column_names()
        headers: str = self.loader.get_averages_headers()
        for period in AveragePeriods:
            stats: Dict[str, Dict[str, any]] = self.loader.load_previous_player_stats(period)
            result: List[Dict[str, any]] = DynamicAveragesCalculator.calculate_averages(stats, rows, columns, period)
            self.depot.do_batch_query_single_dict(_insert_query.format(time=period.name, headers=headers), False,
                                                  result)

    def load_seasons(self):
        seasons: Dict[str, Tuple[date, date]] = {}
        result: List[Dict[str, any]] = self.depot.do_query_many_dict(self._seasons_query)
        for item in result:
            seasons[item['season']] = (item['season_start'], item['season_end'])
        return seasons


def main(args):
    parser = ArgumentParser(prog="NBA Loader")
    parser.add_argument('--season', nargs='?', help='Season (YYYY-YY) to calculate averages for.')
    parser.add_argument('--from_date', nargs='?', help='Date (YYYY-MM-DD) to being calculating averages.')
    parser.add_argument('--to_date', nargs='?', help='Date (YYYY-MM-DD) to stop calculating averages.')
    # args as object
    args: Namespace = parser.parse_args()

    guide: AnalyticsController = AnalyticsController()

    if args is None:
        raise ValueError('Args was not parsed correctly')

    if args.season is not None:
        # Calculate for season
        guide.calc_for_season(args.season)
        return

    if args.from_date is None:
        raise ValueError('Must provide start date to being calculating.')

    from_date: date = datetime.fromisoformat(args.from_date).date()
    if args.to_date is None:
        to_date: date = from_date
    else:
        to_date: date = datetime.fromisoformat(args.to_date).date()

    print(f'Date calc from {from_date} to {to_date}')

    guide.calc_for_dates(from_date, to_date)


if __name__ == "__main__":
    main(sys.argv[1:])
