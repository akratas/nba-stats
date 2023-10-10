import math
import sys
from argparse import ArgumentParser, Namespace
from builtins import isinstance
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Dict, List, Tuple

from caritas.core.depot.abc.api import SQLBasedExternalDepot
from soupsieve.util import lower

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
                     ', %(games_played)s' \
                     ', %(game_rating_score)s' \
                     ', %(minutes_per_game)s' \
                     ', %(center_player_stats)s' \
                     ', %(guard_player_stats)s' \
                     ', %(forward_player_stats)s' \
                     ', %(created_timestamp)s' \
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
                     ', games_played = %(games_played)s' \
                     ', game_rating_score = %(game_rating_score)s' \
                     ', minutes_per_game = %(minutes_per_game)s' \
                     ', center_player_stats = %(center_player_stats)s' \
                     ', guard_player_stats = %(guard_player_stats)s' \
                     ', forward_player_stats = %(forward_player_stats)s' \
                     ', created_timestamp = %(created_timestamp)s ' \
                     'WHERE avgs.game_date = %(game_date)s AND avgs.player = %(player)s ' \
                     'AND avgs.season = %(season)s'


class AveragePeriods(Enum):
    ONE_WEEK = 1
    THREE_WEEK = 3
    NINE_WEEK = 9


class LastSavedDatesTrackerByPeriod:

    def __init__(self, current_date: date):
        self.one_week: bool = False
        self.three_week: bool = False
        self.nine_week: bool = False
        self.dates: Dict[AveragePeriods, date] = {AveragePeriods.ONE_WEEK: current_date,
                                                  AveragePeriods.THREE_WEEK: current_date,
                                                  AveragePeriods.NINE_WEEK: current_date}

    def update_one_week(self, next_date: date) -> None:
        self.one_week = False
        self.dates[AveragePeriods.ONE_WEEK] = next_date

    def update_three_week(self, next_date: date) -> None:
        self.three_week = False
        self.dates[AveragePeriods.THREE_WEEK] = next_date

    def update_nine_week(self, next_date: date) -> None:
        self.nine_week = False
        self.dates[AveragePeriods.NINE_WEEK] = next_date

    def get(self, period: AveragePeriods) -> date:
        return self.dates[period]


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
                                 rows: List[Dict[str, any]], depot: SQLBasedExternalDepot,
                                 games_played: Dict[AveragePeriods, Dict[str, int]],
                                 saved_dates_tracker: LastSavedDatesTrackerByPeriod, headers: str, week: int) -> None:
        for row in rows:
            player: str = str(row.get('player')).strip()
            stat: Dict[str, any] = {'player': player, 'game_date': row.get('game_date'), 'season': row.get('season'),
                                    'week_id': week}

            t: time = row.get('minutes_played')
            stat['minutes_played'] = (t.hour * 60 + t.minute + t.second / 60)
            stat['total_rebounds'] = DynamicAveragesCalculator.normalize(
                row.get('offensive_rebounds') + row.get('defensive_rebounds'))
            stat['field_goals'] = DynamicAveragesCalculator.normalize(row.get('field_goals'))
            stat['field_goal_attempts'] = DynamicAveragesCalculator.normalize(row.get('field_goal_attempts'))
            stat['free_throw_attempts'] = DynamicAveragesCalculator.normalize(row.get('free_throw_attempts'))
            stat['free_throws'] = DynamicAveragesCalculator.normalize(row.get('free_throws'))
            stat['three_points'] = DynamicAveragesCalculator.normalize(row.get('three_points'))
            stat['three_point_attempts'] = DynamicAveragesCalculator.normalize(row.get('three_point_attempts'))
            stat['offensive_rebounds'] = DynamicAveragesCalculator.normalize(row.get('offensive_rebounds'))
            stat['defensive_rebounds'] = DynamicAveragesCalculator.normalize(row.get('defensive_rebounds'))

            stat['assists'] = DynamicAveragesCalculator.normalize(row.get('assists'))
            stat['steels'] = DynamicAveragesCalculator.normalize(row.get('steels'))
            stat['blocks'] = DynamicAveragesCalculator.normalize(row.get('blocks'))
            stat['turn_overs'] = DynamicAveragesCalculator.normalize(row.get('turn_overs'))
            stat['personal_fouls'] = DynamicAveragesCalculator.normalize(row.get('personal_fouls'))
            stat['points_scored'] = DynamicAveragesCalculator.normalize(row.get('points_scored'))
            stat['plus_minus'] = DynamicAveragesCalculator.normalize( row.get('plus_minus'))
            stat['game_rating_score'] = DynamicAveragesCalculator.normalize(row.get('game_rating_score'))

            stat['games_played'] = DynamicAveragesCalculator.check_games_played(games_played, AveragePeriods.ONE_WEEK,
                                                                                player)
            if DynamicAveragesCalculator.check_player_totals(cache, stat, depot, saved_dates_tracker, headers,
                                                             AveragePeriods.ONE_WEEK):
                saved_dates_tracker.one_week = True

            stat['games_played'] = DynamicAveragesCalculator.check_games_played(games_played, AveragePeriods.THREE_WEEK,
                                                                                player)
            if DynamicAveragesCalculator.check_player_totals(cache, stat, depot, saved_dates_tracker, headers,
                                                             AveragePeriods.THREE_WEEK):
                saved_dates_tracker.three_week = True

            stat['games_played'] = DynamicAveragesCalculator.check_games_played(games_played, AveragePeriods.NINE_WEEK,
                                                                                player)
            if DynamicAveragesCalculator.check_player_totals(cache, stat, depot, saved_dates_tracker, headers,
                                                             AveragePeriods.NINE_WEEK):
                saved_dates_tracker.nine_week = True

        if saved_dates_tracker.one_week:
            DynamicAveragesCalculator.reset_games_played(games_played, AveragePeriods.ONE_WEEK)
        if saved_dates_tracker.three_week:
            DynamicAveragesCalculator.reset_games_played(games_played, AveragePeriods.THREE_WEEK)
        if saved_dates_tracker.nine_week:
            DynamicAveragesCalculator.reset_games_played(games_played, AveragePeriods.NINE_WEEK)

    @staticmethod
    def reset_games_played(games_played: Dict[AveragePeriods, Dict[str, int]], period: AveragePeriods):
        gp: Dict[str, int] = games_played.get(period)
        gp.clear()

    @staticmethod
    def check_games_played(games_played: Dict[AveragePeriods, Dict[str, int]], period: AveragePeriods,
                           player: str) -> int:
        gp: Dict[str, int] = games_played.get(period)
        if player not in gp:
            gp[player] = 0
        gp[player] = gp[player] + 1
        return gp[player]

    @staticmethod
    def check_player_totals(cache: Dict[AveragePeriods, Dict[str, List[Dict[str, any]]]], stat: Dict[str, any],
                            depot: SQLBasedExternalDepot, saved_dates_tracker: LastSavedDatesTrackerByPeriod,
                            headers: str, period: AveragePeriods) -> bool:
        all_player_totals: Dict[str, List[Dict[str, any]]] = cache.get(period)
        player: str = stat.get('player')
        player_totals: List[Dict[str, any]] = all_player_totals.get(player)
        if player_totals is None:
            player_totals = []
            all_player_totals[player] = player_totals
        period_date: date = saved_dates_tracker.get(period)
        game_date: date = stat.get('game_date')
        week_id: int = int((game_date - period_date).days / 7)
        saved: bool = False
        if week_id > 0 and int(week_id % period.value) is 0:
            # time to calculate average and store in table
            saved = DynamicAveragesCalculator.write_averages(depot, game_date, headers, period,
                                                             player_totals)
        player_totals.append(stat)
        all_player_totals[player] = player_totals
        return saved

    @staticmethod
    def write_averages(depot: SQLBasedExternalDepot, game_date: date, headers: str, period: AveragePeriods,
                       player_totals: List[Dict[str, any]]) -> bool:
        params: List[Dict[str, any]] = []
        avg: Dict[str, any] = {}
        max_games: int = 0
        saved: bool = False
        for total in player_totals:
            for key in total.keys():
                value = total[key]
                if 'games_played' is key:
                    max_games = max(total.get(key), max_games)
                    continue
                elif 'week_id' is key:
                    avg[key] = value
                elif 'game_date' is key:
                    game_date = max(game_date, value)
                elif isinstance(value, float) or isinstance(value, int):
                    avg[key] = avg[key] + value if key in avg else value
                else:
                    avg[key] = value
            for key in avg.keys():
                value = avg[key]
                if 'week_id' is key:
                    continue
                if isinstance(value, float) or isinstance(value, int):
                    avg[key] = value / max_games
        if len(avg) > 0:
            avg['games_played'] = max_games
            avg['overall_efficiency'] = DynamicAveragesCalculator.normalize(DynamicAveragesCalculator.calc_overall(avg))
            avg['center_player_stats'] = DynamicAveragesCalculator.normalize(
                DynamicAveragesCalculator.calc_center_stats(avg))
            avg['guard_player_stats'] = DynamicAveragesCalculator.normalize(
                DynamicAveragesCalculator.calc_guard_stats(avg))
            avg['forward_player_stats'] = DynamicAveragesCalculator.normalize(
                DynamicAveragesCalculator.calc_forward_stats(avg))
            avg['minutes_per_game'] = avg['minutes_played'] / max_games if 'minutes_played' in avg else 0
            avg['created_timestamp'] = datetime.now()
            avg['game_date'] = game_date
            params.append(avg)
            depot.do_batch_query_single_dict(_insert_query.format(time=lower(period.name), headers=headers), False,
                                             params)
            saved = True
            if len(player_totals) > 0:
                player_totals.pop(0)
        return saved

    @staticmethod
    def calculate_averages(previous_stats: Dict[str, Dict[str, any]], rows: List[Dict[str, any]],
                           columns: List[str], period: AveragePeriods) -> List[Dict[str, any]]:
        """
        This is deprecated
        :param previous_stats:
        :param rows:
        :param columns:
        :param period:
        :return:
        """
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
        minutes_per_game: int = row.get('minutes_per_game') if row.get('minutes_per_game') is not None else 1
        val: float = row.get('points_scored') if row.get('points_scored') is not None else 0 \
            + 85 * row.get('field_goal') if row.get('field_goal') is not None else 0 \
            + 53 * row.get('steels') if row.get('steels') is not None else 0 \
            + 51 * row.get('three_points') if row.get('three_points') is not None else 0 \
            + 46 * row.get('free_throws') if row.get('free_throws') is not None else 0 \
            + 39 * row.get('blocks') if row.get('blocks') is not None else 0 \
            + 39 * row.get('offensive_rebounds') if row.get('offensive_rebounds') is not None else 0 \
            + 34 * row.get('assists') if row.get('assists') is not None else 0 \
            + 14 * row.get('defensive_rebounds') if row.get('defensive_rebounds') is not None else 0 \
            + -17 * row.get('personal_fouls') if row.get('personal_fouls') is not None else 0 \
            + -20 * (row.get('free_throw_attempts') - row.get('free_throws')) if row.get('free_throw_attempts') else 0 \
            + -53 * row.get('turn_overs') if row.get('turn_overs') is not None else 0 \
            + row.get('plus_minus') if row.get('plus_minus') is not None else 0 \
            + row.get('game_rating_score') if row.get('game_rating_score') is not None else 0
        return val * (1 / minutes_per_game)


    @staticmethod
    def calc_center_stats(row: Dict[str, any]) -> float:
        return 80 * (row.get('defensive_rebounds') + row.get('offensive_rebounds')) + 45 * row.get('blocks') + \
            65 * row.get('field_goals') + 45 * row.get('games_played')

    @staticmethod
    def calc_guard_stats(row: Dict[str, any]) -> float:
        return 85 * (row.get('assists') + row.get('steels')) + row.get('three_points') * row.get('points_scored') - \
            row.get('turn_overs') + 25 * row.get('games_played')

    @staticmethod
    def calc_forward_stats(row: Dict[str, any]) -> float:
        return row.get('field_goals') * row.get('points_scored') + 40 * row.get('three_point_attempts') - \
            row.get('three_points') + 60 * row.get('games_played')


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
        rows: List[Dict[str, any]] = self.depot.do_query_many_dict(StatsLoader._headers.format(
            avg_tbl_name=f'{_avg_table_name}{lower(AveragePeriods.ONE_WEEK.name)}'))
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
        cache: Dict[AveragePeriods, Dict[str, List[Dict[str, any]]]] = {AveragePeriods.ONE_WEEK: {},
                                                                        AveragePeriods.THREE_WEEK: {},
                                                                        AveragePeriods.NINE_WEEK: {}}
        headers: str = self.loader.get_averages_headers()
        games_played: Dict[AveragePeriods, Dict[str, int]] = {AveragePeriods.ONE_WEEK: {},
                                                              AveragePeriods.THREE_WEEK: {},
                                                              AveragePeriods.NINE_WEEK: {}}
        saved_dates_tracker: LastSavedDatesTrackerByPeriod = LastSavedDatesTrackerByPeriod(current_date)
        while current_date <= dates[1]:
            game_data: List[Dict[str, any]] = self.loader.load_by_dates(current_date, current_date)
            week: int = int((current_date - dates[0]).days / 7) + 1
            DynamicAveragesCalculator.calculate_moving_average(cache, game_data, self.depot, games_played,
                                                               saved_dates_tracker,
                                                               headers, week)
            current_date: date = current_date + timedelta(days=1)
            if saved_dates_tracker.one_week:
                saved_dates_tracker.update_one_week(current_date)
            if saved_dates_tracker.three_week:
                saved_dates_tracker.update_three_week(current_date)
            if saved_dates_tracker.nine_week:
                saved_dates_tracker.update_nine_week(current_date)

        for period, stats in cache.items():
            for player, totals in stats.items():
                DynamicAveragesCalculator.write_averages(self.depot, totals[0]['game_date'], headers, period, totals)

    def load_seasons(self):
        seasons: Dict[str, Tuple[date, date]] = {}
        result: List[Dict[str, any]] = self.depot.do_query_many_dict(self._seasons_query)
        for item in result:
            seasons[item['season']] = (item['season_start'], item['season_end'])
        return seasons


def main(args):
    parser = ArgumentParser(prog="NBA Data Aggregator")
    parser.add_argument('--season', nargs='?', help='Season (YYYY-YY) to calculate averages for.')

    # args as object
    args: Namespace = parser.parse_args()

    guide: AnalyticsController = AnalyticsController()

    if args is None:
        raise ValueError('Args was not parsed correctly')

    if args.season is None:
        for key in guide.season_dates.keys():
            print(f'averaging season {key}')
            guide.calc_for_season(key)
        return

    # Calculate for season
    guide.calc_for_season(args.season)


if __name__ == "__main__":
    main(sys.argv[1:])
