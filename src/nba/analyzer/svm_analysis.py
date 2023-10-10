import sys
from argparse import ArgumentParser, Namespace
from datetime import date
from typing import Dict, Tuple, List

from caritas.core.depot.abc.api import SQLBasedExternalDepot
from sklearn import svm
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler, Normalizer, QuantileTransformer, KernelCenterer, RobustScaler
from sklearn.svm import LinearSVC

from nba.analyzer.averages import AnalyticsController, AveragePeriods
from nba.collector.depot import init_db


class Classifier:
    """
    Takes matrix of player data points as X axis and player names as Y for a season
    """

    _matrix_query: str = 'SELECT player, {fields} FROM NBA.running_player_averages_{period} WHERE season BETWEEN ' \
                         '\'{from_season}\' AND \'{to_season}\' GROUP BY player'

    _x_axis: Dict[str, List[str]] = {
        'all': ['overall_efficiency', 'field_goals', 'free_throws', 'points_scored'],
        'center': ['overall_efficiency', 'game_rating_score', 'center_player_stats'],
        'guard': ['overall_efficiency', 'game_rating_score', 'guard_player_stats'],
        'forward': ['overall_efficiency', 'game_rating_score', 'forward_player_stats']
    }

    def __init__(self):
        self.depot: SQLBasedExternalDepot = init_db()

    def load_matrix(self, from_season: str, to_season: str, axis_type: str) -> Tuple[List[List[float]], List[str]]:
        grouped_by_player: Dict[str, List[float]] = {}
        y_axis: List[str] = []
        columns: List[str] = Classifier._x_axis.get(axis_type)
        fields: str = ''
        for col in columns:
            fields += f'sum({col}) as {col},'
        fields = fields[0: -1]
        for period in AveragePeriods:
            records: List[Dict[str, any]] = self.depot.do_query_many_dict(
                Classifier._matrix_query.format(fields=fields, period=period.name, from_season=from_season,
                                                to_season=to_season))
            for rec in records:
                player = str(rec.get('player'))
                if player not in grouped_by_player:
                    grouped_by_player[player] = []
                    y_axis.append(player)
                for axis in columns:
                    grouped_by_player[player].append(float(rec.get(axis)))

        x_matrix: List[List[float]] = []
        for player in y_axis:
            x_matrix.append(grouped_by_player.get(player))
        return x_matrix, y_axis

    def classify(self, from_season: str, to_season: str, axis_type: str):
        if axis_type not in Classifier._x_axis:
            raise ValueError(f'Unsupported type {axis_type} for x-axis')
        x_y: Tuple[List[List[float]], List[str]] = self.load_matrix(from_season, to_season, axis_type)

        clf: svm.LinearSVC = make_pipeline(QuantileTransformer(n_quantiles=len(x_y[0])),
                                           RobustScaler(quantile_range=(0.0001, 6.0), unit_variance=True),
                                           StandardScaler(),
                                           LinearSVC(random_state=0, tol=0.0001, C=100, max_iter=100_000))

        clf.fit(x_y[0], x_y[1])
        print(clf.score(x_y[0], x_y[1]))
        predict_stats = [[1.04, 4.5, 2.69, 4.0,
                          1.045, 5.6, 2.8, 4.5,
                          1.06, 6.9, 2.85, 5.0]]
        dec = clf.decision_function(predict_stats)
        print(dec)
        print(clf.predict(predict_stats))


def main(args):
    parser = ArgumentParser(prog="NBA Data Analyzer")
    parser.add_argument('--from_season', nargs='?', help='Season (YYYY-YY) to train from.')
    parser.add_argument('--to_season', nargs='?', help='Season (YYYY-YY) to train to.')
    # args as object
    args: Namespace = parser.parse_args()

    from_season: str = args.from_season
    to_season: str = args.to_season

    if from_season is None or to_season is None:
        raise ValueError('Must provide from and to season values to being calculating.')

    controller: AnalyticsController = AnalyticsController()
    season_dates: Dict[str, Tuple[date, date]] = controller.load_seasons()

    if from_season not in season_dates or to_season not in season_dates:
        raise ValueError(f'from seasons {from_season} or to_season {to_season} not known in {season_dates}')

    classifier: Classifier = Classifier()
    classifier.classify(from_season, to_season, 'all')


if __name__ == "__main__":
    main(sys.argv[1:])
