import sys
from argparse import ArgumentParser, Namespace
from calendar import monthrange
from datetime import datetime, date
from html.parser import HTMLParser
from typing import List, Dict, Tuple

from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from nba.collector.depot import NBADataSink

_base_url = 'http://www.basketball-reference.com/friv/dailyleaders.cgi?month={}&day={}&year={}'


class RowCollector(object):
    def __init__(self):
        self.row = list()

    def parse_row(self, data: Dict[str, str], headers: List[str]) -> None:
        for header in headers:
            self.row.append(data[header])

    def to_map(self, headers: List[str]) -> Dict[str, any]:
        row: Dict[str, any] = {}
        header_counter: int = 0
        for cell in self.row:
            row[headers[header_counter]] = cell
            header_counter += 1
        return row

    def to_csv(self) -> str:
        csv = ''
        count = len(self.row) - 1
        current = 0
        for cell in self.row:
            csv += cell
            if current < count:
                csv += ','
                current += 1
                continue
            csv += '\n'
        return csv


class TableWrapper(object):

    def __init__(self):
        self.rows: List[RowCollector] = []
        self.headers: List[str] = []

    def add_header(self, header: str) -> None:
        self.headers.append(header)

    def get_header(self, index: int) -> str:
        return self.headers[index]

    def add_row(self, row: RowCollector) -> None:
        self.rows.append(row)

    def get_headers(self) -> List[str]:
        return self.headers

    def to_map(self) -> List[Dict[str, any]]:
        rows: List[Dict[str, any]] = []
        for row in self.rows:
            rows.append(row.to_map(self.headers))
        return rows

    def to_csv(self) -> str:
        csv: str = ''
        header_count = len(self.headers) - 1
        current: int = 0
        for header in self.headers:
            csv += header
            if current < header_count:
                csv += ', '
                current += 1
                continue
            csv += '\n'
        for row in self.rows:
            csv += row.to_csv()
        return csv


# create a subclass and override the handler methods
class NBAStatsParser(HTMLParser):

    def __init__(self):
        super().__init__()
        self.is_collecting: bool = False
        self.is_collecting_rows: bool = False
        self.is_collecting_headers: bool = False
        self.table: TableWrapper = TableWrapper()
        self.cells: Dict[str, str] = {}
        self.cell_counter: int = 0

    def handle_starttag(self, tag: str, atts: str) -> None:
        stats = [item for item in atts if 'stats' in item]
        if tag == 'table' and len(stats) > 0:
            self.is_collecting = True
            return
        if tag == 'thead' and self.is_collecting:
            self.is_collecting_headers = True
            return
        thead = [item for item in atts if 'thead' in item]
        if tag == 'tr' and self.is_collecting:
            self.is_collecting_rows = len(thead) == 0
        if tag == 'td' and self.is_collecting_rows:
            self.cell_counter += 1

    def handle_endtag(self, tag: str) -> None:
        if self.is_collecting_headers and tag == 'thead':
            self.is_collecting_headers = False
            return

        if self.is_collecting_rows and tag == 'tr':
            self.is_collecting_rows = False
            row: RowCollector = RowCollector()
            if len(self.cells) > 0:
                row.parse_row(self.cells, self.table.get_headers())
                self.table.add_row(row)
            self.cells = {}
            self.cell_counter = 0
            return

        if self.is_collecting and tag == 'table':
            self.is_collecting = False
            self.is_collecting_rows = False

    def handle_data(self, data: str) -> None:
        if self.is_collecting_headers:
            header = data.strip()
            if len(header.strip()) is 0:
                if len(self.table.headers) == 3:
                    self.table.add_header('game_location')
                return
            self.table.add_header(header)
            return

        if self.is_collecting_rows:
            value = data.strip()
            # Strip out columns we don't collect
            if value is 'W' \
                    or value is 'L':
                return

            header_index = len(self.cells)
            if self.cell_counter - header_index >= 2 or (header_index == 3 and value is not '@'):
                self.cells[self.table.get_header(header_index)] = 'False' if header_index == 3 else ''
                header_index += 1
            header = self.table.get_header(header_index)
            if 'game_location' is header:
                value = 'True' if value == '@' else 'False'
            self.cells[header] = value

    def store_to_csv(self, file_name: str) -> None:
        csv = open(file_name, 'w')
        csv.write(self.table.to_csv())
        csv.close()

    def store_to_db(self, game_date: date, depot: NBADataSink) -> None:
        depot.store_records(game_date, self.table.to_map())

    def error(self, message) -> None:
        pass


def execute_selenium(current_date: date, depot: NBADataSink, url: str):
    options: Options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    with webdriver.Chrome(options=options) as browser:
        browser.get(url)
        print(f'browser called {url}')

        stats: WebElement = browser.find_element(By.ID, 'stats')
        elements: List[WebElement] = stats.find_elements(By.XPATH, f'//{stats.tag_name.strip()}/tbody/tr/child::td')
        rows: List[Dict[str, str]] = []
        print(f'begin parsing {len(elements)} rows')
        row: Dict[str, str] = {}

        has_collected_player: bool = False
        for element in elements:
            att: str = element.get_attribute('class')
            if att == 'thead':
                continue
            att = element.get_attribute('scope')
            if att == 'col':
                continue
            att = element.get_attribute('data-stat')
            if att not in depot.element_attribute_mappings.keys():
                continue
            if att == 'player':
                if not has_collected_player:
                    has_collected_player = True
                else:
                    if len(row) > 0:
                        rows.append(row)
                        row = {}
            if att == 'game_location':
                row[att] = 'True' if element.text == '@' else 'False'
            else:
                row[att] = element.text

        depot.store_records(current_date, rows, True)


def execute_query(current_date: date, depot: NBADataSink, url: str):
    options: Options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    with webdriver.Chrome(options=options) as browser:
        print('loaded driver')
        browser.get(url)
        print(f'browser called {url}')
        content: str = browser.page_source.strip()
        page: BeautifulSoup = BeautifulSoup(content, 'html.parser')
        data = page.select("#stats")[0]
        parser = NBAStatsParser()
        as_string = str(data).strip()
        parser.feed(as_string)
        parser.store_to_db(current_date, depot)
    # parser.store_to_csv(
    #     '../../../data/{}-{}-{}.csv'.format(current_date.year, current_date.month, current_date.day))


def main(args):
    # url = 'http://www.basketball-reference.com/friv/dailyleaders.cgi?month=2&day=14&year=2016'

    parser = ArgumentParser(prog="NBA Loader")
    parser.add_argument('--season', nargs='?', help='Season date belong to')
    parser.add_argument('--from_year', nargs='?', help='Year to being collecting')
    parser.add_argument('--from_month', nargs='?', help='Month to being collecting')
    parser.add_argument('--from_day', nargs='?', help='Day of the month to begin collecting')
    parser.add_argument('--to_year', nargs='?', help='Year to stop collecting')
    parser.add_argument('--to_month', nargs='?', help='Month to stop collecting')
    parser.add_argument('--to_day', nargs='?', help='Day of the month to stop collecting')
    # args as object
    args: Namespace = parser.parse_args()

    if args is None:
        raise ValueError('Args was not parsed correctly')

    season: str = args.season

    if season is None and (args.from_year is None or args.from_month is None):
        raise ValueError('Must provide from year and from current as a minimum.')

    from_year: int = int(args.from_year) if args.from_year is not None else None
    from_month: int = int(args.from_month) if args.from_month is not None else None
    from_day: int = int(args.from_day) if args.from_day is not None else None

    to_year: int = int(args.to_year) if args.to_year is not None else from_year
    to_month: int = int(args.to_month) if args.to_month is not None else from_month
    to_day: int = int(args.to_day) if args.to_day is not None else None

    depot: NBADataSink = NBADataSink()
    depot.season(season)

    if season is not None and from_year is None:
        dates: Tuple[date, date] = depot.season_dates[season]
        from_year = dates[0].year
        from_month = dates[0].month
        from_day = dates[0].day
        to_year = dates[1].year
        to_month = dates[1].month
        to_day = dates[1].day

    if from_year is None or from_month is None:
        raise ValueError('Must provide from and to dates')

    current_date: date = datetime.now().replace(year=from_year, month=from_month,
                                                day=from_day if from_day is not None else 1).date()

    last_day = monthrange(to_year, to_month)
    end_date: date = datetime.now().replace(year=to_year, month=to_month,
                                            day=last_day[1] if to_day is None else to_day).date()
    try:
        while current_date <= end_date:
            url: str = _base_url.format(current_date.month, current_date.day, current_date.year)
            print(current_date)
            if not depot.has_date_been_loaded(game_date=current_date):
                try:
                    # execute_selenium(current_date, depot, url)
                    execute_query(current_date, depot, url)
                except IndexError as err:
                    print(f'No games for date {current_date.day} {current_date.month}, {current_date.year}: ')
            else:
                print(f'Skipping date {current_date}, already loaded')
            current_date += relativedelta(days=1)
            if current_date > end_date:
                break
    except Exception as err:
        print(err)


if __name__ == "__main__":
    main(sys.argv[1:])
