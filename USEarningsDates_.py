import requests
import json
from datetime import datetime, timedelta
import logging


class UsEarningsDates(object):

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)

    def __init__(self):
        self.BASE_URL = 'http://finance.yahoo.com/calendar/earnings'
        self.BASE_STOCK_URL = 'https://finance.yahoo.com/quote'
        self.HISTORY_URL = 'https://finance.yahoo.com/quote/{}/history?period1={}&period2={}&interval=1d' \
                           '&filter=history&frequency=1d'
        self.ZACKS_URL = 'https://www.zacks.com/stock/research/{}/earnings-announcements'

    def earning_scraper(self, from_date, to_date, symbols):
        extract_data = []
        for symbol in symbols:
            earnings_date = []
            url = self.HISTORY_URL.format(symbol, from_date, to_date)
            history_data = self._get_data_dict(url)
            earning_data = history_data['context']['dispatcher']['stores']['HistoricalPriceStore']['prices']
            for data in earning_data[::-1]:
                earning_date = self.str_to_date(datetime.fromtimestamp(data['date']))
                earnings_date.append(earning_date)
            final_data = {symbol: earnings_date}
            extract_data.append(final_data)
            print('{}. Extracted the earning data of {}'.format(str(symbols.index(symbol) + 1), symbol))
        return extract_data

    def _get_data_dict(self, url):
        page = requests.get(url)
        page_content = page.text
        page_data_string = [row for row in page_content.split(
            '\n') if row.startswith('root.App.main = ')][0][:-1]
        page_data_string = page_data_string.split('root.App.main = ', 1)[1]
        return json.loads(page_data_string)

    def get_next_earnings_date(self, symbol):
        url = '{0}/{1}'.format(self.BASE_STOCK_URL, symbol)
        raw_page_data = None
        try:
            page_data_dict = self._get_data_dict(url)
            page_data = page_data_dict.get('context', {}).get('dispatcher', {}).get('stores', {})\
                .get('QuoteSummaryStore', {}).get('calendarEvents', {}).get('earnings', {}).get('earningsDate', {})
            if len(page_data) > 0:
                raw_page_data = datetime.fromtimestamp(page_data[0]['raw'])
        except:
            raise Exception('Invalid Symbol or Unavailable Earnings Date')

        return raw_page_data

    def earnings_on(self, date, symbol):
        date_str = date.strftime('%Y-%m-%d')
        self.logger.debug('Fetching earnings data for %s', date_str)
        dated_url = '{0}?day={1}'.format(self.BASE_URL, date_str)
        page_data_dict = self._get_data_dict(dated_url)
        earning_data = page_data_dict['context']['dispatcher']['stores']['ScreenerResultsStore']['results']['rows']
        final_data = {'companyName': None,
                      'epsactual': None,
                      'epsestimate': None,
                      'epssurprisepct': None,
                      'gmtOffsetMilliSeconds': None,
                      'earning_date': None,
                      'startdatetimetype': date,
                      'symbol': None
                      }
        for data in earning_data:
            if symbol == data['ticker']:
                final_data['companyName'] = data['companyshortname']
                final_data['epsactual'] = data['epsactual']
                final_data['epsestimate'] = data['epsestimate']
                final_data['epssurprisepct'] = data['startdatetime']
                final_data['gmtOffsetMilliSeconds'] = data['gmtOffsetMilliSeconds'],
                final_data['earning_date'] = data['startdatetime']
                final_data['startdatetimetype'] = data['startdatetimetype']
                final_data['symbol'] = data['ticker']
                return final_data

    def earnings_between(self, from_date, to_date, symbols):
        if from_date > to_date:
            raise ValueError(
                'From-date should not be after to-date')

        earnings_symbol_data = []
        delta = timedelta(days=1)
        for symbol in symbols:
            earnings_data = []
            current_date = from_date
            while current_date <= to_date:
                earning_on_data = self.earnings_on(current_date, symbol)
                if earning_on_data is not None:
                    earnings_data_result = {self.str_to_date(current_date): earning_on_data}
                    earnings_data.append(earnings_data_result)
                current_date += delta
            earnings_symbol_data.append({
                symbol: earnings_data
            })
        return earnings_symbol_data

    def str_to_date(self, date):
        return date.strftime('%Y-%m-%d')


if __name__ == '__main__':
    symbols = []
    with open('Symbols.csv') as csvfile:
        for line in csvfile.readlines():
            array = line.split(',')
            symbols.append(array[0].strip())
    symbols = symbols[1:]

    from_date = datetime.now() - timedelta(days=5*365)
    from_date_to_int = int(from_date.timestamp())
    to_date = datetime.now()
    to_date_to_int = int(to_date.timestamp())

    earningDate = UsEarningsDates()
    earningDate.earnings_between(from_date, to_date, symbols)
    # earningDate.earning_scraper(from_date_to_int, to_date_to_int, symbols)


