import os
import csv
import json
import requests
import collections

from lxml import html
from datetime import datetime, timedelta


class UsEarningsDates(object):
    def __init__(self):
        self.ZACKS_URL = 'https://www.zacks.com/stock/research/{}/earnings-announcements'
        self.counter = collections.defaultdict(int)

    # Main function
    def zacks_earning(self, symbols, from_date_to_int):
        earngins_list = {}
        try:
            for symbol in symbols:
                earnings_data = self.finance_earning_data(symbol, from_date_to_int)
                if earnings_data:
                    earngins_list[symbol] = earnings_data[::-1]
                else:
                    earngins_list[symbol] = None

                self.export_to_csv(earngins_list, symbol)
                print('{}. Extracted the earning data of {}'.format(str(symbols.index(symbol) + 1), symbol))
            print('successed')
        except Exception as e:
            print(e)
            raise e

    # Extract the earning data from site
    def finance_earning_data(self, symbol, from_date_to_int):
        url = self.ZACKS_URL.format(symbol)
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/68.0.3440.106 Safari/537.36'
                   }
        resp = requests.get(url, headers=headers)
        earning_data = []
        earnings_data = []
        page = None
        if resp.status_code == 200:
            page = resp.text
        if page:
            company = html.fromstring(page).xpath("//header//h1//a/text()")
            json_page = self._find_between(page, "document.obj_data = ", "};") + '}'
            try:
                earning_data = json.loads(json_page).get('earnings_announcements_earnings_table')
            except:
                'Json Error in {}'.format(symbol)
        for data in earning_data:
            earning = {}
            int_earning_data = int(datetime.strptime(data[0], "%m/%d/%Y").timestamp())

            if int_earning_data < from_date_to_int:
                break
            earning['earning_date'] = datetime.strptime(data[0], "%m/%d/%Y").strftime("%Y-%m-%d")
            if company:
                earning['company'] = company[0]
            else:
                earning['company'] = None
            earning['period_ending'] = data[1]
            if data[2] and data[2] is not '--':
                earning['estimate'] = data[2]
            else:
                earning['estimate'] = None

            if data[3] and data[3] is not '--':
                earning['reported'] = data[3]
            else:
                earning['reported'] = data[3]
            if data[4] and data[4] is not '--':
                surprise = html.fromstring(data[4]).xpath('//div/text()')
                if surprise:
                    earning['surprise'] = surprise[0]
                else:
                    earning['surprise'] = None
            else:
                earning['surprise'] = None
            if data[5] and data[5] is not '--':
                per_surprise = html.fromstring(data[5]).xpath('//div/text()')
                if per_surprise:
                    earning['per_surprise'] = per_surprise[0]
                else:
                    earning['per_surprise'] = None
            else:
                earning['per_surprise'] = None
            earnings_data.append(earning)
        return earnings_data

    # Export to CSV the extracted earning data
    def export_to_csv(self, earning_list, symbol):
        output_dir_path = 'earning.csv'
        data = earning_list[symbol]
        if data:
            for url in data:
                try:
                    if os.path.isfile(output_dir_path):
                        csv_file = open(output_dir_path, 'a+')
                    else:
                        csv_file = open(output_dir_path, 'w')

                    csv_writer = csv.writer(csv_file)

                    row = [symbol, str(url['company']), str(url['earning_date']), str(url['estimate']),
                           str(url['per_surprise']), str(url['period_ending']), str(url['reported']), str(url['surprise'])]
                    csv_writer.writerow(row)
                    csv_file.close()
                except:
                    print(url)
                    continue
        return

    def _find_between(self, s, first, last, offset=0):
        try:
            start = s.index(first, offset) + len(first)
            end = s.index(last, start)
            return s[start:end]
        except ValueError:
            return ""

if __name__ == '__main__':
    symbols = []

    # Import the symbol data from origin csv
    with open('Symbols.csv') as csvfile:
        for line in csvfile.readlines():
            array = line.split(',')
            symbols.append(array[0].strip())
    symbols = symbols[1:]

    # Get the date for last 5 years from current date
    from_date = datetime.now() - timedelta(days=5 * 365)
    from_date_to_int = int(from_date.timestamp())

    # Get the current date
    to_date = datetime.now()
    to_date_to_int = int(to_date.timestamp())

    earningDate = UsEarningsDates()
    earningDate.zacks_earning(symbols, from_date_to_int)
