import os
import csv
import glob
import pymysql
from itertools import combinations


class Combination(object):
    def __init__(self):
        self.current_id = 1
        self.current_date = None

        self.host = '127.0.0.1'
        self.username = 'root'
        self.password = 'root'
        self.db = 'dbo'
        self.port = int('3306')

        self.cursor = self.connect_db()
        db_temp = """CREATE TABLE IF NOT EXISTS combine ( id INT AUTO_INCREMENT PRIMARY KEY, SymbolNM VARCHAR(255), QuoteDTS VARCHAR(255), CriteriaMet VARCHAR(255), CombinationID VARCHAR(255), CombinationString VARCHAR(255))"""
        self.cursor.execute(db_temp)

    def connect_db(self):
        cnx = {'host': self.host,
               'username': self.username,
               'password': self.password,
               'db': self.db,
               'port': int(self.port)
               }
        self.conn = pymysql.connect(db=cnx['db'], host=cnx['host'], port=cnx['port'], user=cnx['username'],
                                    password=cnx['password'])
        self.cursor = self.conn.cursor()
        return self.cursor

    def pull_col_names(self):
        self.cursor.execute("SHOW COLUMNS FROM history ")
        column_names = self.cursor.fetchall()
        return column_names[1:]

    def pull_history(self):
        self.cursor.execute("SELECT * FROM history")
        histories = self.cursor.fetchall()
        return histories

    def combination_from_symbol(self, symbols, num_of_indic):
        col_names = self.pull_col_names()
        histories = self.pull_history()
        sorted_files = []
        for result in histories:
            for symbol in symbols:
                if symbol == result[1]:
                    sorted_files.append(result)

        self.combination_data(sorted_files, col_names, num_of_indic)
        return

    # Combines the indicators with value is 1
    def combination_data(self, sorted_files, col_names, num_of_indic):
        indicator_list = []
        date_list = []
        criteriaMet_list = []
        for data in col_names[3:][:num_of_indic]:
            indicator_list.append(data[0])
        for data in sorted_files:
            date_list.append(data[2])
            criteriaMet_list.append(data[3])

        data_dict_list = []
        for i, data in enumerate(sorted_files):
            data_dict = {}
            indicators = []
            for j, d in enumerate(data[4:][:num_of_indic]):
                if '1' in d:
                    indicators.append(indicator_list[j])
            if indicators:
                data_dict['date'] = date_list[i]
                data_dict['criteriaMet'] = criteriaMet_list[i]
                data_dict['indicators'] = indicators
                data_dict['symbol'] = data[1]
                data_dict_list.append(data_dict)

        for data in data_dict_list:
            indicators = data['indicators']
            l = len(data['indicators'])
            if l > num_maximum_string:
                indicators = data['indicators'][:num_maximum_string]
                l = num_maximum_string
            for i in range(1, l + 1):
                for c in combinations(indicators, i):
                    combine_indicators = list(c)
                    # self.export_to_csv(data['symbol'], data["date"], data['criteriaMet'], combine_indicators)
                    self.export_to_db(data['symbol'], data["date"], data['criteriaMet'], combine_indicators)

        return

    # Export into csv combined data
    def export_to_csv(self, symbol, date, criteriaMet, combine_indicators):
        combine_path = 'combinations_{}.csv'.format(symbol)
        if criteriaMet == 'Y':
            top_combine_path = 'top_combinations_{}.csv'.format(symbol)

        if self.current_date == date:
            self.current_id += 1
        else:
            self.current_id = 1
            self.current_date = date
        try:
            self.filter_combine_path(combine_path, symbol, date, criteriaMet, self.current_id, combine_indicators)
            if criteriaMet == 'Y':
                self.filter_combine_path(top_combine_path, symbol, date, criteriaMet, self.current_id, combine_indicators)
        except:
            print('Error')

    def export_to_db(self, symbol, date, criteriaMet, combine_indicators):
        def add_quote(w):
            return '"{}"'.format(w)
        if self.current_date == date:
            self.current_id += 1
        else:
            self.current_id = 1
            self.current_date = date
        try:
            self.cursor.execute(
                "INSERT INTO combine (SymbolNM, QuoteDTS, CriteriaMet, CombinationID, CombinationString) VALUES ({}, {}, {}, {}, {})"
                    .format(
                    add_quote(symbol),
                    add_quote(date),
                    add_quote(criteriaMet),
                    add_quote(self.current_id),
                    add_quote(', '.join(combine_indicators))
                ))
            self.conn.commit()
        except Exception as e:
            print(e)

    def filter_combine_path(self, combine_output_path, symbol, date, criteriaMet, current_id, combine_indicators):
        if os.path.isfile(combine_output_path):
            csv_file = open(combine_output_path, 'a+')
        else:
            csv_file = open(combine_output_path, 'w')

        csv_writer = csv.writer(csv_file, lineterminator='\n')

        row = [symbol, date, criteriaMet, current_id, ', '.join(combine_indicators)]
        csv_writer.writerow(row)
        csv_file.close()

    def _find_between(self, s, first, last, offset=0):
        try:
            start = s.index(first, offset) + len(first)
            end = s.index(last, start)
            return s[start:end]
        except ValueError:
            return ""

    def export_to_history(self, histories):
        def add_quote(w):
            return '"{}"'.format(w)
        for history in histories:
            self.cursor.execute("""INSERT INTO dbo.history(
                                                        SymbolNM, QuoteDTS, CriteriaMet, Indicator1, Indicator2,
                                                        Indicator3, Indicator4, Indicator5, Indicator6, Indicator7,
                                                        Indicator8, Indicator9, Indicator10, Indicator11, Indicator12,
                                                        Indicator13, Indicator14, Indicator15, Indicator16, Indicator17,
                                                        Indicator18, Indicator19, Indicator20, Indicator21, Indicator22,
                                                        Indicator23, Indicator24, Indicator25, Indicator26, Indicator27,
                                                        Indicator28, Indicator29, Indicator30, Indicator31, Indicator32,
                                                        Indicator33, Indicator34, Indicator35, Indicator36, Indicator37,
                                                        Indicator38, Indicator39, Indicator40, Indicator41, Indicator42,
                                                        Indicator43, Indicator44, Indicator45, Indicator46, Indicator47,
                                                        Indicator48, Indicator49, Indicator50) VALUES (
                                                        {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                                                        {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                                                        {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                                                        {}, {}, {}, {}, {})"""
                                .format(
                                add_quote(history[0]),
                                add_quote(history[1]),
                                add_quote(history[2]),
                                add_quote(history[3]),
                                add_quote(history[4]),
                                add_quote(history[5]),
                                add_quote(history[6]),
                                add_quote(history[7]),
                                add_quote(history[8]),
                                add_quote(history[9]),
                                add_quote(history[10]),
                                add_quote(history[11]),
                                add_quote(history[12]),
                                add_quote(history[13]),
                                add_quote(history[14]),
                                add_quote(history[15]),
                                add_quote(history[16]),
                                add_quote(history[17]),
                                add_quote(history[18]),
                                add_quote(history[19]),
                                add_quote(history[20]),
                                add_quote(history[21]),
                                add_quote(history[22]),
                                add_quote(history[23]),
                                add_quote(history[24]),
                                add_quote(history[25]),
                                add_quote(history[26]),
                                add_quote(history[27]),
                                add_quote(history[28]),
                                add_quote(history[29]),
                                add_quote(history[30]),
                                add_quote(history[31]),
                                add_quote(history[32]),
                                add_quote(history[33]),
                                add_quote(history[34]),
                                add_quote(history[35]),
                                add_quote(history[36]),
                                add_quote(history[37]),
                                add_quote(history[38]),
                                add_quote(history[39]),
                                add_quote(history[40]),
                                add_quote(history[41]),
                                add_quote(history[42]),
                                add_quote(history[43]),
                                add_quote(history[44]),
                                add_quote(history[45]),
                                add_quote(history[46]),
                                add_quote(history[47]),
                                add_quote(history[48]),
                                add_quote(history[49]),
                                add_quote(history[50]),
                                add_quote(history[51]),
                                add_quote(history[52])
            ))
            self.conn.commit()
        return
if __name__ == '__main__':
    cb = Combination()
    # path = os.getcwd()  # Point the current directory (can change the directory manually)
    # extension = 'csv'   # Point the extension of file
    # os.chdir(path)      # Open the file from directory.

    # results = [i for i in glob.glob('*.{}'.format(extension))]

    # Import the symbol data from origin csv
    symbols = []
    with open('Symbols.csv') as csvfile:
        for line in csvfile.readlines():
            array = line.split(',')
            symbols.append(array[0].strip())
    symbols = symbols[1:]
    num_of_indic = 15
    num_maximum_string = 6
    cb.combination_from_symbol(symbols, num_of_indic)
    # histories = []
    # with open('history.csv') as csvfile:
    #     for line in csvfile.readlines():
    #         array = line.split(',')
    #         histories.append(array)
    # histories = histories[1:]
    # cb.export_to_history(histories)
