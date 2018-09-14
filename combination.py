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
        # db_temp = """CREATE TABLE IF NOT EXISTS combine ( id INT AUTO_INCREMENT PRIMARY KEY, SymbolNM VARCHAR(255), QuoteDTS VARCHAR(255), CriteriaMet VARCHAR(255), CombinationID VARCHAR(255), CombinationString VARCHAR(255))"""
        # symbol_db_temp = """CREATE TABLE IF NOT EXISTS symbols ( id INT AUTO_INCREMENT PRIMARY KEY, SymbolNM VARCHAR(255))"""
        # self.cursor.execute(symbol_db_temp)
        # self.save_to_db_symbols()

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

    def save_to_db_symbols(self):
        def add_quote(w):
            return '"{}"'.format(w)
        symbols = ['MCHP', 'MDRX', 'BHVN', 'MAN', 'CGNX', 'KMPR', 'CASI', 'MYOK', 'NBL', 'SRI', 'CARA', 'APEI', 'RFIL', 'RJF', 'CENTA', 'DERM', 'PES', 'INTC', 'IRET', 'IO', 'EXEL', 'TREE', 'JCOM', 'HTHT', 'PFPT', 'SMPL', 'IMMR', 'ARW', 'WSBC', 'BGG', 'ADMA', 'FBT', 'VRAY', 'CLW', 'SANM', 'EXAS', 'HMLP', 'PAH', 'BNED', 'FPRX', 'PBF', 'TPRE', 'JAG', 'EGBN', 'LABD', 'NEOS', 'JFR', 'NHTC', 'LNG', 'ABMD', 'SHAK', 'BGNE', 'EGRX', 'LRN', 'ZG', 'UGAZ', 'RECN', 'ROCK', 'CONN', 'FTR', 'RARE', 'PFGC', 'PJT', 'UGP', 'EDN', 'TYG', 'CHEF', 'FNSR', 'HLNE', 'PSCH', 'FND', 'NERV', 'YTRA', 'TERP', 'MDP', 'TXMD', 'AUO', 'BNO', 'GDXJ', 'UFPI', 'MZOR', 'RDY', 'MMLP', 'FNJN', 'VEEV', 'HSC', 'MOV', 'AMRS', 'CVI', 'IYW', 'FFIV', 'GLNG', 'IVTY', 'SHOO', 'CIR', 'DANOY', 'PDFS', 'MTOR', 'XOG', 'DORM', 'IIIN', 'MODN', 'ENDP', 'CTSO', 'CTS', 'IMMU', 'EBR', 'NVGS', 'GLMD', 'LFVN', 'YELP', 'EXR', 'SNCR', 'HUN', 'HFC', 'HIIQ', 'GCI', 'CAT', 'COLL', 'NBLX', 'VNET', 'JOE', 'WLH', 'EEX', 'CLR', 'NGLOY', 'REPH', 'ENTG', 'CRTO', 'AHT', 'CWH', 'ASHR', 'DO', 'LPX', 'FMCKJ', 'CNX', 'SPLK', 'CO', 'CBS', 'PI', 'RP', 'UA', 'ATNX', 'QTEC', 'TLYS', 'BCC', 'VLP', 'PBH', 'SD', 'SN', 'KIN', 'PNC', 'TRV', 'PIRS', 'MRSN', 'SAH', 'COLB', 'SBGSY', 'WMGI', 'ALNY', 'MDGL', 'KANG', 'NCS', 'PTGX', 'SUPV', 'VIAB', 'NWY', 'BHPLF', 'BHGE', 'KPTI', 'ASND', 'GMRE', 'EVRI', 'CBPO', 'MDSO', 'MVF', 'PRTA', 'RRC', 'STX', 'DISH', 'DDD', 'HHC', 'CNAT', 'IIN', 'XOXO', 'TCS', 'FATE', 'NTLA', 'ENZ', 'CRC', 'JD', 'NG', 'SJT', 'GES', 'BIG', 'APTI', 'XON', 'PETQ', 'HOS', 'BHE', 'SNAP', 'VTL', 'SLM', 'APPN', 'ADSK', 'DEST', 'EADSY', 'KURA', 'RIOT', 'IPG', 'RETA', 'FCB', 'MED', 'SGH', 'VET', 'VREX', 'FANUY', 'QHC', 'QLD', 'ZNGA', 'RESN', 'TKPYY', 'PDCE', 'MRTX', 'LJPC', 'MAG', 'PACW', 'SOXL', 'SIG', 'PTLA', 'FNMAS', 'NSIT', 'KL', 'ATEN', 'PODD', 'ENPH', 'SUP', 'URTY', 'GRBK', 'REI', 'GLCNF', 'OILU', 'ZFGN', 'PUK', 'MGA', 'RRR', 'OIS', 'LABU', 'MPC', 'EEQ', 'ALK', 'SBBP', 'ELS', 'SIGM', 'COBZ', 'COOL', 'VYGR', 'JNUG', 'TFX', 'NSANY', 'CATO', 'VGR', 'NKTR', 'GPS', 'PAGP', 'UBNK', 'RYI', 'AYX', 'UBNT', 'CARB', 'AAON', 'NFLX', 'ZUMZ', 'HIBB', 'SCS', 'BOOM', 'BURL', 'TCP', 'CAR', 'CODYY', 'EXTR', 'IVAC', 'ADI', 'AKRX', 'VKTX', 'XNET', 'AXGN', 'NPO', 'UBSI', 'CAL', 'PNK', 'SCGLY', 'TRHC', 'MLI', 'GIII', 'MD', 'PHM', 'EPZM', 'SNBR', 'ANET', 'FBR', 'USFD', 'ZAYO', 'ICE', 'ABG', 'VAC', 'EQGP', 'NBIX', 'GME', 'STBZ', 'EDU', 'CUTR', 'LOW', 'NVTR', 'BDN', 'SPAR', 'CATY', 'OEC', 'ANGI', 'AMSC', 'RMTI', 'IBP', 'PTCT', 'STML', 'KMT', 'CAF', 'HCHC', 'NEWR', 'GTXI', 'TOL', 'TILE', 'CPS', 'FDX', 'GVA', 'ARKW', 'TSLA', 'HOME', 'SHLM', 'VSH', 'NGVC', 'ELGX', 'UCO', 'CBIO', 'DCOM', 'APVO', 'EDIT', 'KRA', 'HQY', 'GTS', 'HEAR', 'JNP', 'GTHX', 'TKR', 'TREX', 'EVBG', 'BLUE', 'BRKL', 'HMN', 'TSEM', 'ABEO', 'ZGNX', 'TX', 'FPI', 'BLFS', 'GORO', 'ASMB', 'IPGP', 'RDFN', 'GTY', 'HSBC', 'LZB', 'AGYS', 'TOWN', 'HIFR', 'ITRI', 'MIK', 'VICR', 'APTS', 'MLNX', 'NUAN', 'SRCI', 'TUSK', 'BPY', 'TELNY', 'VCEL', 'VLO', 'SCVL', 'CCT', 'TTT', 'CCOI', 'CLUB', 'PLXS', 'ARWR', 'TPIC', 'POAHY', 'KEYS', 'AMBA', 'AOI', 'ARGX', 'ASUR', 'FOLD', 'GRMN', 'TGS', 'BNFT', 'INST', 'TSG', 'NEWM', 'LNTH', 'AKCA', 'MCFT', 'BYD', 'TISI', 'INTU', 'TECL', 'NTNX', 'RESI', 'ASTE', 'AM', 'ZTO', 'KRNT', 'TIVO', 'SPRO', 'SYRS', 'ADAP', 'ERX', 'ASIX', 'CVNA', 'NTRA', 'TRNC', 'XPO', 'SGMO', 'NR', 'CERS', 'RVNC', 'DK', 'WNS', 'CCRC', 'COTV', 'QTNT', 'SIVB', 'YEXT', 'FEP', 'TRYIY', 'GEMP', 'PAM', 'TAHO', 'DBVT', 'FIVE', 'CSIQ', 'PEN', 'VIV', 'GCO', 'SUI', 'SND', 'MGNX', 'ARA', 'SNX', 'MTSI', 'CDNA', 'IBOC', 'FENG', 'CALA', 'CLSD', 'ITCI', 'SFE', 'SFBS', 'REGI', 'UMPQ', 'TDC', 'MHLD', 'GLYC', 'AAOI', 'ATTU', 'FRME', 'SAP', 'EGAN', 'FTAI', 'NSTG', 'QQQX', 'TDY', 'SGSOY', 'GGAL', 'PLCE', 'CALM', 'AROC', 'PUMP', 'GOOS', 'USM', 'HCMLY', 'UEIC', 'FET', 'AIR', 'ORIT', 'GWPH', 'RACE', 'HPT', 'PBPB', 'BMA', 'NRG', 'NTES', 'CMCM', 'GG', 'BRC', 'GGP', 'XES', 'CFR', 'CPF', 'CLDR', 'I', 'SABR', 'MYRG', 'ADVM', 'AQN', 'IVC', 'STAG', 'URI', 'KALU', 'GEM', 'CYRX', 'MMYT', 'MTRX', 'NICE', 'HIMX', 'MBUU', 'VCYT', 'JOBS', 'ZSAN', 'WDR', 'ADS', 'RNP', 'CZR', 'BKE', 'AGIO', 'SINA', 'FANG', 'MEDP', 'YNDX', 'INO', 'ELVT', 'HTZ', 'GOGL', 'ATI', 'ATGE', 'TWOU', 'GFF', 'CSCO', 'SMLP', 'FUL', 'NOK', 'ESTE', 'DAKT', 'TRCO', 'SIEN', 'AMWD', 'FNGN', 'MGEN', 'BTI', 'KBE', 'PCTY', 'FSD', 'NWN', 'SORL', 'CRS', 'GPOR', 'JBAXY', 'MMI', 'FLXN', 'ILG', 'NDSN', 'HBI', 'CSTM', 'SSTI', 'GBT', 'WPM', 'PCOM', 'ITG', 'ARRY', 'TCMD', 'NUS', 'BOLD', 'SVRA', 'GNMK', 'SSP', 'RPD', 'HNGR', 'NLNK', 'PWR', 'BIOC', 'SPTN', 'FB', 'AXL', 'GALT', 'GTN', 'RTRX', 'ANAB', 'XENT', 'FFWM', 'LOXO', 'AGLE', 'CARS', 'ARCB', 'BBSEY', 'SSTK', 'RBA', 'AVDL', 'MORN', 'JNCE', 'IAG', 'LNN', 'PE', 'CYOU', 'MB', 'EXPE', 'WRD', 'CPYYY', 'ENS', 'Z', 'CX', 'LMAT', 'LCI', 'LFUS', 'UWT', 'UEPS', 'APLS', 'AKBA', 'PNFP', 'UNIT', 'FANH', 'WCC', 'GEF', 'TEI', 'BDJ', 'BXC', 'ISNPY', 'MSFT', 'JDST']
        for symbol in symbols:
            self.cursor.execute("""INSERT INTO symbols (SymbolNM) VALUES ({})"""
                                .format(
                                add_quote(symbol)))
            self.conn.commit()
        print('succeessed')

    def pull_col_names(self):
        self.cursor.execute("SHOW COLUMNS FROM history ")
        column_names = self.cursor.fetchall()
        return column_names[1:]

    def pull_history(self):
        self.cursor.execute("SELECT * FROM history")
        histories = self.cursor.fetchall()
        return histories

    def pull_symbols_from_db(self):
        self.cursor.execute("SELECT SymbolNM FROM symbols")
        symbols = self.cursor.fetchall()
        return symbols

    def combination_from_symbol(self, num_of_indic):
        symbols = self.pull_symbols_from_db()
        col_names = self.pull_col_names()
        histories = self.pull_history()
        sorted_files = []

        for result in histories:
            for symbol in symbols:
                if ''.join(symbol) == result[1]:
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
    # symbols = []
    # with open('Symbols.csv') as csvfile:
    #     for line in csvfile.readlines():
    #         array = line.split(',')
    #         symbols.append(array[0].strip())
    # symbols = symbols[1:]
    num_of_indic = 15
    num_maximum_string = 6
    cb.combination_from_symbol(num_of_indic)
    # histories = []
    # with open('history.csv') as csvfile:
    #     for line in csvfile.readlines():
    #         array = line.split(',')
    #         histories.append(array)
    # histories = histories[1:]
    # cb.export_to_history(histories)
