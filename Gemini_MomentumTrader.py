#
# Python Wrapper Class for
# Gemini API
# by Michael Schwed
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
# Larger parts of the  docstring texts taken
# from the official Gemini API documentation under
# https://docs.gemini.com/rest-api/
#
import zmq
import hmac
import hashlib
import time
import base64
import json
import requests
import configparser
import datetime
import numpy as np
import pandas as pd


class MomentumTrader(object):
    def __init__(self, key, secrect_key, symbol, amount, price, side,
                              option, client_order_id, momentum, sandbox, debug=False):
        self.key = key
        self.secret_key = secrect_key
        self.symbol = symbol
        self.amount = amount
        self.price = price
        self.side = side
        self.momentum = momentum
        self.option = option
        self.client_order_id = client_order_id

        self.data = pd.DataFrame()
        if sandbox:
            self.url = 'https://api.sandbox.gemini.com/v1/'
        else:
            self.url = 'https://api.gemini.com/v1/'

        self.last_order_id = None
        self.debug = debug
        self.ticks = 0

    def start(self):
        method = 'pubticker/{}'.format(self.symbol)
        response = requests.get(self.url + method)
        if response.status_code == 200:
            json_resp = json.loads(response.text)
        if json_resp:
            ask = json.loads(response.text).get('ask')
            bid = json.loads(response.text).get('bid')
            time = str(datetime.datetime.fromtimestamp(json.loads(response.text).get('volume').get('timestamp') / 1e3))

        self.on_success(time, bid, ask)
        return 'Finished'

    def on_success(self, time, bid, ask):
        ''' Takes actions when new tick data arrives. '''
        self.ticks += 1
        print(self.ticks, end=' ')

        self.data = self.data.append(
            pd.DataFrame({'time': [time], 'bid': [bid], 'ask': [ask]}))
        self.data.index = pd.DatetimeIndex(self.data['time'])
        self.resam = self.data.resample('5s', label='right').last()
        self.resam['mid'] = self.resam.mean(axis=1)
        self.resam = self.resam.ffill()
        self.resam['returns'] = np.log(self.resam['mid'] / self.resam['mid'].shift(1))
        self.resam['position'] = np.sign(
            self.resam['returns'].rolling(self.momentum).mean())

        self.new_order(self.symbol, self.amount, self.price, self.side, self.option,
                       time, bid, ask, self.client_order_id)

    def send_privat_request(self, method, payload):
        ''' Sends all privat requests to the Gemini server.
        '''
        payload['nonce'] = str(int(time.time() * 100000))
        payload = json.dumps(payload)

        payload_encode = base64.b64encode(bytearray(payload, 'utf-8'))
        sig = hmac.new(bytearray(self.secret_key, 'utf-8'),
                       payload_encode, hashlib.sha384).hexdigest()
        headers = {'X-GEMINI-APIKEY': self.key,
                   'X-GEMINI-PAYLOAD': payload_encode,
                   'X-GEMINI-SIGNATURE': sig}
        url = self.url + method
        if self.debug:
            print('URL: ', url)
            print('Payload: ', payload)
        resp = requests.post(url, headers=headers, timeout=10).json()
        print('\n\n', resp, '\n')

        msg = 'SMA | Market data:\n%s' % self.resam.iloc[-1]
        socket.send_string(msg)

    def new_order(self, symbol, amount, price, side, option, time, bid, ask,
                  client_order_id):
        method = 'order/new'
        params = {'request': '/v1/order/new',
                  'symbol': symbol,
                  'type': 'exchange limit'}
        try:
            amount = float(amount)
        except:
            raise TypeError('amount must be a number')
        try:
            price = float(price)
        except:
            raise TypeError('price must be a number')
        if side not in ['buy', 'sell']:
            raise ValueError('side must be either "buy" or "sell"')

        if option and option not in ['maker-or-cancel',
                                     'immediate-or-cancel',
                                     'auction-only']:
            raise ValueError(
                'option must either be empty or one of "maker-or-cancel", \
              "immediate-or-cancel", "auction-only"')

        params['amount'] = str(amount)
        params['price'] = str(price)
        params['side'] = side

        options = list()
        options.append(option)
        if len(option) is not 0:
            params['options'] = options

        if client_order_id is not False:

            if self.last_order_id and not client_order_id > self.last_order_id:
                raise ValueError(
                    'client_order_id is not increasing ( %s !> %s )'
                    % (client_order_id, self.last_order_id))

            self.last_order_id = client_order_id
            params['client_order_id'] = str(client_order_id)

        res = self.send_privat_request(method, params)
        return res

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('pyalgo.cfg')
    key = config['gemini']['key']
    secret_key = config['gemini']['secret_key']

    symbols = ['BTCUSD', 'ETHUSD', 'ETHBTC']
    for symbol in symbols:
        momentum = MomentumTrader(key, secret_key, symbol, amount='1', price='200', side='buy',
                                  option='maker-or-cancel', momentum=6, sandbox=True, client_order_id=None)
        momentum.start()