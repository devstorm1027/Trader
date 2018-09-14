#
# Python Script
# with Momentum Trading Class
# for Oanda v20
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
import v20
import numpy as np
import pandas as pd
import configparser
import requests

config = configparser.ConfigParser()
# config.read('../pyalgo.cfg')
config.read('oanda.cfg')
# account_id = config['oanda_v20']['account_id']
# access_token = config['oanda_v20']['access_token']
account_id = config['oanda']['account_id']
access_token = config['oanda']['access_token']
# oanda = opy.API(environment='practice',
# access_token=config['oanda']['access_token'])

class MomentumTrader(object):
    def __init__(self, momentum, instrument, units, *args, **kwargs):

        self.ticks = 0
        self.position = 0
        self.data = pd.DataFrame()
        self.momentum = momentum
        self.account_id = account_id
        self.instrument = instrument
        self.units = units
        self.ctx = v20.Context(
            'api-fxpractice.oanda.com',
            443,
            True,
            application='sample_code',
            token=access_token,
            datetime_format='RFC3339'
        )
        self.ctx_stream = v20.Context(
            'stream-fxpractice.oanda.com',
            443,
            True,
            application='sample_code',
            token=access_token,
            datetime_format='RFC3339'
        )

    def create_order(self, units):
        ''' Places orders with Oanda. '''
        request = self.ctx.order.market(
            self.account_id,
            instrument=self.instrument,
            units=units,
        )
        order = request.get('orderFillTransaction')
        print('\n\n', order.dict(), '\n')

    def start(self, stop=250):
        ''' Starts the streaming of data and the triggering of actions. '''
        response = self.ctx_stream.pricing.stream(
            self.account_id,
            snapshot=True,
            instruments=self.instrument
        )
        for msg_type, msg in response.parts():
            if msg_type == 'pricing.Price':
                self.on_success(msg.time, msg.bids[0].price,
                                          msg.asks[0].price)
            if self.ticks == stop:
                if self.position == 1:
                    self.create_order(-self.units)
                elif self.position == -1:
                    self.create_order(self.units)
                return 'Finished.'

    def on_success(self, time, bid, ask):
        ''' Takes actions when new tick data arrives. '''
        self.ticks += 1
        print(self.ticks, end=' ')
        self.data = self.data.append(
            pd.DataFrame({'time': [time], 'bid': [bid], 'ask': [ask]}))
        self.data.index = pd.DatetimeIndex(self.data['time'])
        resam = self.data.resample('5s', label='right').last()
        resam['mid'] = resam.mean(axis=1)
        resam = resam.ffill()
        resam['returns'] = np.log(resam['mid'] / resam['mid'].shift(1))
        resam['position'] = np.sign(
            resam['returns'].rolling(self.momentum).mean())
        # print(resam[['ask', 'returns', 'position']].tail())
        if len(resam) > self.momentum:
            if resam['position'].iloc[-2] == 1:
                if self.position == 0:
                    self.create_order(self.units)
                elif self.position == -1:
                    self.create_order(self.units * 2)
                self.position = 1
            elif resam['position'].iloc[-2] == -1:
                if self.position == 0:
                    self.create_order(-self.units)
                elif self.position == 1:
                    self.create_order(-self.units * 2)
                self.position = -1

if __name__ == '__main__':
    strat = 2
    if strat == 1:
        mom = MomentumTrader(3, 'DE30_EUR', 1)
        mom.start(stop=100)
    elif strat == 2:
        mom = MomentumTrader(momentum=6, instrument='EUR_USD', units=100000)
        mom.start(stop=100)
    else:
        print('Strategy not known.')