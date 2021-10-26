import json


class Binance(object):
    @staticmethod
    def coinToRestUrl(baseUrl, coin, limit):
        restUrlStr = '{0}?symbol={1}&limit={2}'.format(
            baseUrl, coin.upper(), limit)
        return restUrlStr.encode('UTF-8')

    @staticmethod
    def streamsToWebsocketUrl(exchangeInfo, streams, coin):
        websocketUrl = exchangeInfo['websocketUrl']
        for stream in streams:
            streamInfo = exchangeInfo['streams'][stream['streamType']]
            streamUrl = streamInfo['format']
            if 'U' in stream:
                streamUrl = streamUrl.replace('[U]', str(stream['U']))
            if 'I' in stream:
                streamUrl = streamUrl.replace('[I]', str(stream['I']))
            streamUrl = streamUrl.replace('[C]', coin)
            websocketUrl += streamUrl + '/'
        return websocketUrl[:-1]

    @staticmethod
    def parseRestMessage(message):
        message = json.loads(message.decode('UTF-8'))
        messageCsv, messageEs = Binance.orderbookToCsv(message)
        return messageCsv, messageEs, 'depthSnapshot'

    @staticmethod
    def parseStreamMessage(message):
        message = json.loads(message['payload'].decode('utf8'))
        streamType = Binance.parseStreamType(message['stream'])

        if streamType == 'trade':
            messageCsv, messageTs = Binance.tradesToCsv(message['data'])
        elif streamType == 'kline':
            messageCsv, messageTs = Binance.klineToCsv(message['data'])
        elif streamType == 'depth':
            messageCsv, messageTs = Binance.diffToCsv(message['data'])
        else:
            raise Exception('Unknown stream type: {0}, data: {1}'.format(
                streamType, message['data']))
        return messageCsv, messageTs, streamType

    @staticmethod
    def parseStreamType(stream):
        streamType = stream.split('@')[1]
        streamType = streamType.split('_')[0] # for 'kline_1m' case
        return streamType

    @staticmethod
    def tradesToCsv(trade):
        tradeCsv = '{0};{1};{2};{3};{4};{5};{6};{7};{8}\n'.format(
            trade['E'],
            trade['t'],
            trade['p'],
            trade['q'],
            trade['b'],
            trade['a'],
            trade['T'],
            trade['m'],
            trade['M']
        )
        return tradeCsv, trade['E']

    @staticmethod
    def klineToCsv(kline):
        klineCsv = '{0};{1};{2};{3};{4};{5};{6};{7};{8};{9};{10};{11};{12};{13};{14};{15};{16}\n'.format(
            kline['E'],
            kline['k']['t'],
            kline['k']['T'],
            kline['k']['i'],
            kline['k']['f'],
            kline['k']['L'],
            kline['k']['o'],
            kline['k']['c'],
            kline['k']['h'],
            kline['k']['l'],
            kline['k']['v'],
            kline['k']['n'],
            kline['k']['x'],
            kline['k']['q'],
            kline['k']['V'],
            kline['k']['Q'],
            kline['k']['B'],
        )
        return klineCsv, kline['E']


    @staticmethod
    def diffToCsv(diff):
        diffCsv = '{0};{1};{2};{3};{4}\n'.format(
            diff['E'],
            diff['U'],
            diff['u'],
            str(diff['b']),
            str(diff['a'])
        )
        return diffCsv, diff['E']

    @staticmethod
    def orderbookToCsv(orderbook):
        orderbookCsv = '{0};{1};{2}\n'.format(
            orderbook['lastUpdateId'],
            str(orderbook['bids']),
            str(orderbook['asks'])
        )
        return orderbookCsv, orderbook['lastUpdateId']
