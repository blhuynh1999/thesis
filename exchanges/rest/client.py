from twisted.internet import reactor, ssl
from twisted.internet.protocol import ReconnectingClientFactory
from autobahn.twisted.websocket import WebSocketClientProtocol, WebSocketClientFactory, connectWS


class ExchangeClientProtocol(WebSocketClientProtocol):

    def __init__(self):
        super(WebSocketClientProtocol, self).__init__()

    def onConnect(self, response):
        self.factory.resetDelay()

    def onMessage(self, payload, isBinary):

        for call in reactor.getDelayedCalls():
            if call.func.__name__ == 'clientConnectionTimeout':
                call.cancel()

        reactor.callLater(
            int(self.factory.config['timeout']), self.factory.clientConnectionTimeout)
        self.factory.callback({
            'type': 'onMessage',
            'payload': payload,
            'isBinary': isBinary
        })

    def onClose(self, wasClean, code, reason):
        self.factory.callback({
            ' type': 'onClose',
            'code': code,
            'reason': reason,

            'wasClean': wasClean,
        })


class ExchangeClientFactory(WebSocketClientFactory, ReconnectingClientFactory):

    config = None
    protocol = ExchangeClientProtocol

    def clientConnectionFailed(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback({
                'type': 'onFailed',
                'message': 'Connection failed, max retries reached',
                'reason': str(reason)
            })

    def clientConnectionLost(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback({
            'type': 'onLost',
            'message': 'Connection lost, max retries reached',
            'reason': str(reason)
            })

    def clientConnectionTimeout(self):
        self.callback({
            'type': 'onTimeout',
            'message': 'Connection timed out',
        })


    class Client(object):

        def __init__(self, config, callback):

            self.config = config

            self.factory = ExchangeClientFactory(self.config['url'])

            self.factory.initialDelay = int(self.config['retries']['initialDelay'])
            self.factory.maxDelay = int(self.config['retries']['maxDelay'])
            self.factory.maxRetries = int(self.config['retries']['count'])

            self.factory.config = config
            self.factory.callback = callback
            self.factory.protocol = ExchangeClientProtocol
            self.sslContextFactory = ssl.ClientContextFactory()

        def run(self):
            path = connectWS(self.factory, self.sslContextFactory)
            reactor.callLater(
                int(self.config['timeout']), self.factory.clientConnectionTimeout)