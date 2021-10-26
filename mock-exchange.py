import sys
import time

from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketServerFactory
from autobahn.twisted.websocket import WebSocketServerProtocol


class MyServerProtocol(WebSocketServerProtocol):

    def onConnect(self, request):
        print("onConnect")
        print(request)

    def onConnecting(self, transport_details):
        print("onConnnecting")
        print(transport_details)

    def onOpen(self):

        print("onOpen")
        self.sendMessage('1'.encode('utf8'), True)

    def onMessage(self, payload, isBinary):
        print("onMessage")
        t = payload.decode('utf8')
        time.sleep(int(t))
        self.sendMessage(str(t).encode('utf8'), False)

    def onPing(self, payload):
        print("onPing")
        print(payload)

    def onPong(self, payload):
        print("onPong")
        print(payload)

    def onClose(self, wasClean, code, reason):
        print("onClose")
        print(wasClean)
        print(code)
        print(reason)


if __name__ == '__main__':

    factory = WebSocketServerFactory()
    factory.protocol = MyServerProtocol


    reactor.listenTCP(9000, factory)
    reactor.run()