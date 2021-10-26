import json

from utilities.config import Config

C = Config.getInstance()


class ExchangesDB(object):

    def __init__(self, dynamodb):

        self.table = dynamodb.Table(C.items['ExchangesDB']['name'])

    def getExchange(self, exchange):

        response = self.table.get_item( Key = { 'exchange': exchange }, ConsistentRead =
            True )
        return response.get('Item', None)