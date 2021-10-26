from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()


class IngesterBotsDB(object):

    def __init__(self, dynamodb):

        self.table = dynamodb.Table(C.items['ingesterBotsDB']['name'])

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def createRecord(self, record):

        response = self.table.put_item(Item=record)
        return response

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def updateStatus(self, status):

        response = self.table.update_item(
            Key={'ingesterId': C.items['ingesterId']},
            UpdateExpression='SET #st = :newStatus',
            ExpressionAttributeNames={'#st': 'status'},
            ExpressionAttributeValues={':newStatus': status}
        )
        return response

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def updateSequence(self, sequence):

        response = self.table.update_item(
            Key={'ingesterId': C.items['ingesterId']},
            UpdateExpression='SET #st = :newSequence',

            ExpressionAttributeNames={'#st': 'sequence'},
            ExpressionAttributeValues={':newSequence': sequence}
        )
        return response