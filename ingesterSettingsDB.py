from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()

class IngesterSettingsDB(object):

    def __init__(self, dynamodb):

        self.table = dynamodb.Table(C.items['ingesterSettingsDB']['name'])

    @Logger.logTrace(L)
    @N.notifyAndExitIfException

    def getSettings(self):

        response = self.table.get_item(
            Key = { 'profile': C.items['settings'] },
            ConsistentRead = True,
        )

        return response.get('Item', None)