import os

from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()



class Startup(object):

    @staticmethod
    @Logger.logTrace(L)
    @N.notifyIfException
    def setup():

        if not os.path.exists(C.items['batchManager']['trainingDataLocalDirectory']):
            os.makedirs(C.items['batchManager']['trainingDataLocalDirectory'])

        return True