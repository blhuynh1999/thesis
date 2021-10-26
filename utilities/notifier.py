import sys
import json
import functools
import traceback
from utilities.log import Logger
from utilities.config import Config

L = Logger()
C = Config.getInstance()


class Notifier(object):

    __instance = None

    def __init__(self, email):

        if Notifier.__instance != None:
            raise Exception(
                'Cannot re-initialize Notifier, it\'s a singleton class')

        Notifier.__instance = self
        self.email = email

    @staticmethod
    def getInstance():

        if Notifier.__instance == None:
            Notifier()

        return Notifier.__instance

    def notifyIfException(self, function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            try:
                value = function(*args, **kwargs)

            except Exception as e:
                L.critical('{0} crashed'.format(function.__name__))
                L.critical(traceback.format_exc())
                self.email.send(
                    'Aithina-Mycroft-Ingester: [CRITICAL] - {0} crashed unexpectedly'.format(
                        function.__name__),
                        traceback.format_exc())

                sys.exit(1)

            else:
                return value
        return wrapper

    def notifyAndExitIfException(self, function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            try:
                value = function(*args, **kwargs)

            except Exception as e:
                L.critical('{0} crashed'.format(function.__name__))
                L.critical(traceback.format_exc())
                self.email.send(
                    'Aithina-Mycroft-Ingester: [CRITICAL] - {0} crashed unexpectedly'.format(
                        function.__name__),
                        traceback.format_exc())

            else:
                return value
        return wrapper

    def sendWebsocketCrashed(self, message):

        # ToDo send log as attachment
        self.email.send(
            'Aithina-Mycroft-Ingester: [CRITICAL] - websocket crashed unexpectedly',
            json.dumps(message)
        )
