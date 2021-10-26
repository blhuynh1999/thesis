import logging
import functools

from utilities.config import Config

C = Config.getInstance()


class Logger:

    __logger = None

    def __new__(self):
        if self.__logger is None:
            self.__logger = logging.getLogger(C.items['log']['name'])

        return self.__logger

    @staticmethod
    def logTrace(logger):
        def log(function):
            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                # ToDo: add filename too with function name
                logger.debug('Executing {0}'.format(function.__name__))
                value = function(*args, **kwargs)
                logger.debug('Executed {0}'.format(function.__name__))

                # ToDo: check what is best to print? repr, str, or object address
                logger.debug('Returned {0}'.format(repr(value)))
                return value
            return wrapper
        return log

    @staticmethod
    def setup():

        logC = C.items['log']
        logger = logging.getLogger(logC['name'])
        logger.setLevel(Logger.getLogLevel(logC['logLevel']))
        formatter = logging.Formatter(
            '%(asctime)s: [%(name)s]-[%(levelname)s]: %(message)s')

        logFilename = '{0}/{1}'.format(logC['logDirectory'],
            C.items['startTs'])
        handler = logging.FileHandler(filename=logFilename, encoding='utf-8')
        handler.setLevel(Logger.getLogLevel(logC['logLevel']))
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        handler = logging.StreamHandler()
        handler.setLevel(Logger.getLogLevel(logC['logLevel']))
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    @staticmethod

    def getLogLevel(logLevel: str) -> int:

        logLevelMap = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }

        if logLevel in logLevelMap:
            return logLevelMap.get(logLevel)

        else:
            raise Exception('Unknown log level: {0}, allowed log levels: {1}'.format(
                logLevel, logLevelMap.keys()))
