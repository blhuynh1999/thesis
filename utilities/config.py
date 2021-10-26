import copy
import json


class Config(object):

    __instance = None

    def __init__(self):

        if Config.__instance != None:
            raise Exception(
                'Cannot re-initialize Config, it\'s a singleton class')

        Config.__instance = self
        self.items = Config.loadFromJSON('./config.json')

    @staticmethod
    def loadFromJSON(path):

        config = {}
        with open(path, 'r') as configFile:
            config = json.load(configFile)


        return config

    @staticmethod
    def getInstance():

        if Config.__instance == None:
            Config()

        return Config.__instance
