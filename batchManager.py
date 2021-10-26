from zipfile import ZipFile, ZIP_BZIP2

import os
import json
import queue
from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

from exchanges.parser.binance import Binance
from exchanges.rest.client import Client as RestClient
from exchanges.websocket.client import Client as WebsocketClient


L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()


class BatchManager(object):

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def __init__(self, syncJobsQueue, ingesterBotsDB, trainingDataBucket):

        self.syncJobsQueue = syncJobsQueue
        self.ingesterBotsDB = ingesterBotsDB
        self.trainingDataBucket = trainingDataBucket


        self.uploadQueue = queue.Queue()

        self.batchFile = {}
        self.batchCount = {}
        self.batchStartTs = {}
        self.messagesInBatch = {}

        for stream in C.items['streams']:
            if stream['streamType'] == 'depth':
                self.batchCount['depthSnapshot'] = 0
                self.messagesInBatch['depthSnapshot'] = 0

            self.batchCount[stream['streamType']] = 0
            self.messagesInBatch[stream['streamType']] = 0

        websocketConfig = {}
        websocketConfig['url'] = Binance.streamsToWebsocketUrl(
            C.items['exchangeInfo'], C.items['streams'], C.items['coin'])
        websocketConfig = {**C.items['websocket'], **websocketConfig}
        self.websocketClient = WebsocketClient(
            websocketConfig, self.processStreamMessage)

        restConfig = {}
        restConfig['url'] = Binance.coinToRestUrl(
            C.items['exchangeInfo']['restUrl'], C.items['coin'],
            C.items['batchManager']['orderbookSnapshotDepth'])
        restConfig['snapshotFrequency'] = int(
            C.items['batchManager']['orderbookSnapshotFrequency'])

        self.restClient = RestClient(restConfig, self.processRestMessage)

        self.restClient.run()
        self.websocketClient.run()

    @N.notifyAndExitIfException
    def processRestMessage(self, message):

        messageCsv, messageEs, streamType = Binance.parseRestMessage(message)

        L.info('Stream: {0}, Batch sequence: {1}, Batch ES: {2}'.format(
            streamType, self.batchCount[streamType], messageEs))

        messageFile = '{0}/{1}_{2}_{3}_{4}'.format(
            C.items['batchManager']['trainingDataLocalDirectory'], C.items['exchange'],
            C.items['coin'], streamType, messageEs)
        with open(messageFile, 'w') as f:
            f.write(messageCsv)

        batchZipFile = '{0}.zip'.format(messageFile)
        self.compressAndUploadBatch(streamType, messageFile, batchZipFile)

    @N.notifyAndExitIfException
    def processStreamMessage(self, message):

        if message['type'] in ['onConnecting', 'onConnect', 'onOpen', 'onPing']:
            L.debug(message)


        elif message['type'] in ['onClose', 'onFailed', 'onLost', 'onTimeout']:
            # ToDo: re-think severity
            L.critical(message)
            self.ingesterBotsDB.updateStatus('crashed')
            N.sendWebsocketCrashed(message)

        elif message['type'] == 'onMessage':

            messageCsv, messageTs, streamType = Binance.parseStreamMessage(
                message)

            if self.messagesInBatch[streamType] == 0:
                self.batchStartTs[streamType] = messageTs
                self.batchFile[streamType] = '{0}/{1}_{2}_{3}_{4}'.format(
                    C.items['batchManager']['trainingDataLocalDirectory'],
                    C.items['exchange'], C.items['coin'], streamType, messageTs)

            with open(self.batchFile[streamType], 'a') as f:
                f.write(messageCsv)

            self.messagesInBatch[streamType] = self.messagesInBatch[streamType] + 1
            if self.messagesInBatch[streamType] % (int(C.items['exchangeInfo']['streams'][streamType]['batchSize']) // 10) == 0:
                    L.info('Stream: {0}, Batch sequence: {1}, Batch start TS: {2}, Batch size: {3}'.format(
                        streamType, self.batchCount[streamType], self.batchStartTs[streamType],
                        self.messagesInBatch[streamType]))


            if self.messagesInBatch[streamType] == int(C.items['exchangeInfo']['streams'][streamType]['batchSize']):
                batchZipFile = '{0}_{1}.zip'.format(
                    self.batchFile[streamType], messageTs)
                self.compressAndUploadBatch(
                    streamType, self.batchFile[streamType], batchZipFile)

    @N.notifyAndExitIfException
    def compressAndUploadBatch(self, streamType, batchFile, batchZipFile):

        if self.uploadQueue.qsize() == int(C.items['batchManager']['uploadQueueMaxSize']):
            L.critical('Upload Queue Max Size Reached')
            self.ingesterBotsDB.updateStatus('crashed')
            N.sendWebsocketCrashed('Upload Queue Max Size Reached')

        batchKey = batchZipFile.split('/')[-1]
        with ZipFile(batchZipFile, mode='w', compression=ZIP_BZIP2) as zf:
            zf.write(batchFile)

        os.remove(batchFile)

        self.uploadQueue.put((streamType, batchKey, batchZipFile))

        self.messagesInBatch[streamType] = 0
        self.batchCount[streamType] = self.batchCount[streamType] + 1


        for index in range(int(C.items['batchManager']['uploadQueueStepSize'])):

            if self.uploadQueue.qsize() == 0:
                break

            streamType, batchKey, batchZipFile = self.uploadQueue.get()

            self.trainingDataBucket.uploadFile(batchZipFile, batchKey)

            os.remove(batchZipFile)

            self.syncJobsQueue.pushSyncJob({
                'bucketName': C.items['trainingDataBucket']['name'],
                'streamType': streamType,
                'key': batchKey
            })
            
            self.ingesterBotsDB.updateSequence(json.dumps(self.batchCount))