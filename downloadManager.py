from twisted.internet import reactor

from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

import boto3
from data.syncJobsQueue import SyncJobsQueue
sqs = boto3.client('sqs')

L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()



class DownloadManager(object):

    def __init__(self, ingesterBotsDB, trainingDataBucket):

        self.ingesterBotsDB = ingesterBotsDB
        self.trainingDataBucket = trainingDataBucket

        self.activeIngesters = dict()
        self.activeIngestersQueues = dict()

    def run(self):

        reactor.callLater(2, self.downloadFromSyncQueue)
        reactor.callLater(int(
            C.items['downloadManager']['activeIngesters']['pollDelay']),
            self.updateActiveIngesters)

    def updateActiveIngesters(self):
        activeIngesters = self.ingesterBotsDB.getActiveIngesters()
        for activeIngester in activeIngesters:
            if activeIngester['ingesterId'] not in activeIngesters:
                self.activeIngesters[activeIngester['ingesterId']
                    ] = activeIngester
                self.activeIngestersQueues[activeIngester['ingesterId']] = SyncJobsQueue(
                    sqs, activeIngester['syncQueueUrl'])

        reactor.callLater(int(
            C.items['downloadManager']['activeIngesters']['pollDelay']),
            self.updateActiveIngesters)

    def downloadFromSyncQueue(self):

        for ingester in self.activeIngesters:
            jobs, receipts = self.activeIngestersQueues[ingester].getJobs()

            if jobs != None:
                for job in jobs:
                    self.trainingDataBucket.downloadFile(job['bucketName'], job['key'],
                        '{0}/{1}'.format(
                        C.items['downloadManager']['trainingDataLocalDirectory'],
                            job['key']))
            else:
                pass

        reactor.callLater(2, self.downloadFromSyncQueue)