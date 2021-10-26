import json

from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()



class SyncJobsQueue(object):

    def __init__(self, sqs):

        self.sqs = sqs

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def createQueue(self):

        queueName = 'i-{0}-{1}-{2}.fifo'.format(C.items['exchange'].lower(
        ), C.items['coin'].lower(), C.items['ingesterId'].lower())
        response = self.sqs.create_queue(
            QueueName=queueName,
            Attributes=C.items['syncJobQueueSettings']
        )

        C.items['syncJobQueueSettings']['name'] = queueName
        C.items['syncJobQueueSettings']['url'] = response['QueueUrl']
        return response

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def pushSyncJob(self, job):

        response = self.sqs.send_message(
            QueueUrl=C.items['syncJobQueueSettings']['url'],
            MessageBody=json.dumps(job),

            MessageGroupId='ingester'
        )

        return response