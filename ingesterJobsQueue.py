import json

from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()

class IngestersJobsQueue(object):

    def __init__(self, sqs):

        self.sqs = sqs

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def getJob(self):

        response = self.sqs.receive_message(
            QueueUrl = C.items['ingesterJobsQueue']['url'],
            MaxNumberOfMessages = 1
        )

        if 'Messages' in response:
            message = response['Messages'][0]
            job = json.loads(message['Body'])
            reciept = message['ReceiptHandle']
            return job, reciept

        else:
            return None, None

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def acceptJob(self, reciept):

        response = self.sqs.delete_message(
            QueueUrl = C.items['ingesterJobsQueue']['url'],
            ReceiptHandle = reciept
        )

        return response

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def declineJob(self, reciept):

        response = self.sqs.change_message_visibility(
            QueueUrl = C.items['ingesterJobsQueue']['url'],
            ReceiptHandle = reciept,
            VisibilityTimeout = 0
        )

        return response