from utilities.log import Logger
from utilities.config import Config

L = Logger()
C = Config.getInstance()


class NotificationEmail(object):

    def __init__(self, ses):

        self.ses = ses

    @Logger.logTrace(L)
    def send(self, subject, body):

        response = self.ses.send_email(
            Destination={
                'ToAddresses': C.items['notifications']['email']['to']},
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': body,
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': subject,
                },
            },
            Source=C.items['notifications']['email']['from']
        )
        return response