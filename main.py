import boto3
import uuid
from flask import Flask, request

from exchangesDB import ExchangesDB
from ingesterJobsQueue import IngestersJobsQueue

sqs = boto3.client('sqs')

ingestersJobsQueue = IngestersJobsQueue(sqs)

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/startNewIngester', methods=['POST'])
def startNewIngester():

    ingesterId = str(uuid.uuid4())
    job = {
        'ingesterId': ingesterId,
        'exchange': 'binance',
        'coin': 'btcusdt',
        'streams': [{'streamType': 'trade'}, {'streamType': 'depth', 'U': 100},
        {'streamType': 'kline', 'I': '1m'}],
        'settings': 'default-ingester',
    }

    ingestersJobsQueue.pushIngesterJob(job)
    return {}


if __name__ == '__main__':


    from utilities.config import Config
    C = Config.getInstance()

    app.run()