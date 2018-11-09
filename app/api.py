import logging
import os
import json
import requests
from flask import Flask, jsonify, request


app = Flask(__name__)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

GREMLIN_ENDPOINT = os.getenv('GREMLIN_ENDPOINT', '')
NEPTUNE_LOADER_ENDPOINT = os.getenv('NEPTUNE_LOADER_ENDPOINT', '')
S3_LOADER_ROLE = os.getenv('S3_LOADER_ROLE', '')


class BaseGremlinQuery(object):

    def __init__(self, raw_query=None):
        if raw_query:
            self.raw_query = raw_query

    def get_bindings(self):
        return {}

    def _get_query(self):
        return "".join(
            self.raw_query.split()
        )

    def _get_request_payload(self):
        return json.dumps({
            "gremlin": self._get_query(),
            "bindings": self.get_bindings()
        })

    def _get_request_headers(self):
        return {
            "Content-Type": 'application/json',
        }

    def _request(self):
        return requests.post(
            GREMLIN_ENDPOINT,
            data=self._get_request_payload(),
            headers=self._get_request_headers(),
            verify=False
        ).json()

    @property
    def result(self):
        flattened = []
        for element in self._request()['result']['data']:
            logger.info(element)
            flattened.append({
                k: (v[0] if len(v) == 1 else v)
                for (k, v) in element.items()
            })
        return flattened


class SimpleQuery(BaseGremlinQuery):

    raw_query = "g.V().count()"


@app.route('/')
def lambda_handler():
    query = request.get_json()
    if not query:
        q = SimpleQuery()
    else:
        q = SimpleQuery(raw_query=query.get('gremlin', ''))
    return jsonify(data=q._request())


@app.route('/load/<folder>')
def load_data(folder):
    payload = {
        "source": "s3://neptune-aws-aml-data/%s" % folder,
        "format": "csv",
        "iamRoleArn": S3_LOADER_ROLE,
        "region": "us-east-1",
        "failOnError": "false"
    }
    return jsonify(
        response=requests.post(
            NEPTUNE_LOADER_ENDPOINT,
            data=json.dumps(payload),
            headers={
                'Content-Type': 'application/json'
            }
        ).json()
    )


@app.route('/load/status/<load_id>')
def load_status(load_id):
    load_url = '%s/%s' % (NEPTUNE_LOADER_ENDPOINT, load_id)
    return jsonify(
        response=requests.get(load_url).json()
    )


if __name__ == '__main__':
    app.run()
