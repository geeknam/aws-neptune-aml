import logging
import os
import json
import requests
from flask import Flask, jsonify


app = Flask(__name__)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

GREMLIN_HTTP = os.getenv('GREMLIN_HTTP', '')


class BaseGremlinQuery(object):

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
            GREMLIN_HTTP,
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
    q = SimpleQuery()
    return jsonify(data=q._request())


if __name__ == '__main__':
    app.run()
