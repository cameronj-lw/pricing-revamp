
import logging
import os

from flask import Flask
from flask_cors import CORS
from flask_restx import Api, Resource

# globals
app = Flask(__name__)
api = Api(app)
CORS(app)


@api.route('/api/run-cmd')
class RunCmd(Resource):
    def post(self):
        payload = api.payload
        cmd = payload['cmd']
        logging.info(f'Running cmd: {cmd}')
        os.system(cmd)
        return


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)



