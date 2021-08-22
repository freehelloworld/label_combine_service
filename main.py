import os
from flask import Flask, abort
from combiner import Combiner
from storage_helper import StorageHelper
import pandas as pd
from datetime import datetime

app = Flask(__name__)

source_path = os.getenv("SOURCE_PATH") if os.getenv("SOURCE_PATH") else '/Users/whoever/Downloads/New_Data/'
output_path = os.getenv("OUTPUT_PATH") if os.getenv("OUTPUT_PATH") else '/Users/whoever/Downloads/New_Data/'


@app.route('/combine/<string:loc_id>', methods=["GET"])
def combine_by_id(loc_id):
    helper = StorageHelper(source_path, output_path)
    combiner = Combiner(helper, loc_id)
    return combiner.combine(), 200


@app.route('/combine/all', methods=["GET"])
def combine_all():
    manifest = os.path.join(source_path, 'attribute_manifest.csv')
    df = pd.read_csv(manifest)
    loc_ids = list(set(df['loc_id'].values))
    helper = StorageHelper(source_path, output_path)

    for loc_id in loc_ids:
        print(loc_id)
        combiner = Combiner(helper, str(loc_id))
        combiner.combine()
    return 'Done', 200


@app.route('/combine/new/<string:action_date>', methods=["GET"])
def combine_new(action_date):
    try:
        datetime.strptime(action_date, '%Y-%m-%d')
    except ValueError:
        return abort(400,
                     description="Invalid format, please provide date in format of %Y-%m-%d.")

    manifest = os.path.join(source_path, 'attribute_manifest.csv')
    df = pd.read_csv(manifest)
    df = df[df['date'] >= action_date]
    loc_ids = list(set(df['loc_id'].values))
    cnt = 0
    helper = StorageHelper(source_path, output_path)
    for loc_id in loc_ids:
        print(cnt, loc_id)
        combiner = Combiner(helper, str(loc_id))
        combiner.combine()
        cnt += 1
    return 'Done', 200


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)