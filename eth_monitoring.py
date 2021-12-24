import json
import datetime
import google.auth
import logging

import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.sql import SqlTransform


def amount_trans(elem, decimal=10**18):
    elem["value"] = elem["value"] / decimal
    elem["block_timestamp"] = datetime.datetime.fromtimestamp(elem["block_timestamp"]).strftime("%Y-%m-%d, %H:%M:%S")
    return elem

def run(subscription, output_topic, sql, decimal, pipeline_args):
    options = pipeline_options.PipelineOptions(pipeline_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as p:

        _ = (
            p
            | "read from pubsub" >> beam.io.ReadFromPubSub(subscription=subscription)
            | 'transform to json' >> beam.Map(json.loads)
            | 'convert value to readable' >> beam.Map(amount_trans, decimal=decimal)
            | 'create beam row' >> beam.Map(
                lambda elem: beam.Row(
                    hash=str(elem["hash"]),
                    transaction_index=int(elem["transaction_index"]),
                    from_address=str(elem["from_address"]),
                    to_address=str(elem["from_address"]),
                    amount=float(elem["value"]),
                    receipt_status=int(elem["receipt_status"]),
                    block_ts=str(elem["block_timestamp"]),
                    block_hash=str(elem["block_hash"])))
            | 'filter whale transaction' >> SqlTransform(sql)
            | 'transform to dict' >> beam.Map(lambda row: {
                "ts": row.ts,
                "hash": row.hash,
                "from_address": row.from_address,
                "to_address": row.to_address,
                "amount": row.amount})
            | 'transform to string' >> beam.Map(json.dumps)
            | 'UTF-8 encode' >> beam.Map(lambda s: s.encode("utf-8"))
            | 'write to pubsub' >> beam.io.WriteToPubSub(topic=output_topic))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
      '--subscription',
      dest='subscription',
      required=True,
      help=(
          'Cloud PubSub subscription to read from (e.g. '
          'projects/my-project/topics/my-topic), must be created prior to '
          'running the pipeline.'))

    parser.add_argument(
      '--output_topic',
      dest='output_topic',
      required=True,
      help=(
          'Cloud PubSub topic to write to (e.g. '
          'projects/my-project/topics/my-topic), must be created prior to '
          'running the pipeline.'))

    parser.add_argument(
      '--sql',
      dest='sql',
      required=True,
      help=(
          'Full SQL to filter data, must contains the from clause "from PCOLLECTION", '
          'ex. SELECT from_address, to_address, value / 100 as value from PCOLLECTION WHERE value > 100 '))

    parser.add_argument(
      '--decimal',
      dest='decimal',
      required=True,
      type=int,
      help=('Digits of transaction value'))
    
    
    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.subscription, known_args.output_topic, known_args.sql, known_args.decimal, pipeline_args)