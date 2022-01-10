import json
import datetime
import google.auth
import logging
from typing import NamedTuple, Optional, Sequence

import apache_beam as beam
from apache_beam import coders
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.sql import SqlTransform
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection

class AddressMeta(NamedTuple):
    name: Optional[str]
    account_type: Optional[str]
    contract_type: Optional[str]
    entity: Optional[str]
    label: Optional[str]
    tags: Sequence[str]
    created_at: Optional[int]

class Transaction(NamedTuple):
    type: str
    hash: str
    transaction_index: int
    from_address: str
    to_address: Optional[str]
    gas: int
    gas_price: float
    block_timestamp: int
    block_hash: str
    block_number: int
    max_fee_per_gas: Optional[float]
    max_priority_fee_per_gas: Optional[float]
    transaction_type: int
    receipt_cumulative_gas_used: Optional[int]
    receipt_gas_used: Optional[int]
    receipt_contract_address: Optional[str]
    receipt_root: Optional[str]
    receipt_status: int
    receipt_effective_gas_price: Optional[float]
    item_id: Optional[str]
    item_timestamp: Optional[str]
    amount: float
    from_address_name: Optional[str]
    from_address_account_type: Optional[str]
    from_address_contract_type: Optional[str]
    from_address_entity: Optional[str]
    from_address_label: Optional[str]
    from_address_tags: Sequence[str]
    from_address_created_at: Optional[int]
    to_address_name: Optional[str]
    to_address_account_type: Optional[str]
    to_address_contract_type: Optional[str]
    to_address_entity: Optional[str]
    to_address_label: Optional[str]
    to_address_tags: Sequence[str]
    to_address_created_at: Optional[int]

def amount_trans(elem, decimal=10**18):
    elem["amount"] = elem["amount"] / decimal
    elem["gas_price"] = elem["gas_price"] / decimal
    elem["max_fee_per_gas"] = elem["max_fee_per_gas"] / decimal if elem["max_fee_per_gas"] else None
    elem["max_priority_fee_per_gas"] = elem["max_priority_fee_per_gas"] / decimal if elem["max_priority_fee_per_gas"] else None
    elem["receipt_effective_gas_price"] = elem["receipt_effective_gas_price"] / decimal if elem["receipt_effective_gas_price"] else None
    # elem["block_timestamp"] = datetime.datetime.fromtimestamp(elem["block_timestamp"]).strftime("%Y-%m-%d, %H:%M:%S")
    return elem

def flatten(elem):
    def add_prefix(data, prefix):
        return {f"{prefix}{k}": v for k, v in data.items()}

    from_address_meta = add_prefix(elem["from_address_meta"], "from_address_")
    to_address_meta = add_prefix(elem["to_address_meta"], "to_address_")
    elem.pop("from_address_meta")
    elem.pop("to_address_meta")
    return {**elem, **from_address_meta, **to_address_meta}

def run(subscription, output_topic, sql, decimal, pipeline_args):
    options = pipeline_options.PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    coders.registry.register_coder(AddressMeta, coders.RowCoder)
    coders.registry.register_coder(Transaction, coders.RowCoder)
    with beam.Pipeline(options=options) as p:

        result = (
            p
            | "read from pubsub" >> beam.io.ReadFromPubSub(subscription=subscription)
            | 'transform to json' >> beam.Map(json.loads)
            | 'filter empty elements' >> beam.Filter(lambda x: x.get("hash"))
            | 'convert amount to readable' >> beam.Map(amount_trans, decimal=decimal)
            | 'flatten address tags' >> beam.Map(flatten)
            | 'bind schema' >> beam.Map(lambda x: Transaction(**x)).with_output_types(Transaction)
            | 'run customized sql' >> SqlTransform(sql)
            | 'transform to string' >> beam.Map(lambda x: json.dumps(x._asdict()).encode())
            | 'write to pubsub' >> beam.io.WriteToPubSub(topic=output_topic)
        )

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