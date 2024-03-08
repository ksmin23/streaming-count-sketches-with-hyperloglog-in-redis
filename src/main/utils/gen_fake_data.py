#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import argparse
from datetime import datetime
import json
import random
import time
import traceback

import boto3

from mimesis.locales import Locale
from mimesis.schema import Field, Schema

random.seed(47)


def put_records_to_kinesis(client, options, payload_list):
  MAX_RETRY_COUNT = 3

  if options.dry_run:
    print(json.dumps(payload_list, ensure_ascii=False))
    return

  for _ in range(MAX_RETRY_COUNT):
    try:
      response = client.put_records(Records=payload_list, StreamName=options.stream_name)
      if options.verbose:
        print('[KINESIS]', response, file=sys.stderr)
      break
    except Exception as ex:
      traceback.print_exc()
      time.sleep(random.randint(1, 10))
  else:
    raise RuntimeError('[ERROR] Failed to put_records into stream: {}'.format(options.stream_name))


def main():
  parser = argparse.ArgumentParser()

  parser.add_argument('--region-name', action='store', default='us-east-1',
    help='aws region name (default: us-east-1)')
  parser.add_argument('--service-name', required=True, choices=['kinesis', 'firehose', 'console'])
  parser.add_argument('--stream-name', help='The name of the stream to put the data record into (default: 10)')
  parser.add_argument('--max-count', default=10, type=int, help='The max number of records to put (default: 10)')
  parser.add_argument('--dry-run', action='store_true')
  parser.add_argument('--verbose', action='store_true', help='Show debug logs')

  options = parser.parse_args()

  _CURRENT_YEAR = datetime.now().year
  _USERS = ['user-875', 'user-190', 'user-646', 'user-033', 'user-672']

  _ = Field(locale=Locale.EN)

  _schema = Schema(schema=lambda: {
    "user_id": _("choice", items=_USERS),
    "site_id": _("choice", items=[489, 715, 283]),
    "event": _("choice", items=['view', 'like', 'cart', 'purchase']),
    "sku": _("pin", mask='@@####@@@@'),
    "amount":  _("integer_number", start=1, end=10),
    "event_time": _("formatted_datetime", fmt="%Y-%m-%d %H:%M:%S", start=_CURRENT_YEAR, end=_CURRENT_YEAR)
  })

  client = boto3.client(options.service_name, region_name=options.region_name) if options.service_name != 'console' else None

  cnt = 0
  payload_list = []
  for record in _schema.create(options.max_count):
    cnt += 1
    partition_key = 'part-{:05}'.format(random.randint(1, 1024))
    payload_list.append({'Data': record, 'PartitionKey': partition_key})

    if len(payload_list) % 10 == 0:
      put_records_to_kinesis(client, options, payload_list)
      payload_list = []

    time.sleep(random.choices([0.01, 0.03, 0.05, 0.07, 0.1])[-1])

  if not payload_list:
    put_records_to_kinesis(client, options, payload_list)

  print(f'[INFO] {cnt} records are processed', file=sys.stderr)


if __name__ == '__main__':
  main()
