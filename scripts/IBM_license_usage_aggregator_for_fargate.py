#
# Copyright 2022 IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#

import csv
import datetime
import math
import os
import sys


DEBUG = False

def _log(message):
    date_time_obj = datetime.datetime.now()
    timestamp_str = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    return print(f'{timestamp_str} - {message}')


def _debug(message):
    if DEBUG:
        _log(str(f'DEBUG: {message}'))


def _info(message):
    _log(str(f'INFO: {message}'))


def _read_storage(s3_license_usage_directory):
    output_csv_rows = []

    days = sorted(os.listdir(s3_license_usage_directory))
    start_date = days[0]
    end_date = days[-1]

    for day in os.listdir(s3_license_usage_directory):
        _info(f'Aggregation started for day - {day}')

        for product in os.listdir(os.path.join(s3_license_usage_directory, day)):
            _info(f'Aggregation started for product - {product}')
            values = {}

            for task in os.listdir(os.path.join(s3_license_usage_directory, day, product)):
                _debug(f'Aggregation started for task - {task}')
                _read_task(
                    day, product, s3_license_usage_directory, task, values)
                _debug(f'Aggregation finished for task - {task}')

            _debug(f'Aggregation finished for product - {product}')

            if values:
                for prod in values:
                    daily_hwm = int(math.ceil(max(values[prod].values())))
                    _debug(f'HWM calculated = {prod} - {daily_hwm}')
                    csv_row = {"date": day, "name": prod[0], "id": prod[1],
                               "metricName": prod[2], "metricQuantity": daily_hwm, "clusterId": prod[3]}
                    output_csv_rows.append(csv_row)

        _debug(f'Aggregation finished for day - {day}')

    return [output_csv_rows, start_date, end_date]


def _read_task(day, product, s3_license_usage_directory, task, values):
    task_path = os.path.join(s3_license_usage_directory, day, product, task)
    _debug(f'Reading file - {task_path}')
    csvreader = csv.DictReader(open(task_path))

    for row in csvreader:

        if not _validate(row, product):
            break

        product_unique_id = tuple(
            [row['ProductName'], row['ProductId'], row['Metric'], row['ClusterId']])

        if product_unique_id not in values:
            values[product_unique_id] = {}

        if row['Timestamp'] in values[product_unique_id]:
            values[product_unique_id][row['Timestamp']] += float(row['vCPU'])
        else:
            values[product_unique_id][row['Timestamp']] = float(row['vCPU'])


def _prepare_daily_hwm_files(csv_rows):
    output_csv_rows = sorted(csv_rows[0], key=lambda x: (
        x["name"], x["metricName"], x["date"]))

    csv_files = {}
    for row in output_csv_rows:
        file_name = '_'.join(('products_daily', csv_rows[1], csv_rows[2], row["clusterId"].replace(
            ':', '_').replace('/', '_')))
        _debug(f'Preparing content for filename = {file_name}')
        if file_name not in csv_files:
            csv_files[file_name] = []
        if row["metricName"] == 'PROCESSOR_VALUE_UNIT':
            row["metricQuantity"] *= 70

        csv_files[file_name].append(row)

    return csv_files


def _export_daily_hwm_files(csv_files, output_directory):
    header = ['date', 'name', 'id', 'metricName',
              'metricQuantity', 'clusterId']

    for file_name in csv_files:
        with open(f'{output_directory}{os.sep}{file_name}.csv', 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(csv_files[file_name])

    return None


def _validate(row, product):
    if row['ProductId'] not in product:
        _debug(f'Wrong ProductId - skipping {row}')
        return False

    try:
        # test
        float(row['vCPU'])
    except TypeError:
        _debug(f'Wrong vCPU value - skipping {row}')
        return False

    return True


def main(argv):
    _info('License Service Aggregator started')

    s3_license_usage_directory = './test_files/input'
    output_directory = './test_files/output'

    if not argv:
        _info('Usage:')
        _info('IBM_license_usage_aggregator_for_fargate.py <s3_license_usage_directory> <output_directory>')
        sys.exit()

    if argv[0]:
        s3_license_usage_directory = argv[0]

    if argv[1]:
        output_directory = argv[1]

    _info(f's3_license_usage_directory = {s3_license_usage_directory}')

    if os.path.exists(s3_license_usage_directory) and os.path.isdir(s3_license_usage_directory):
        if not os.listdir(s3_license_usage_directory):
            _info('s3_license_usage_directory is empty')
            sys.exit()
    else:
        _info('Given s3_license_usage_directory not exists')
        sys.exit()

    _info(f'Output directory = {output_directory}')

    if os.path.exists(output_directory) and os.path.isdir(output_directory):
        if os.listdir(output_directory):
            _info('Output_directory should be empty')
            sys.exit()
    else:
        _info('Given output_directory not exists')
        sys.exit()

    _info('License Service Aggregator - reading s3_license_usage_directory started')
    csvs = _read_storage(s3_license_usage_directory)
    _info('License Service Aggregator - reading s3_license_usage_directory finished')

    _info('License Service Aggregator - preparing daily HWMs started')
    daily_hwms = _prepare_daily_hwm_files(csvs)
    _info('License Service Aggregator - preparing daily HWMs finished')

    _info('License Service Aggregator - saving output started')
    _export_daily_hwm_files(daily_hwms, output_directory)
    _info('License Service Aggregator - saving output finished')

    _info('License Service Aggregator finished')


if __name__ == '__main__':
    main(sys.argv[1:])
