# By Ran Tzur for OwnCompany
import argparse
import time
from datetime import datetime
from typing import List, Tuple

import psutil
from opensearchpy import OpenSearch
from requests import HTTPError


class OpenShiftConnectionCleaner:

    def __init__(self, es_hosts: List[str], auth: Tuple[str, str]):
        print('Opening OpenShift connection')
        opensearch = OpenSearch(
            hosts=es_hosts,
            http_auth=auth,
            verify_certs=False,
            ssl_show_warn=False,
            ssl_assert_hostname=False,
        )
        self.client = opensearch

    def __enter__(self):
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('Closing OpenShift connection')
        self.client.close()


def collect_data() -> dict:
    cpu_percent = psutil.cpu_percent(interval=1)
    memory_percent = psutil.virtual_memory().percent
    disk_percent = psutil.disk_usage('/').percent
    network_info = psutil.net_io_counters()
    network_sent = network_info.bytes_sent
    network_recv = network_info.bytes_recv
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    data = {
        'timestamp': timestamp,
        'cpu_percent': cpu_percent,
        'memory_percent': memory_percent,
        'network_sent': network_sent,
        'network_recv': network_recv,
        'disk_percent': disk_percent
    }

    print(f'Collected data: {data}')
    return data


def send_data(es_client: 'OpenSearch', data: dict, index: str):
    try:
        if not es_client.indices.exists(index=index):
            print(f'Creating index: {index}')
            es_client.indices.create(index=index)
        print(f'Sending data to index: {index}')
        es_client.index(index=index, body=data)
        print('Finished sending data chunk')
    except HTTPError as e:
        print(f'Failed sending data: {data} to index: {index}\nError: {e}')


def collect_and_report_to_es(es_hosts: List[str],
                             index: str,
                             auth: Tuple[str, str],
                             sleep_interval_in_seconds: int = 30):
    with OpenShiftConnectionCleaner(es_hosts, auth) as es_client:
        while True:
            try:
                data = collect_data()
                send_data(es_client=es_client, data=data, index=index)
                time.sleep(sleep_interval_in_seconds)
            except KeyboardInterrupt:
                print('Interrupted by keyboard')
                break


def collect_args():
    parser = argparse.ArgumentParser(description='Collect and report performance data to OpenSearch, task by OwnCompany by Ran Tzur')
    parser.add_argument('--hosts', required=True, nargs='+', help='OpenSearch hosts URL (for example: http://localhost:9200)')
    parser.add_argument('--interval', type=int, default=30, help='Data collection interval in seconds (default: 30)')
    parser.add_argument('--index', required=False, default='performance_task_own_company', help='OpenSearch index name (DB Name)')
    parser.add_argument("--credentials", metavar=("username", "password"),
                        nargs=2, type=str, help="Username and password if needed",
                        required=False, default=('admin', 'admin'))
    return parser.parse_args()


if __name__ == '__main__':
    args = collect_args()
    print(f'Started running program to collect data with args: {args}')
    collect_and_report_to_es(sleep_interval_in_seconds=args.interval,
                             es_hosts=args.hosts,
                             auth=args.credentials,
                             index=args.index)
