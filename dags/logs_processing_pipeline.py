from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
import logging
from utils import get_secret
import re
logger = logging.getLogger(__name__)



def parse_log_entry(log_entry):
    logger.debug("Parsing log entry")
    log_patterrn = r'(?P<timestamp>[\d-]+\s[\d:]+) - (?P<ip>[\d.]+) - (?P<method>\w+) (?P<endpoint>[\w/]+) - (?P<status_code>\d+) - User-Agent: (?P<user_agent>.+?) - Referrer: (?P<referrer>.+?) - Response Time: (?P<response_time_ms>\d+)ms'
    match = re.match(log_patterrn, log_entry)
    if not match:
        logger.warning(f'Invalid log format: {log_entry}')
        return None
    
    data = match.groupdict()

    try:
        data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
        data['response_time_ms'] = int(data['response_time_ms'])
    except ValueError as e:
        logger.error(f'Error parsing log entry: {log_entry}, Error: {e}')
        return None 
    
    return data


def create_es_index(es, index_name):
    logger.info(f"Checking/creating Elasticsearch index: {index_name}")
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f'Created index: {index_name}')
    except Exception as e:
        logger.error(f'Failed to create index: {index_name}, Error: {e}')


def poll_and_parse_logs(consumer, index_name, batch_size=15000):
    logger.info(f"Polling and parsing logs from Kafka topic: {index_name}")
    logs = []
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaException._PARTITON_EOF:
                break
            raise KafkaException(msg.error())
        log_entry = msg.value().decode('utf-8')
        parsed_log = parse_log_entry(log_entry)
        if parsed_log:
            logs.append(parsed_log)
        if len(logs) >= batch_size:
            yield logs
            logs = []
    if logs:
        yield logs


def bulk_index_logs(es, index_name, logs):
    logger.info(f"Bulk indexing {len(logs)} logs into Elasticsearch index: {index_name}")
    actions = [
        {
            '_op_type': 'create',
            '_index': index_name,
            '_source': log
        }
        for log in logs
    ]
    try:
        success, failed = es.bulk(index=index_name, body=actions, refresh=True)
        logger.info(f'Successfully indexed {success} logs, Failed: {failed}')
    except Exception as e:
        logger.error(f'Error indexing logs: {e}')


def consume_index_log():
    logger.info("Starting consume_index_log process")
    secrets = get_secret('mwaa_secret_v2')
    consumer_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'session.timeout.ms': 60000,
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'latest'
    }
    es_config = {
        'hosts': [secrets['ELASTICSEARCH_URL']],
        'api_key': secrets['ELASTICSEARCH_API_KEY']
    }

    consumer = Consumer(consumer_config)
    es = Elasticsearch(**es_config)
    topic = 'logs_topic'
    index_name = 'logs_topic'
    consumer.subscribe([topic])

    create_es_index(es, index_name)

    try:
        for logs_batch in poll_and_parse_logs(consumer, index_name):
            bulk_index_logs(es, index_name, logs_batch)
    finally:
        consumer.close()
        logger.info('Consumer closed successfully')







defalut_args = {
    'owner': 'AWS Data Engineer Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': '2023-10-01',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id = 'log_processing_pipeline',
    default_args = defalut_args,
    description = "Generate and produce logs",
    schedule_interval= '*/5 * * * *',
    start_date= datetime(year=2025, month=5, day=24),
    catchup=False,
    tags=['logs', 'kafka', 'production']
)


consum_logs_task = PythonOperator(
    task_id = 'consume_logs',
    python_callable= consume_index_log,
    dag = dag
)


