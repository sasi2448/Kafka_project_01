from faker import Faker
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from confluent_kafka import Producer
import boto3
from utils import get_secret


# Importing necessary libraries
Faker = Faker()
logger = logging.getLogger(__name__)


def generate_log():
    """ Generate synthetic log entries """
    methods =  ['GET', 'POST', 'PUT', 'DELETE']
    endpoints = ['/api/v1/resource', '/api/v2/resource', '/api/v3/resource']
    status_codes = [200, 201, 400, 404, 500]
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15',
        'Mozilla/5.0 (Linux; Android 10; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Mobile Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
    ]
    referrers = [
        'https://www.google.com/',
        'https://www.example.com/',
        'https://www.airflow.apache.org/',
        'https://www.github.com/'
    ]
    ip = Faker.ipv4()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    method = Faker.random.choice(methods)
    endpoint = Faker.random.choice(endpoints)
    status_code = Faker.random.choice(status_codes)
    user_agent = Faker.random.choice(user_agents)
    referrer = Faker.random.choice(referrers)
    response_time_ms = Faker.random.randint(50, 5000)
    
    log_entry = (
        f"{timestamp} - {ip} - {method} {endpoint} - {status_code} - "
        f"User-Agent: {user_agent} - Referrer: {referrer} - "
        f"Response Time: {response_time_ms}ms"
    )
    return log_entry




def create_kafka_producer(config):
    return Producer(config)


def delivery_report(err, msg):
    """ Callback function to report delivery status """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def produce_logs():
    """ produce log entries into kafka """
    secrets = get_secret('mwaa_secret_v2')
    kafka_config = {
        'bootstrap.servers':secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'session.timeout.ms': 60000
    }
    producer = create_kafka_producer(kafka_config)
    topic = 'logs_topic'
    for _ in range(15000):
        log = generate_log()
        try:
            producer.produce(topic, value=log, on_delivery = delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f"Error producing log: {e}")
    logger.info(f"Produced 15000 log entries to topic {topic}")



defalut_args = {
    'owner': 'AWS Data Engineer Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': '2023-10-01',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id = 'log_generation_pipeline',
    default_args = defalut_args,
    description = "Generate and produce logs",
    schedule_interval= '*/5 * * * *',
    start_date= datetime(year=2025, month=5, day=24),
    catchup=False,
    tags=['logs', 'kafka', 'production']
)


produce_logs_task = PythonOperator(
    task_id = 'generate_and_produce_logs',
    python_callable= produce_logs,
    dag = dag
)


