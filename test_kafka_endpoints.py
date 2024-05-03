#!/usr/bin/env python3

import sys
import os
import socket
import argparse
import logging
import traceback
from kafka import KafkaProducer
from kafka.errors import KafkaError

DEFAULT_LEVEL = 5
logging.addLevelName(DEFAULT_LEVEL, "DEFAULT")

class CustomLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        handler = logging.StreamHandler()
        handler.setFormatter(CustomFormatter())
        self.addHandler(handler)
        self.setLevel(DEFAULT_LEVEL)

    def default(self, message, *args, **kwargs):
        self._log(DEFAULT_LEVEL, message, args, **kwargs)

class CustomFormatter(logging.Formatter):
    def __init__(self, fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)

    def format(self, record):
        if record.levelno == DEFAULT_LEVEL:
            self._style._fmt = '%(asctime)s - %(message)s'
        else:
            self._style._fmt = '%(asctime)s - %(levelname)s - %(message)s'

        return super().format(record)

def setup_logger(verbosity):
    logging.setLoggerClass(CustomLogger)

    logger = logging.getLogger(__name__)
    level_limits = [
        [DEFAULT_LEVEL, logging.CRITICAL],  # verbosity == 0
        [DEFAULT_LEVEL, logging.CRITICAL, logging.ERROR],  # verbosity == 1
        [DEFAULT_LEVEL, logging.CRITICAL, logging.ERROR, logging.WARNING],  # verbosity == 2
        [DEFAULT_LEVEL, logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO],  # verbosity == 3
        [DEFAULT_LEVEL, logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]  # verbosity == 4
    ]
    active_levels = level_limits[min(verbosity, len(level_limits) - 1)]

    logger.handlers[0].addFilter(lambda record: record.levelno in active_levels)
    return logger

def main():
    parser = argparse.ArgumentParser(description='Test connection to one or more Kafka brokers.')
    parser.add_argument('--broker', action='append', dest='broker_list', required=True, help='Kafka broker address, and optionally :{PORT}.  Can be specified multiple times.')
    parser.add_argument('--port', type=int, default=9092, dest='default_port', help='Set default port for all brokers defined')
    parser.add_argument('--topic', required=True, help='Destination Kafka topic')
    parser.add_argument('--file', type=str, help='File containing the message to send')
    parser.add_argument('--auth', action='store_true', help='Use authentication')
    parser.add_argument('--protocol', choices=['SASL_PLAINTEXT', 'SASL_SSL'], default='SASL_PLAINTEXT')
    parser.add_argument('--mechanism', choices=['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'], default='PLAIN')
    parser.add_argument('--username', help='SASL username')
    parser.add_argument('--password', help='SASL password')
    parser.add_argument('-v', '--verbose', action='count', default=0, dest='verbosity', help='Can be supplied multiple times to increase verbosity')

    args = parser.parse_args()
    logger = setup_logger(args.verbosity)
    kafka_logger = logging.getLogger('kafka')
    kafka_logger.handlers = logger.handlers
    kafka_logger.setLevel(logger.level)

    if args.auth:
        missing_args = []
        if not args.username:
            missing_args.append("--username")
        if not args.password:
            missing_args.append("--password")
        
        if missing_args:
            parser.error(f"The following arguments are required with --auth: {' '.join(missing_args)}")

    message = None
    if args.file:
        if os.path.isfile(args.file) and os.access(args.file, os.R_OK):
            with open(args.file, 'r') as file:
                message = file.read().encode('utf-8')
        else:
            logger.critical(f"Error: The file {args.file} does not exist or is not readable.")
            sys.exit(255)
    else:
        if not sys.stdin.isatty():
            message = sys.stdin.read().encode('utf-8')
        else:
            logger.critical("Error: No input provided via stdin or --file. Please provide a message to send.")
            sys.exit(255)

    if message is None:
        logger.error("Error: No message provided to send to the Kafka broker.")
        sys.exit(255)

    args.broker_list = list(set(args.broker_list))
  
    failures = 0
    for current_broker in args.broker_list:
        broker = current_broker if ':' in current_broker else f"{current_broker}:{args.default_port}"
        producer_config = {
            'bootstrap_servers': [broker],
            'retries': 1,
            'retry_backoff_ms': 1000,
            'max_block_ms': 5000,
        }

        if args.auth and args.username and args.password:
            producer_config.update({
                'security_protocol': args.protocol,
                'sasl_mechanism': args.mechanism,
                'sasl_plain_username': args.username,
                'sasl_plain_password': args.password,
            })

        try:
            producer = KafkaProducer(**producer_config)
            future = producer.send(args.topic, value=message)
            result = future.get(timeout=10)
            logger.default(f"{broker}: Success")
        except KafkaError as e:
            logger.critical(f"{broker}: Kafka related failure - {e}")
            failures += 1
        except Exception as e:
            logger.debug(traceback.format_exc())
            logger.critical(f"{broker}: Unexpected network related failure - {e}")
        finally:
            if 'producer' in locals():
                producer.close()
    sys.exit(failures)

if __name__ == '__main__':
    main()

