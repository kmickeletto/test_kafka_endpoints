#!/usr/bin/env python3

import sys
import os
import socket
import argparse
import logging
import traceback
from kafka import KafkaProducer
from kafka.errors import KafkaError

def setup_logging(verbosity):
    level = logging.CRITICAL
    if verbosity == 1:
        level = logging.ERROR
    elif verbosity == 2:
        level = logging.WARNING
    elif verbosity == 3:
        level = logging.INFO
    elif verbosity >= 4:
        level = logging.DEBUG

    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description='Test connection to one or more Kafka brokers.')
    parser.add_argument('--broker', action='append', dest='broker_list', required=True, help='Kafka broker address, and optionally port :9092. Can be specified multiple times.')
    parser.add_argument('--topic', required=True, help='Destination Kafka topic')
    parser.add_argument('--auth', action='store_true', help='Use authentication')
    parser.add_argument('--protocol', choices=['SASL_PLAINTEXT', 'SASL_SSL'], default='SASL_PLAINTEXT')
    parser.add_argument('--mechanism', choices=['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'], default='PLAIN')
    parser.add_argument('--username', help='SASL username')
    parser.add_argument('--password', help='SASL password')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='Increase logging verbosity (can be specified multiple times)')

    args = parser.parse_args()
    if args.auth:
        missing_args = []
        if not args.username:
            missing_args.append("--username")
        if not args.password:
            missing_args.append("--password")
        
        if missing_args:
            parser.error(f"The following arguments are required with --auth: {' '.join(missing_args)}")

    if os.isatty(sys.stdin.fileno()):
        sys.exit("Error: This script expects input via stdin")

    setup_logging(args.verbose)
    message = sys.stdin.read().encode('utf-8')

    # Set a global socket timeout for DNS resolution
    socket.setdefaulttimeout(2)  # Timeout for DNS resolution

    for current_broker in args.broker_list:
        broker = current_broker if ':' in current_broker else f"{current_broker}:9092"
        producer_config = {
            'bootstrap_servers': [broker],
            'retries': 1,
            'retry_backoff_ms': 1000,
            'max_block_ms': 5000,
        }

        # Update the configuration if authentication is enabled
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
            result = future.get(timeout=10)  # Ensures that the future resolves within 10 seconds
            print(f"{broker}: Success")
        except KafkaError as e:
            sys.exit(f"{broker}: Kafka related failure - {e}")
        except Exception as e:
            logging.debug(traceback.format_exc())
            sys.exit(f"{broker}: Unexpected network related failure - {e}")
        finally:
            if 'producer' in locals():
                producer.close()

if __name__ == '__main__':
    main()

