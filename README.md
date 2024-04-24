# Kafka Endpoint Tester

This script, `test_kafka_producer.py`, is designed to test and validate Kafka broker connections prior to configuring these brokers in FortiSIEM. It provides a straightforward method to ensure that the necessary "plumbing" between your application and Kafka brokers is functioning correctly before you proceed with integration into FortiSIEM. This is particularly useful as the FortiSIEM GUI may not provide detailed error messages or troubleshooting information when issues arise with broker connections.

## Features

- **Multiple Broker Support**: Test one or multiple brokers by specifying each broker's address.
- **SASL Authentication**: Supports testing brokers that require SASL authentication with configurable mechanisms such as PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512.
- **Verbosity Levels**: Adjustable logging verbosity to control the amount of output detail.
- **Input via STDIN**: Accepts message input through standard input to send to the Kafka broker.

## Prerequisites

- Python 3.6 or higher
- `kafka-python` package (This script will attempt to install the package if it is not already installed)

## Installation

Clone this repository to your local machine using:

```bash
git clone https://github.com/kmickeletto/test_kafka_endpoints.git
cd test_kafka_endpoints
```

## Usage

1. **Prepare Input**: Pipe in the message you want to send to the Kafka topic via STDIN.
2. **Run the Script**:
    ```bash
    echo "Your message here" | python3 test_kafka_producer.py --broker your_broker_address:9092 --topic your_topic_name [additional options]
    ```

### Command Line Arguments

- `--broker`: Specify the Kafka broker's address. This option can be repeated to specify multiple brokers.
- `--topic`: Specify the Kafka topic to which the message will be sent.
- `--file` : File containing the message to send.
- `--auth`: Enable SASL authentication (must be used with `--username` and `--password`).
- `--protocol`: Choose the security protocol (`SASL_PLAINTEXT` or `SASL_SSL`). Default is `SASL_PLAINTEXT`.
- `--mechanism`: Specify the SASL mechanism (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`). Default is `PLAIN`.
- `--username`: SASL username (required if `--auth` is used).
- `--password`: SASL password (required if `--auth` is used).
- `-v`, `--verbose`: Increase logging verbosity. This can be specified multiple times to increase verbosity.

## Contributing

Contributions to improve the script or suggestions for additional features are welcome. Please fork the repository and submit a pull request with your changes.

