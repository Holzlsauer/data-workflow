import logging
import socket
from confluent_kafka import Producer, Message # type: ignore

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)


class KafkaProducer:
    """
    Simple producer class for sending messages to a Kafka topic on localhost.

    Args:
        topic (str, optional): The Kafka topic name. Defaults to "default".
    """

    def __init__(self, topic: str = 'default') -> None:
        self._producer = Producer({ # type: ignore
            'bootstrap.servers': 'kafka:19092',
            'client.id': socket.gethostname()
        })
        self._topic = topic

    def _acked(self, err, msg: Message) -> None: # type: ignore
        """
        Callback method for handling message delivery status.

        Args:
            err (Any): Error object, None if successful.
            msg (Message): The message object that was delivered.
        """

        if err is not None:
            logging.error("Failed to deliver message: %s: %s", str(msg), str(err)) # type: ignore
        else:
            logging.info("Message produced: %s", str(msg.value())) # type: ignore

    def produce(self, message: str) -> None:
        """
        Sends a message to the configured Kafka topic.

        Args:
            message (str): The message content to send.
        """
        self._producer.produce( # type: ignore
            self._topic, value=message, callback=self._acked # type: ignore
        )
        self._producer.poll(1) # type: ignore

