from confluent_kafka import Producer, Message # type: ignore
import socket
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)


class KafkaProducer():
    def __init__(self, topic: str='default') -> None:
        self._producer = Producer({ # type: ignore
            'bootstrap.servers': 'localhost:9092',
            'client.id': socket.gethostname()
        })
        self._topic = topic

    def _acked(self, err, msg: Message) -> None: # type: ignore
        if err is not None:
            logging.error("Failed to deliver message: %s: %s" % (str(msg), str(err))) # type: ignore
        logging.debug("Message produced: %s" % (str(msg.value()))) # type: ignore
    
    def produce(self, message: str) -> None:
        self._producer.produce(self._topic, value=message, callback=self._acked) # type: ignore
        self._producer.poll(1) # type: ignore
