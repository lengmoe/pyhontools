import logging
import json
from kafka import KafkaProducer

class KafkaHandler(logging.Handler):
    """Class to instantiate the kafka logging facility."""
    producer = None

    def __init__(self, topic, config:dict, **kwargs):
        super(KafkaHandler, self).__init__(level=logging.WARN, **kwargs)
        self.topic = topic
        self.producer = KafkaProducer(
            linger_ms=10,
            acks=0,
            # metadata_max_age_ms=100,
            request_timeout_ms=500,
            max_block_ms=500,
            batch_size=65536,
            compression_type="lz4",
            # buffer_memory=268435456,
            max_request_size=3145728,
            **config
        )
        try:
            self.producer.send(topic=topic, value=b'')
        except:
            pass


    def emit(self, record):
        if 'kafka' in record.name:
            return

        try:
            msg = None
            message = {}
            message.update(record.__dict__)
            message.pop('message')
            try:
                msg = json.dumps(message, skipkeys=True)
            except:
                msg = self.format(record)

            try:
                self.producer.send(self.topic, value=msg.encode(), partition=0)
            except:
                pass

        except (KeyboardInterrupt, SystemExit):
            raise

        except Exception as e:
            logging.Handler.handleError(self, record)


    def flush(self):
        """
        Ensure all logging output has been flushed.

        This version does nothing and is intended to be implemented by
        subclasses.
        """
        #get the module data lock, as we're updating a shared structure.
        if self.producer is not None and not self.producer._closed:
            self.acquire()
            try:    #unlikely to raise an exception, but you never know...
                self.producer.flush(timeout=1000)
            finally:
                self.release()
           
    # def close(self):
    #     print(10)
    #     super().close()
    #     print(11)
    #     # self.acquire()
    #     # try:    #unlikely to raise an exception, but you never know...
    #     #     self.producer.flush(timeout=1000)
    #     # finally:
    #     #    self.release()
    #     # super().close()
    #     # if self.producer._sender.is_alive():
    #     #     self.producer._sender.join()
