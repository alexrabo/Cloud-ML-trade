from quixstreams import Application
from loguru import logger


class QuixStreamingService:
    def __init__(self, broker_address: str, topic_name: str):
        self.broker_address = broker_address
        self.topic_name = topic_name
        self.app = None
        self.topic = None
        self.producer = None

    async def start(self):
        self.app = Application(broker_address=self.broker_address)
        self.topic = self.app.topic(name=self.topic_name, value_serializer='json')
        self.producer = self.app.get_producer()
        await self.producer.__aenter__()

    async def stop(self):
        if self.producer:
            await self.producer.__aexit__(None, None, None)

    async def publish_trade(self, trade):
        message = self.topic.serialize(
            key=trade.pair.replace('/', '-'),
            value=trade.to_dict(),
        )

        await self.producer.produce(
            topic=self.topic.name,
            value=message.value,
            key=message.key
        )
        logger.info(f'Pushed trade to Kafka: {trade}') 