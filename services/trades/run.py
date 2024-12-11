from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from kraken_api.base import TradesAPI
from kraken_api.mock import KrakenMockAPI
from kraken_api.rest import KrakenRestAPI
from kraken_api.websocket import KrakenWebsocketAPI
from loguru import logger
from quixstreams import Application
from typing import Protocol
from dataclasses import dataclass


class TradesService(Protocol):
    def get_trades_api(self) -> TradesAPI:
        """Returns configured trades API instance"""
        pass

@dataclass
class KrakenTradesService:
    data_source: str
    pairs: list[str]
    last_n_days: int = None
    _trades_api: TradesAPI = None

    def get_trades_api(self) -> TradesAPI:
        if not self._trades_api:
            self._trades_api = self._create_trades_api()
        return self._trades_api

    def _create_trades_api(self) -> TradesAPI:
        if self.data_source == 'live':
            return KrakenWebsocketAPI(pairs=self.pairs)
        elif self.data_source == 'historical':
            return KrakenRestAPI(pairs=self.pairs, last_n_days=self.last_n_days)
        elif self.data_source == 'test':
            return KrakenMockAPI(pairs=self.pairs)
        else:
            raise ValueError(f'Invalid data source: {self.data_source}')


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


class TradesRouter(APIRouter):
    def __init__(self):
        super().__init__()
        self.trades_task = None

    async def process_trades(self):
        trades_api = self.state.trades_api
        streaming_service = self.state.streaming_service

        while not trades_api.is_done():
            trades = trades_api.get_trades()

            for trade in trades:
                message = streaming_service.topic.serialize(
                    key=trade.pair.replace('/', '-'),
                    value=trade.to_dict(),
                )

                await streaming_service.producer.produce(
                    topic=streaming_service.topic.name,
                    value=message.value,
                    key=message.key
                )
                logger.info(f'Pushed trade to Kafka: {trade}')

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Load config
        from config import config

        # Initialize services
        trades_service = KrakenTradesService(
            data_source=config.data_source,
            pairs=config.pairs,
            last_n_days=config.last_n_days
        )

        streaming_service = QuixStreamingService(
            broker_address=config.kafka_broker_address,
            topic_name=config.kafka_topic
        )

        # Start services
        await streaming_service.start()
        trades_api = trades_service.get_trades_api()

        # Store services in router state
        self.state = type('RouterState', (), {
            'trades_api': trades_api,
            'streaming_service': streaming_service
        })

        # Start processing trades
        import asyncio
        self.trades_task = asyncio.create_task(self.process_trades())
        
        logger.info('Trades service started')
        yield

        # Cleanup
        if self.trades_task:
            self.trades_task.cancel()
            try:
                await self.trades_task
            except asyncio.CancelledError:
                pass

        await streaming_service.stop()
        logger.info('Trades service stopped')


def create_app() -> FastAPI:
    app = FastAPI()
    
    # Create and configure trades router
    trades_router = TradesRouter()
    trades_router.add_api_route("/status", lambda: {"status": "running"}, methods=["GET"])
    
    # Mount the trades router with its own lifespan
    app.mount("/trades", trades_router)
    
    return app


app = create_app()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)