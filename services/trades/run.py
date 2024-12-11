from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from kraken_api.base import TradesAPI
from factory import TradesAPIFactory
from loguru import logger
import asyncio
from typing import Protocol
from dataclasses import dataclass
from streaming import QuixStreamingService
from importlib import import_module


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
                await streaming_service.publish_trade(trade)

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from config import config

        # Initialize services using factory
        trades_api = TradesAPIFactory.create(
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

        # Store services in router state
        self.state = type('RouterState', (), {
            'trades_api': trades_api,
            'streaming_service': streaming_service
        })

        # Start processing trades
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
    trades_router = TradesRouter()
    trades_router.add_api_route("/status", lambda: {"status": "running"}, methods=["GET"])
    app.mount("/trades", trades_router)
    return app


app = create_app()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)