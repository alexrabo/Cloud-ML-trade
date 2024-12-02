from datetime import datetime
from pydantic import BaseModel, field_validator
from typing import Optional

class Trade(BaseModel):
    """
    A trade from the Kraken API.

    "symbol": "MATIC/USD",
    "side": "sell",
    "price": 0.5117,
    "qty": 40.0,
    "ord_type": "market",
    "trade_id": 4665906,
    "timestamp": "2023-09-25T07:49:37.708706Z"
    """
    pair: str
    price: float
    volume: float
    timestamp: datetime
    timestamp_ms: int
    
    # # TODO: let Pydantic do the initialization of timestamp_ms from timestamp
    # @field_validator("timestamp_ms", mode="after")
    # def set_timestamp_ms(cls, v, values):
    #     """
    #     Converts the timestamp to milliseconds.
    #     """
    #     return int(values["timestamp"].timestamp() * 1000)

    def to_dict(self) -> dict:
        # pydantic method to convert the model to a dict
        return self.model_dump_json()
        
        # if you prefer not to serialize all the fields, you can do this:
        # return {
        #     "pair": self.pair,
        #     "price": self.price,
        #     "volume": self.volume,
        #     "timestamp_ms": self.timestamp_ms,
        #     "timestamp": self.timestamp,
        # }
        # return {**self.model_dump_json()}