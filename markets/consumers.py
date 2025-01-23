import json
from channels.generic.websocket import AsyncWebsocketConsumer
import random
import time
import asyncio

ASSETS = [
    "BTC", "ETH", "LTC", "XRP", "BCH", "USDC", "XMR", "XLM",
    # Add all assets from the assignment
]

class MarketConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        print(f"Client connected")

    async def disconnect(self, close_code):
        print(f"Client disconnected")
        pass


    async def receive(self, text_data):
        message = json.loads(text_data)
        if message.get("event") == "subscribe" and message.get("channel") == "rates":
            print(f"Subscription received")
            while True:
                await self.send(json.dumps({
                    "channel": "rates",
                    "event": "data",
                    "data": self.generate_mock_data()
                }))
                await asyncio.sleep(1)

    def generate_mock_data(self):
        asset = random.choice(ASSETS)
        base_price = 90000  
        bid = base_price - random.uniform(0, 1000)
        ask = base_price + random.uniform(0, 1000)
        return {
            "symbol": f"{asset}_CAD",
            "timestamp": int(time.time()),
            "bid": round(bid, 2),
            "ask": round(ask, 2),
            "spot": round((bid + ask) / 2, 2),
            "change": round(random.uniform(-5, 5), 2)
        }