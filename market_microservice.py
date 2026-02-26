"""
Market Microservice - Reactive Order Book Engine
Architecture Choice: Wrapper pattern around ccxt for unified exchange interface
with automatic reconnection, rate limiting, and Firestore state persistence.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from enum import Enum
import ccxt
import ccxt.pro as ccxt_pro
import numpy as np
from firebase_admin import firestore

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"

class ExchangeStatus(Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RATE_LIMITED = "rate_limited"
    MAINTENANCE = "maintenance"

class MarketMicroservice:
    """Unified exchange interface with automatic reconnection and state persistence"""
    
    def __init__(self, exchange_id: str, api_key: Optional[str] = None, 
                 api_secret: Optional[str] = None, sandbox: bool = False):
        """
        Initialize exchange connection with Firestore state tracking
        
        Args:
            exchange_id: CCXT exchange ID (e.g., 'binance', 'coinbasepro')
            api_key: Optional API key for authenticated endpoints
            api_secret: Optional API secret
            sandbox: Use testnet/sandbox environment
        """
        self.exchange_id = exchange_id
        self.sandbox = sandbox
        self.status = ExchangeStatus.DISCONNECTED
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.rate_limit_reset = None
        
        # Initialize Firestore client for state persistence
        try:
            self.db = firestore.client()
            self.state_ref = self.db.collection('exchange_states').document(exchange_id)
        except Exception as e:
            logger.error(f"Firestore initialization failed: {e}")
            self.db = None
            self.state_ref = None
        
        # Initialize exchange
        self._init_exchange(api_key, api_secret)
        
    def _init_exchange(self, api_key: Optional[str], api_secret: Optional[str]) -> None:
        """Initialize CCXT exchange with error handling"""
        try:
            exchange_class = getattr(ccxt, self.exchange_id)
            config = {
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'}
            }
            
            if self.sandbox:
                config.setdefault('urls', {})['api'] = 'https://testnet.binance.vision/api'
            
            self.exchange = exchange_class(config)
            self.status = ExchangeStatus.CONNECTED
            logger.info(f"Initialized {self.exchange_id} (sandbox={self.sandbox})")
            
            # Persist initial state
            self._persist_state()
            
        except AttributeError:
            logger.error(f"Exchange {self.exchange_id} not found in CCXT")
            raise ValueError(f"Unsupported exchange: {self.exchange_id}")
        except Exception as e:
            logger.error(f"Exchange initialization failed: {e}")
            self.status = ExchangeStatus.DISCONNECTED
            raise
    
    def _persist_state(self) -> None:
        """Persist exchange state to Firestore"""
        if not self.state_ref:
            return
            
        try:
            state_data = {
                'status': self.status.value,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'reconnect_attempts': self.reconnect_attempts,
                'sandbox': self.sandbox,
                'has_credentials': bool(self.exchange.apiKey)
            }
            self.state_ref.set(state_data, merge=True)
        except Exception as e:
            logger.warning(f"State persistence failed: {e}")
    
    def _handle_rate_limit(self) -> None:
        """Handle rate limiting with exponential backoff"""
        if self.status == ExchangeStatus.RATE_LIMITED:
            if self.rate_limit_reset and datetime.now() < self.rate_limit_reset:
                wait_time = (self.rate_limit_reset - datetime.now()).seconds
                logger.warning(f"Rate limited, waiting {wait_time} seconds")
                asyncio.sleep(wait_time)
                return
            
        self.status = ExchangeStatus.CONNECTED
    
    async def get_order_book(self, symbol: str, limit: int = 10) -> Dict[str, List[List[float]]]:
        """
        Fetch order book with automatic reconnection
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
            limit: Number of orders to return
            
        Returns:
            Dictionary with 'bids' and 'asks' lists
        """
        self._handle_rate_limit()
        
        for attempt in range(self.max_reconnect_attempts):
            try:
                order_book = await self.exchange.fetch_order_book(symbol, limit)
                
                # Validate response structure
                if 'bids' not in order_book or 'asks' not in order_book:
                    raise ValueError("Invalid order book structure")
                
                logger.debug(f"Fetched order book for {symbol}: {len(order_book['bids'])} bids, {len(order_book['asks'])} asks")
                self.reconnect_attempts = 0
                self._persist_state()
                
                return {
                    'bids': order_book['bids'],
                    'asks': order_book['asks'],
                    'timestamp': order_book.get('timestamp', datetime.now().timestamp()),
                    'symbol': symbol
                }
                
            except ccxt.RateLimitExceeded:
                self.status = ExchangeStatus.RATE_LIMITED
                self.rate_limit_reset = datetime.now().fromtimestamp(self.exchange.last_response_headers.get('x-ratelimit-reset', 0))
                logger.warning(f"Rate limit exceeded for {self.exchange_id}")
                self._persist_state()
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                
            except ccxt.RequestTimeout:
                logger.warning(f"Request timeout for {self.exchange_id}, attempt {attempt + 1}")
                await asyncio.sleep(1)
                
            except ccxt.ExchangeNotAvailable:
                self.status = ExchangeStatus.MAINTENANCE
                logger.warning(f"Exchange {self.exchange_id} under maintenance")
                self._persist_state()
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Order book fetch failed: {e}, attempt {attempt + 1}")
                self.reconnect_attempts +=