# TODO list

- LiquidityManager [concept]: responsible for providing the Assets which are demanded by Traders.
  - one global manager:
    - requests withdrawal jobs to balance liquidity among exchanges 
  - one manager per exchange
    - manages liquidity storing assets (like BTC, USDT) (currently unused altcoin liquidity goes back to these)
    - provides urgent requests for liquidity demands of assets (non liquitidy storing assets) required for upcoming trade requests  

- TradeRoom: extensive Order Request validation using Wallet & Ticker
- TradeRoom: reject OrderBundles, where another active Order of the same TradePair is still active 
- Query own Orders API binance,bitfinx
- Submit Order API binance,bitfinex
- Query trade/withdraw fees

- Wallet API binance,bitfinex
- Withdrawal API binance, bitfinex

- temporary TradePair down detection + action
- temporary Exchange maintenance/down detection + action 
