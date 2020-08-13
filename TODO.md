# TODO list

in progress:
- Receive Wallet data from binance+bitfinex [roman]

TODO
- TradeRoom: extensive Order Request validation using Wallet & Ticker
  and also the simulated costs of the 2 compensating transactions:
  - the one beforehand, to buy the altcoin from an uninvolved reserve asset (BTC/USDT/...)
  - and one afterwards to convert the bought altcoin on the other exchange back to an uninvolved reserve asset   
  
- TradeRoom: reject OrderBundles, where another active Order of the same TradePair is still active 
- Query own Orders:  API implementation for binance+bitfinx
- Submit Order API impl. binance+bitfinex
- Query trade/withdraw fees

- Withdrawal API binance, bitfinex

- temporary TradePair down detection + action
- temporary Exchange maintenance/down detection + action 
