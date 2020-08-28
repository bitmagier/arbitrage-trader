#IN PROGRESS
- Receive own Orders:  API implementation for binance [roman]

#TODO
- Receive Wallet data from bitfinex
- Receive own Orders:  API implementation for bitfinex
- blacklist assets per exchange (e.g. for NOT touching personal investments)
- decouple delivery of reference ticker from ExchangeTPDataManager channel, because ReferenceTicker shall contain as much TradePairs as possible and not just the ones remaining after trade pair cleanup

- TradeRoom: Optimize order limits based on orderbook
- Submit Order API impl. binance+bitfinex
- Query trade fees

- temporary TradePair down detection + action
- temporary Exchange maintenance/down detection + action 
- shudown app in case of serious exceptions


#NOTES
- TradeRoom: reject OrderBundles, where another active Order of the same TradePair is still active
