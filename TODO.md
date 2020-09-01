#IN PROGRESS
- Receive own order updates:  API implementation for binance [roman]

#TODO
- automatic integration tests for order pipeline (create -> fill -> finish) against exchanges with minimal value
- Receive Wallet data from bitfinex
- Receive own order updates:  API implementation for bitfinex
- blacklist assets per exchange (e.g. for NOT touching personal investments)
- decouple delivery of reference ticker from ExchangeTPDataManager channel, because ReferenceTicker shall contain as much TradePairs as possible and not just the ones remaining after trade pair cleanup

- TradeRoom: Optimize order limits based on orderbook
- Submit Order API impl. binance+bitfinex
- Query trade fees

- temporary TradePair down detection + action
- temporary Exchange maintenance/down detection + action 
- shudown app in case of serious exceptions


#NOTES
Test strategy: 
    - Unit tests
    - Expectation validations wherever possible in production code
       
       Because a lot of things can only be tested with responses from real-world exchanges,
       I prefer in-code expectation validations instead of integration-tests.
      
