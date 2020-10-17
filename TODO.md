
#TODO

- drop trade pairs on exchange, which have low liquidity

- steering-man actor: taking care, that we keep going into the profit zone
  - provides/analyzes overall statistics
  - proposes adjustments / fine-tuning of configuration [later] 
  - stops the whole application when a loss is detected   

- temporary TradePair down detection + action
- temporary Exchange maintenance/down detection + action 
- shudown app completely in case of serious exceptions

#NOTES
Test strategy: 
    - Unit tests
    - Expectation validations wherever possible in production code
       
       Because a lot of things can only be tested with responses from real-world exchanges,
       I prefer in-code expectation validations instead of integration-tests.
      
