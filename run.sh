export SBT_OPTS="-Xmx2G -XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
sbt run 2>&1 | tee /tmp/arbitrage-trader-run$(date "+%Y-%m-%dT%H:%M"|tr -d '\n').log

