export SBT_OPTS="-Xmx2G -XX:+UseZGC" # -XX:+UseG1GC"
sbt run 2>&1 | tee /tmp/arbitrage-trader-run$(date "+%Y-%m-%dT%H:%M"|tr -d '\n').log

