export SBT_OPTS="-Xmx2G -XX:+UseShenandoahGC"
sbt run 2>&1 | tee /tmp/arbitrage-trader-run$(date "+%Y-%m-%dT%H:%M"|tr -d '\n').log

