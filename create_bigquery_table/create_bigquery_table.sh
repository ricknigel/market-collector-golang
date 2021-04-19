#!/usr/bin/bash

echo "start craete DataSet in BigQuery"

dataset_name="BTC_MARKET_PRICE"

bq mk ${dataset_name}

echo "start create Tables in BigQuery"

exchange_array=(BINANCE_BTCUSDT BITFINEX_BTCUSD BITFLYER_BTCFXJPY BITMEX_BTCUSD)

period_array=(1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 3d 1w)

for exchange in ${exchange_array[@]}; do
  for period in ${period_array[@]}; do
    table_name="${exchange}_${period}"
    echo "create ${table_name} table"
    bq mk --table "${dataset_name}.${table_name}" table_schema.json
  done
done
