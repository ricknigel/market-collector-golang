#!/usr/bin/bash

echo "start craete DataSet in BigQuery"

dataset_name="BTC_MARKET_PRICE"

bq --location=asia-northeast1 mk -d ${dataset_name}

echo "start create Tables in BigQuery"

exchange_array=(BINANCE_BTCUSDT BITFINEX_BTCUSD BITFLYER_BTCFXJPY BITMEX_BTCUSD-PERPETUAL-FUTURE-INVERSE)

period_array=(1M 3M 5M 15M 30M 1H 2H 4H 6H 12H 1D 3D 1W)

for exchange in ${exchange_array[@]}; do
  for period in ${period_array[@]}; do
    table_name="${exchange}_${period}"
    echo "create ${table_name} table"
    bq mk -t "${dataset_name}.${table_name}" BTC_MARKET_PRICE_schema.json
  done
done
