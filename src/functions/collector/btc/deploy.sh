#!/usr/bin/bash

topicId="collect-schedule-topic"

# トピック作成
gcloud pubsub topics create ${topicId}

# スケジュール作成
gcloud scheduler jobs create pubsub "market-collect-scheduler" \
  --schedule="0 */8 * * *" \
  --topic=${topicId} \
  --message-body="none" \
  --time-zone="Asia/Tokyo"

# 関数作成
gcloud functions deploy "BtcMarketCollector" \
  --runtime=go113 \
  --trigger-topic=${topicId} \
  --region=asia-northeast1 \
  --env-vars-file=.env.yml \
  --timeout=300
