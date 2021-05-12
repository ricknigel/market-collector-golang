#!/usr/bin/bash

# gcloud pubsub topics create "error-report-topic"

gcloud functions deploy "ErrorReport" \
  --runtime go113 \
  --trigger-topic "error-report-topic" \
  --region asia-northeast1 \
  --env-vars-file .env.yml
