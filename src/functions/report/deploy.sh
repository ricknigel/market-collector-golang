#!/usr/bin/bash

topicId="error-report-topic"

gcloud pubsub topics create ${topicId}

gcloud functions deploy "ErrorReport" \
  --runtime go113 \
  --trigger-topic ${topicId} \
  --region asia-northeast1 \
  --env-vars-file .env.yml
