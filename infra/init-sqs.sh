#!/bin/sh
set -eu
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true,ContentBasedDeduplication=false
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes FifoQueue=true,ContentBasedDeduplication=false
awslocal sqs create-queue --queue-name wager-events.fifo --attributes FifoQueue=true,ContentBasedDeduplication=false
