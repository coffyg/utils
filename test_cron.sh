#!/bin/bash

# Run only cron-related tests
echo "Running all cron tests..."
go test -v -timeout 60s -run "Cron" .