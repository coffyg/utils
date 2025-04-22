#!/bin/bash
set -e

echo "Running all cron tests without race detection..."
echo "================================================"
go test -v -run "^TestCron|TestPreciseTiming|TestJobReplacement" ./...

echo ""
echo "Running all cron tests with race detection..."
echo "============================================="
go test -race -v -run "^TestCron|TestPreciseTiming|TestJobReplacement" ./...

echo ""
echo "Running stress test for cron system..."
echo "====================================="
go test -v -run TestCronStress -timeout 30s ./...

echo ""
echo "All tests completed successfully!"