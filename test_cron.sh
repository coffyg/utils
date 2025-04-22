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

# Comment out the following line to run CPU diagnostics
exit 0

echo ""
echo "Running CPU diagnostics to check for potential issues..."
echo "======================================================="
echo "This test will take 3 seconds. Please monitor your CPU usage during this time."
echo ""
go test -v -run TestCronCPUSpinCheck ./...

echo ""
echo "Running full CPU profiling (this will take longer)..."
echo "==================================================="
echo "This test will take 10 seconds. Please monitor your CPU usage during this time."
echo ""
go test -v -run TestCronCPUUsage ./...