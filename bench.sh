go test -bench=BenchmarkSafeMap -run=^$ -benchmem -benchtime=10s
go test -bench=BenchmarkSyncMap -run=^$ -benchmem -benchtime=10s
go test -bench=BenchmarkCronManager -run=^$ -benchmem -benchtime=5s
go test -bench=BenchmarkMonitor -run=^$ -benchmem -benchtime=5s
go test -bench=BenchmarkCountOpenFileDescriptors -run=^$ -benchmem -benchtime=5s
