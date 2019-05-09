.PHONY: all

all: test_example test_homework cleanup gendata

test_example:
	go test -v -run=TestExampleURLTop -timeout 20m

test_homework:
	go test -v -run=TestURLTop -timeout 20m

cleanup:
	go test -v -run=TestCleanData

gendata:
	go test -v -run=TestGenData

te:
	go test -v -run=TestExampleURLTop -timeout 20m -cpuprofile=cpu.profile -blockprofile=block.profile -memprofile=mem.profile

memp:
	go tool pprof -http=:8080 ./talent.test ./mem.profile
cpup:
	go tool pprof -http=:8080 ./talent.test ./cpu.profile
blockp:
	go tool pprof -http=:8080 ./talent.test ./block.profile
