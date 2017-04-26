
test:
	go test -v -bench=. -coverprofile=.coverage.out
	go tool cover -func=.coverage.out
