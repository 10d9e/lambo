build:
	go build -tags netgo -ldflags '-s -w' -o lambo

run:
	go run main.go

clean:
	rm lambo