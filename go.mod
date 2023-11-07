module github.com/hootuu/cang

go 1.20


replace github.com/hootuu/utils => ../utils

require (
	github.com/hootuu/utils v1.0.2
)

require (
	github.com/boltdb/bolt v1.3.1
	go.uber.org/zap v1.26.0
)

require golang.org/x/sys v0.13.0 // indirect
