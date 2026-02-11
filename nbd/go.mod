module main

go 1.26.0

replace libguestfs.org/nbdkit => ./libguestfs.org/nbdkit

replace github.com/mulgadc/viperblock => ../

require (
	github.com/mulgadc/viperblock v1.0.10
	libguestfs.org/nbdkit v1.0.0
)

require (
	github.com/aws/aws-sdk-go v1.55.8 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/text v0.31.0 // indirect
)
