module main

go 1.24.3

replace libguestfs.org/nbdkit => ./libguestfs.org/nbdkit

//#replace github.com/mulgadc/viperblock => /home/ben/Development/viperblock

replace github.com/mulgadc/viperblock => ../

require libguestfs.org/nbdkit v1.0.0

require (
	github.com/aws/aws-sdk-go v1.55.7 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/mulgadc/viperblock v0.0.0-20250512101733-2a708f4c606e
)
