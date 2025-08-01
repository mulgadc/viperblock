module github.com/mulgadc/viperblock

go 1.23.0

//toolchain go1.24.3

replace libguestfs.org/nbdkit => ./nbd/libguestfs.org/nbdkit

//replace github.com/mulgadc/predastore => /home/ben/Development/predastore

require (
	github.com/aws/aws-sdk-go v1.55.7
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/stretchr/testify v1.9.0
	golang.org/x/sync v0.14.0
)

require libguestfs.org/nbdkit v0.0.0-00010101000000-000000000000 // indirect

require (
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gofiber/fiber/v2 v2.52.7 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mulgadc/predastore v0.0.0-20250424095920-e9bcbc4795e1
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.51.0 // indirect
	github.com/valyala/tcplisten v1.0.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
