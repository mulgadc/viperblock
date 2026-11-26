//go:build race

package viperblock

// raceDetector is true in a -race build, which slows a stream beside its
// neighbours enough to change how often they replace its position.
const raceDetector = true
