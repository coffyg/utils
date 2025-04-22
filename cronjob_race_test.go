//go:build race

package utils

// This file is only compiled when the race detector is enabled
// It sets a global flag in the test package to indicate that the race detector is active

func init() {
	raceEnabled = true
}