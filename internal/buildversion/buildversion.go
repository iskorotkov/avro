// Package buildversion reports the module version a binary was built from.
package buildversion

import "runtime/debug"

// Get returns the version of the module that contains the running main package.
func Get() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return fromBuildInfo(info)
}

// fromBuildInfo reads Main, which is the module of the main package for go build, go install and go tool alike.
func fromBuildInfo(info *debug.BuildInfo) string {
	m := info.Main
	if m.Replace != nil {
		m = *m.Replace
	}
	if m.Version == "" {
		return "unknown"
	}
	return m.Version
}
