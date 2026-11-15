package kv

// CompatibilityFormats reports the format levels this state machine currently
// writes and the highest command kind it can apply. The values are intended for
// compatibility reporting; they do not activate commands cluster-wide.
func CompatibilityFormats() (commandVersion, snapshotVersion, maxCommandKind byte) {
	return wireVersion, smSnapshotVersion, kindLeaseTick
}
