package kv

// CompatibilityFormats reports the command and snapshot formats supported by
// this state machine. The values are intended for compatibility reporting;
// they do not activate commands cluster-wide.
func CompatibilityFormats() (commandVersion, snapshotWriteVersion, maxCommandKind byte, snapshotReadVersions []byte) {
	return wireVersion, smSnapshotVersion, kindLeaseTick, []byte{1, 2, snapshotVersion, smSnapshotVersion}
}
