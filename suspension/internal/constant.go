package internal

// ===== Input annotations (written by other controllers) =====

const (
	// Gate annotations (must exist; missing => WAIT)
	AnnoRestorePhase = "migration.dcnlab.com/restore-phase" // succeeded|failed|none|...
	AnnPVRestore     = "stwmig.karmada.io/pv-migration"          // completed|error|none|...

	// RestorePhase values
	RestoreSucceeded = "succeeded"
	RestoreFailed    = "failed"
	RestoreNone      = "none"

	// PV restore values
	PVCompleted = "completed"
	PVError     = "error"
	PVNone      = "none"
)