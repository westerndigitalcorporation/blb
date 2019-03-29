package blb

import (
	"time"
)

// BackupReadBehavior lets clients decide when to enable or disable backup reads. The delay
// field determines how long before the backup request is sent.
type BackupReadBehavior struct {
	// Enabled is a flag to enable the backup read feature.
	Enabled bool

	// Delay is the time duration between successive backup reads.
	Delay time.Duration

	// MaxNumBackups is the maximum number of requests sent in addition
	// to the primary read.
	MaxNumBackups int
}

type backupReadState struct {
	BackupReadBehavior

	// backupDelay is the delay before sending backup reads.
	delayFunc func(time.Duration) <-chan time.Time
}

func makeBackupReadState(behavior BackupReadBehavior) backupReadState {
	if behavior.Enabled {
		if behavior.MaxNumBackups <= 0 {
			// Give it a default value of 1.
			behavior.MaxNumBackups = 1
		}
	} else {
		// MaxNumBackups isn't used in the read path if this is disabled, clear this.
		behavior.MaxNumBackups = 0
	}
	return backupReadState{
		BackupReadBehavior: behavior,
		delayFunc:          time.After,
	}
}
