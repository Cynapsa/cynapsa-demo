package commandgate

import (
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

type terminalDisposition struct {
	once     sync.Once
	command  model.Command
	callback CompletionDisposition
}

func (disposition *terminalDisposition) resolve(committed bool) {
	if disposition == nil || disposition.callback == nil {
		return
	}
	disposition.once.Do(func() {
		disposition.callback(disposition.command, model.Result{}, committed)
	})
}

func (disposition *terminalDisposition) resolveResult(result model.Result, committed bool) {
	if disposition == nil || disposition.callback == nil {
		return
	}
	disposition.once.Do(func() {
		disposition.callback(disposition.command, result, committed)
	})
}
