// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"github.com/artashesbalabekyan/aistore/cluster"
	"github.com/artashesbalabekyan/aistore/cluster/meta"
	"github.com/artashesbalabekyan/aistore/cmn"
	"github.com/artashesbalabekyan/aistore/fs/mpather"
)

type BckJog struct {
	t       cluster.Target
	joggers *mpather.Jgroup
	Base
}

func (r *BckJog) Init(id, kind string, bck *meta.Bck, opts *mpather.JgroupOpts) {
	r.t = opts.T
	r.InitBase(id, kind, bck)
	r.joggers = mpather.NewJoggerGroup(opts)
}

func (r *BckJog) Run()                   { r.joggers.Run() }
func (r *BckJog) Target() cluster.Target { return r.t }

func (r *BckJog) Wait() error {
	for {
		select {
		case errCause := <-r.ChanAbort():
			r.joggers.Stop()
			return cmn.NewErrAborted(r.Name(), "x-bck-jog", errCause)
		case <-r.joggers.ListenFinished():
			return r.joggers.Stop()
		}
	}
}
