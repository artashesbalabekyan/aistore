// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cluster"
	"github.com/artashesbalabekyan/aistore/cluster/meta"
	"github.com/artashesbalabekyan/aistore/cmn/cos"
	"github.com/artashesbalabekyan/aistore/cmn/debug"
	"github.com/artashesbalabekyan/aistore/xact"
	"github.com/artashesbalabekyan/aistore/xact/xreg"
)

type (
	eleFactory struct {
		xreg.RenewBase
		xctn *Election
	}
	Election struct {
		xact.Base
	}
)

// interface guard
var (
	_ cluster.Xact   = (*Election)(nil)
	_ xreg.Renewable = (*eleFactory)(nil)
)

func (*eleFactory) New(xreg.Args, *meta.Bck) xreg.Renewable { return &eleFactory{} }

func (p *eleFactory) Start() error {
	p.xctn = &Election{}
	p.xctn.InitBase(cos.GenUUID(), apc.ActElection, nil)
	return nil
}

func (*eleFactory) Kind() string        { return apc.ActElection }
func (p *eleFactory) Get() cluster.Xact { return p.xctn }

func (*eleFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprUse, nil
}

func (*Election) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *Election) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)
	return
}
