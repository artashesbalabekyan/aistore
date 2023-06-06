// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"time"

	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cluster"
	"github.com/artashesbalabekyan/aistore/cluster/meta"
	"github.com/artashesbalabekyan/aistore/cmn"
	"github.com/artashesbalabekyan/aistore/cmn/cos"
	"github.com/artashesbalabekyan/aistore/cmn/debug"
)

// NOTE: compare with cluster/lom_dp.go

type OfflineDP struct {
	tcbmsg         *apc.TCBMsg
	comm           Communicator
	requestTimeout time.Duration
}

// interface guard
var _ cluster.DP = (*OfflineDP)(nil)

func NewOfflineDP(msg *apc.TCBMsg, lsnode *meta.Snode) (*OfflineDP, error) {
	comm, err := GetCommunicator(msg.Transform.Name, lsnode)
	if err != nil {
		return nil, err
	}
	pr := &OfflineDP{tcbmsg: msg, comm: comm}
	pr.requestTimeout = time.Duration(msg.Transform.Timeout)
	return pr, nil
}

// Returns reader resulting from lom ETL transformation.
func (dp *OfflineDP) Reader(lom *cluster.LOM) (cos.ReadOpenCloser, cos.OAH, error) {
	var (
		r   cos.ReadCloseSizer // note: +sizer
		err error
	)
	debug.Assert(dp.tcbmsg != nil)
	call := func() (int, error) {
		r, err = dp.comm.OfflineTransform(lom.Bck(), lom.ObjName, dp.requestTimeout)
		return 0, err
	}
	// TODO: Check if ETL pod is healthy and wait some more if not (yet).
	err = cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call:      call,
		Action:    "read [" + dp.tcbmsg.Transform.Name + "]-transformed " + lom.Cname(),
		SoftErr:   5,
		HardErr:   2,
		Sleep:     50 * time.Millisecond,
		BackOff:   true,
		Verbosity: cmn.RetryLogQuiet,
	})
	if err != nil {
		return nil, nil, err
	}
	lom.SetAtimeUnix(time.Now().UnixNano())
	oah := &cmn.ObjAttrs{
		Size:  r.Size(),
		Ver:   "",            // transformed object - current version does not apply
		Cksum: cos.NoneCksum, // TODO: checksum
		Atime: lom.AtimeUnix(),
	}
	return cos.NopOpener(r), oah, nil
}
