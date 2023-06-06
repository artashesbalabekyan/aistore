// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/xact/xreg"
)

func Init() {
	xreg.RegBckXact(&tcbFactory{kind: apc.ActCopyBck})
	xreg.RegBckXact(&tcbFactory{kind: apc.ActETLBck})
	xreg.RegBckXact(&mncFactory{})
	xreg.RegBckXact(&putFactory{})
}
