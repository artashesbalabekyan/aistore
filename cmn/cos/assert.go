// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"

	"github.com/artashesbalabekyan/aistore/3rdparty/glog"
)

const assertMsg = "assertion failed"

// NOTE: Not to be used in the datapath - consider instead one of the flavors below.
func Assertf(cond bool, f string, a ...any) {
	if !cond {
		AssertMsg(cond, fmt.Sprintf(f, a...))
	}
}

func Assert(cond bool) {
	if !cond {
		glog.Flush()
		panic(assertMsg)
	}
}

// NOTE: when using Sprintf and such, `if (!cond) { AssertMsg(false, msg) }` is the preferable usage.
func AssertMsg(cond bool, msg string) {
	if !cond {
		glog.Flush()
		panic(assertMsg + ": " + msg)
	}
}

func AssertNoErr(err error) {
	if err != nil {
		glog.Flush()
		panic(err)
	}
}
