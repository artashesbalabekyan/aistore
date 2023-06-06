// Package main for the AIS node executable.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"os"

	"github.com/artashesbalabekyan/aistore/ais"
	"github.com/artashesbalabekyan/aistore/cmn"
	"github.com/artashesbalabekyan/aistore/cmn/debug"
)

var (
	build     string
	buildtime string
)

func main() {
	debug.Assert(build != "", "missing build")
	debug.Assert(buildtime != "", "missing build time")
	ecode := ais.Run(cmn.VersionAIStore+"."+build, buildtime)
	os.Exit(ecode)
}
