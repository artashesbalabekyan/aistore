//go:build !gcp

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cluster"
)

func NewGCP(_ cluster.TargetPut) (cluster.BackendProvider, error) {
	return nil, newErrInitBackend(apc.GCP)
}
