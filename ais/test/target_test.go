// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"

	"github.com/artashesbalabekyan/aistore/api"
	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cmn"
	"github.com/artashesbalabekyan/aistore/tools"
	"github.com/artashesbalabekyan/aistore/tools/readers"
	"github.com/artashesbalabekyan/aistore/tools/tassert"
)

func TestPutObjectNoDaemonID(t *testing.T) {
	const (
		objName = "someObject"
	)
	var (
		sid          string
		objDummyData = []byte("testing is so much fun")
		proxyURL     = tools.RandomProxyURL()
		smap         = tools.GetClusterMap(t, proxyURL)
		bck          = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
	)

	si, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	sid = si.ID()

	url := smap.Tmap[sid].URL(cmn.NetPublic)
	baseParams := tools.BaseAPIParams(url)
	reader := readers.NewBytesReader(objDummyData)
	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	if _, err := api.PutObject(putArgs); err == nil {
		t.Errorf("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}

func TestDeleteInvalidDaemonID(t *testing.T) {
	val := &apc.ActValRmNode{
		DaemonID:          "abcde:abcde",
		SkipRebalance:     true,
		KeepInitialConfig: true,
	}
	if _, err := api.DecommissionNode(tools.BaseAPIParams(), val); err == nil {
		t.Errorf("Error is nil, expected NotFound error on a delete of a non-existing target")
	}
}
