// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"errors"
	"net/http"
	"testing"

	"github.com/artashesbalabekyan/aistore/api"
	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cmn"
	"github.com/artashesbalabekyan/aistore/cmn/cos"
	"github.com/artashesbalabekyan/aistore/tools"
	"github.com/artashesbalabekyan/aistore/tools/readers"
	"github.com/artashesbalabekyan/aistore/tools/tassert"
	"github.com/artashesbalabekyan/aistore/tools/tlog"
	"github.com/artashesbalabekyan/aistore/tools/trand"
)

func createBaseParams() (unAuth, auth api.BaseParams) {
	unAuth = tools.BaseAPIParams()
	unAuth.Token = ""
	auth = tools.BaseAPIParams()
	return
}

func expectUnauthorized(t *testing.T, err error) {
	tassert.Fatalf(t, err != nil, "expected unauthorized error")
	var herr *cmn.ErrHTTP
	tassert.Fatalf(t, errors.As(err, &herr), "expected cmn.ErrHTTP, got %v", err)
	tassert.Fatalf(
		t, herr.Status == http.StatusUnauthorized,
		"expected status unauthorized, got: %d", herr.Status,
	)
}

func TestAuthObj(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiresAuth: true})
	var (
		unAuthBP, authBP = createBaseParams()
		bck              = cmn.Bck{Name: trand.String(10)}
	)
	err := api.CreateBucket(authBP, bck, nil)
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to create %s\n", authBP.Token[:16], bck.String())
	defer func() {
		err := api.DestroyBucket(authBP, bck)
		tassert.CheckFatal(t, err)
		tlog.Logf("bucket %s destroyed\n", bck.String())
	}()

	r, _ := readers.NewRandReader(fileSize, cos.ChecksumNone)
	objName := trand.String(10)
	_, err = api.PutObject(api.PutArgs{
		BaseParams: unAuthBP,
		Bck:        bck,
		Reader:     r,
		Size:       fileSize,
		ObjName:    objName,
	})
	expectUnauthorized(t, err)

	r, _ = readers.NewRandReader(fileSize, cos.ChecksumNone)
	_, err = api.PutObject(api.PutArgs{
		BaseParams: authBP,
		Bck:        bck,
		Reader:     r,
		Size:       fileSize,
		ObjName:    objName,
	})
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to PUT %s\n", authBP.Token[:16], bck.Cname(objName))
}

func TestAuthBck(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiresAuth: true})
	var (
		unAuthBP, authBP = createBaseParams()
		bck              = cmn.Bck{Name: trand.String(10)}
	)
	err := api.CreateBucket(unAuthBP, bck, nil)
	expectUnauthorized(t, err)

	err = api.CreateBucket(authBP, bck, nil)
	tassert.CheckFatal(t, err)
	tlog.Logf("used token[%s...] to create %s\n", authBP.Token[:16], bck.String())

	p, err := api.HeadBucket(authBP, bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, p.Provider == apc.AIS, "expected provider %q, got %q", apc.AIS, p.Provider)

	defer func() {
		err := api.DestroyBucket(authBP, bck)
		tassert.CheckFatal(t, err)
		tlog.Logf("%s destroyed\n", bck.String())
	}()

	err = api.DestroyBucket(unAuthBP, bck)
	expectUnauthorized(t, err)
}
