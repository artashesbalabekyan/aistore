// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// CreateArchMultiObj allows to archive multiple objects.
// The option to append multiple objects to an existing archive is also supported.
// The source and the destination buckets are defined as `fromBck` and `toBck`, respectively
// (not necessarily distinct)
// For supported archiving formats, see `cos.ArchExtensions`.
//
// See also: api.AppendToArch
func CreateArchMultiObj(bp BaseParams, fromBck cmn.Bck, msg cmn.ArchiveMsg) (string, error) {
	bp.Method = http.MethodPut
	q := fromBck.AddToQuery(nil)
	return dolr(bp, fromBck, apc.ActArchive, msg, q)
}

// `fltPresence` applies exclusively to remote `fromBck` (is ignored if the source is ais://)
// and is one of: { apc.FltExists, apc.FltPresent, ... } - for complete enum, see api/apc/query.go

func CopyMultiObj(bp BaseParams, fromBck cmn.Bck, msg cmn.TCObjsMsg, fltPresence ...int) (xid string, err error) {
	bp.Method = http.MethodPost
	q := fromBck.AddToQuery(nil)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	return dolr(bp, fromBck, apc.ActCopyObjects, msg, q)
}

func ETLMultiObj(bp BaseParams, fromBck cmn.Bck, msg cmn.TCObjsMsg, fltPresence ...int) (xid string, err error) {
	bp.Method = http.MethodPost
	q := fromBck.AddToQuery(nil)
	if len(fltPresence) > 0 {
		q.Set(apc.QparamFltPresence, strconv.Itoa(fltPresence[0]))
	}
	return dolr(bp, fromBck, apc.ActETLObjects, msg, q)
}

// DeleteList sends request to remove a list of objects from a bucket.
func DeleteList(bp BaseParams, bck cmn.Bck, filesList []string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := cmn.SelectObjsMsg{ObjNames: filesList}
	return dolr(bp, bck, apc.ActDeleteObjects, msg, q)
}

// DeleteRange sends request to remove a range of objects from a bucket.
func DeleteRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := cmn.SelectObjsMsg{Template: rng}
	return dolr(bp, bck, apc.ActDeleteObjects, msg, q)
}

// EvictList sends request to evict a list of objects from a remote bucket.
func EvictList(bp BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := cmn.SelectObjsMsg{ObjNames: fileslist}
	return dolr(bp, bck, apc.ActEvictObjects, msg, q)
}

// EvictRange sends request to evict a range of objects from a remote bucket.
func EvictRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	bp.Method = http.MethodDelete
	q := bck.AddToQuery(nil)
	msg := cmn.SelectObjsMsg{Template: rng}
	return dolr(bp, bck, apc.ActEvictObjects, msg, q)
}

// PrefetchList sends request to prefetch a list of objects from a remote bucket.
func PrefetchList(bp BaseParams, bck cmn.Bck, fileslist []string) (string, error) {
	bp.Method = http.MethodPost
	q := bck.AddToQuery(nil)
	msg := cmn.SelectObjsMsg{ObjNames: fileslist}
	return dolr(bp, bck, apc.ActPrefetchObjects, msg, q)
}

// PrefetchRange sends request to prefetch a range of objects from a remote bucket.
func PrefetchRange(bp BaseParams, bck cmn.Bck, rng string) (string, error) {
	bp.Method = http.MethodPost
	q := bck.AddToQuery(nil)
	msg := cmn.SelectObjsMsg{Template: rng}
	return dolr(bp, bck, apc.ActPrefetchObjects, msg, q)
}

// multi-object list-range (delete, prefetch, evict, archive, copy, and etl)
func dolr(bp BaseParams, bck cmn.Bck, action string, msg any, q url.Values) (xid string, err error) {
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathBuckets.Join(bck.Name)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: action, Value: msg})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return
}
