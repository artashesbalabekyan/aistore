// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package apitests

import (
	"testing"

	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

func BenchmarkActionMsgMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		msg := apc.ActMsg{
			Name:   "test-name",
			Action: apc.ActDeleteObjects,
			Value:  &apc.ListRange{Template: "thisisatemplate"},
		}
		data, err := jsoniter.Marshal(&msg)
		if err != nil {
			b.Errorf("marshaling errored: %v", err)
		}
		msg2 := &apc.ActMsg{}
		err = jsoniter.Unmarshal(data, &msg2)
		if err != nil {
			b.Errorf("unmarshaling errored: %v", err)
		}
		err = cos.MorphMarshal(msg2.Value, &apc.ListRange{})
		if err != nil {
			b.Errorf("morph unmarshal errored: %v", err)
		}
	}
}
