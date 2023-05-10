// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// supported archive types (file extensions)
const (
	ExtTar    = ".tar"
	ExtTgz    = ".tgz"
	ExtTarTgz = ".tar.gz"
	ExtZip    = ".zip"

	// msgpack doesn't have a "common extension", see for instance:
	// * https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
	// however, there seems to be a de-facto agreement wrt Content-Type
	// * application/msgpack
	// * application/x-msgpack (<<< recommended)
	// * application/*+msgpack
	// AIS uses the following single constant for both the default file extension
	// and for the Content-Type (the latter with offset [1:])
	ExtMsgpack = ".msgpack"
)

const TarBlockSize = 512 // Size of each block in a tar stream

type ErrUnknownMime struct{ detail string }

var (
	ArchExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip, ExtMsgpack}

	ErrTarIsEmpty = errors.New("tar is empty")
)

// Map user-specified mime type OR the filename's extension to one of the supported ArchExtensions
// NOTE see also: `ais.mimeByMagic` (that reads the first 512 bytes and compares)
func Mime(mime, filename string) (ext string, err error) {
	// user-specified (intended) format takes precedence
	if mime != "" {
		return byMime(mime)
	}
	// otherwise, by filename extension
	for _, ext := range ArchExtensions {
		if strings.HasSuffix(filename, ext) {
			return ext, nil
		}
	}
	err = NewUnknownMimeError(filename)
	return
}

func byMime(mime string) (string, error) {
	if strings.Contains(mime, ExtTarTgz[1:]) { // ExtTarTgz contains ExtTar
		return ExtTarTgz, nil
	}
	for _, ext := range ArchExtensions {
		if strings.Contains(mime, ext[1:]) {
			return ext, nil
		}
	}
	return "", NewUnknownMimeError(mime)
}

// Exists for all ais-created/appended TARs - common code to set auxiliary bits in a header
// NOTE:
// - currently, not using os.Getuid/gid (or user.Current) to set Uid/Gid, and
// - not calling standard tar.FileInfoHeader(finfo-of-the-file-to-archive) as well
// - see also: /usr/local/go/src/archive/tar/common.go
func SetAuxTarHeader(hdr *tar.Header) {
	hdr.Mode = int64(PermRWR)
}

// OpenTarForAppend opens a TAR and uses tar's reader Next() to skip
// to the position right _after_ the last file in the TAR
// (padding bytes including).
//
// Background:
//
//	TAR file is padded with one or more 512-byte blocks of zero bytes.
//	The blocks must be overwritten, otherwise newly added files won't be
//	accessible. Different TAR formats (such as `ustar`, `pax` and `GNU`)
//	write different number of zero blocks.
func OpenTarForAppend(cname, workFQN string) (*os.File, error) {
	fh, err := os.OpenFile(workFQN, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	err = _seekTarEnd(cname, fh)
	if err != nil {
		fh.Close()
	}
	return fh, err
}

func _seekTarEnd(cname string, fh *os.File) error {
	var (
		size int64
		pos  = int64(-1)
		twr  = tar.NewReader(fh)
	)
	for {
		hdr, err := twr.Next()
		if err != nil {
			if err != io.EOF {
				return err
			}
			// EOF
			if pos < 0 {
				return ErrTarIsEmpty
			}
			break
		}
		pos, err = fh.Seek(0, io.SeekCurrent)
		if err != nil {
			debug.AssertNoErr(err) // unlikely
			return err
		}
		size = hdr.Size
	}
	if pos == 0 {
		return fmt.Errorf("failed to seek end of the TAR %s", cname)
	}
	padded := CeilAlignInt64(size, TarBlockSize)
	_, err := fh.Seek(pos+padded, io.SeekStart)
	return err
}

////////////////////
// ErrUnknownMime //
////////////////////

func NewUnknownMimeError(d string) *ErrUnknownMime {
	return &ErrUnknownMime{d}
}

func (e *ErrUnknownMime) Error() string {
	return "unknown mime type \"" + e.detail + "\""
}
