// Copyright 2020 Eryx <evorui аt gmail dοt com>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvfs

import (
	"crypto/sha256"
	"hash/crc32"
	"path/filepath"
	"strings"
)

const (
	pathHashSize = 16
)

type pathItem struct {
	path string
}

func newPathItem(path string) *pathItem {
	return &pathItem{
		path: pathFilter(path),
	}
}

func pathFilter(path string) string {
	return filepath.Clean("/" + path)
}

func pathName(path string) string {
	path = pathFilter(path)
	if n := strings.LastIndex(path, "/"); n >= 0 && (n+1) < len(path) {
		return path[n+1:]
	}
	return path
}

func (it *pathItem) dir() string {
	if n := strings.LastIndex(it.path, "/"); n >= 0 {
		return it.path[:n]
	}
	return it.path
}

func (it *pathItem) name() string {
	if n := strings.LastIndex(it.path, "/"); n >= 0 && (n+1) < len(it.path) {
		return it.path[n+1:]
	}
	return ""
}

func (it *pathItem) metaKey() []byte {
	if it.path == "" {
		it.path = "/"
	}
	var (
		m  = strings.Count(it.path, "/")
		n  = strings.LastIndex(it.path, "/")
		dk = sha256.Sum256([]byte(strings.ToLower(it.path[:n])))
		fk = sha256.Sum256([]byte(strings.ToLower(it.path[n+1:])))
	)
	dk[pathHashSize-1] = 0x00
	fk[pathHashSize-1] = 0x00
	return append(append([]byte{FileMeta, uint8(m)}, dk[:pathHashSize]...), fk[:pathHashSize]...)
}

func (it *pathItem) foldKey() []byte {
	var (
		m  = strings.Count(it.path, "/")
		dk [32]byte
	)
	if it.path == "/" {
		dk = sha256.Sum256([]byte(""))
	} else {
		m += 1
		dk = sha256.Sum256([]byte(strings.ToLower(it.path)))
	}
	dk[pathHashSize-1] = 0x00
	return append([]byte{FileMeta, uint8(m)}, dk[:pathHashSize]...)
}

const pathBlockChunkNum = uint32(kvFsBlockSize / kvFsChunkSize)

func pathDataChunkKey(inode uint64, chunkId uint32) []byte {

	hid := crc32.ChecksumIEEE(
		append(uint64ToBytes(inode), uint32ToBytes(chunkId/pathBlockChunkNum)...))

	return nsKeyEncode(FileData,
		uint32ToBytes(hid),
		uint64ToBytes(inode), uint32ToBytes(chunkId))
}
