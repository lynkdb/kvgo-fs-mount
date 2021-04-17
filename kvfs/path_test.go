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
	"testing"
)

func Test_PathFilter(t *testing.T) {

	if v := pathFilter(""); v != "/" {
		t.Fatalf("fail %s", v)
	}

	if v := pathFilter("."); v != "/" {
		t.Fatalf("fail %s", v)
	}

	if v := pathFilter("./"); v != "/" {
		t.Fatalf("fail %s", v)
	}

	if v := pathFilter(".."); v != "/" {
		t.Fatalf("fail %s", v)
	}

	if v := pathFilter("../"); v != "/" {
		t.Fatalf("fail %s", v)
	}

	if v := pathFilter("a"); v != "/a" {
		t.Fatalf("fail %s", v)
	}

	if v := pathFilter("/a/b/"); v != "/a/b" {
		t.Fatalf("fail %s", v)
	}
}

func Test_PathName(t *testing.T) {

	if v := pathName("/a/b"); v != "b" {
		t.Fatalf("fail %s", v)
	}

	if v := pathName("/a/b/"); v != "b" {
		t.Fatalf("fail %s", v)
	}
}

func Test_PathItem(t *testing.T) {

	for _, tv := range []string{
		"/",
		"/a/b/",
		"/a/b/c",
	} {

		v := newPathItem(tv)
		t.Logf("tree %s, fold %s, name %s",
			tv, v.dir(), v.name())
	}
}
