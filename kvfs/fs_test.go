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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lynkdb/kvgo"
	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

var (
	dbTestMu sync.Mutex
)

func dbOpen(name string) (kv2.ClientTable, error) {

	dbTestMu.Lock()
	defer dbTestMu.Unlock()

	test_dir := fmt.Sprintf("/dev/shm/kvgo-db/%s", name)

	if _, err := exec.Command("rm", "-rf", test_dir).Output(); err != nil {
		return nil, err
	}

	cfg := kvgo.NewConfig(test_dir)

	db, err := kvgo.Open(cfg)
	if err != nil {
		return nil, err
	}

	return db.OpenTable("main"), nil
}

func Test_FsShell(t *testing.T) {

	tName := fmt.Sprintf("fs-shell-%d", time.Now().Unix())

	db, err := dbOpen(tName)
	if err != nil {
		t.Fatalf("fail %v", err)
	}

	mp := fmt.Sprintf("/tmp/kvfs-test/%s", tName)

	_, err = exec.Command("mkdir", "-p", mp).Output()
	if err != nil {
		t.Fatalf("fail %v", err)
	}

	fss, err := Setup(&Config{
		MountPoint: mp,
	}, db)
	if err != nil {
		t.Fatalf("fail %v", err)
	}
	defer fss.Close()

	if _, err := exec.Command("mkdir", mp+"/abc/cd").Output(); err == nil {
		t.Fatalf("fail")
	}

	if _, err := exec.Command("mkdir", "-p", mp+"/abc/cd").Output(); err != nil {
		t.Fatalf("fail %v", err)
	}

	if _, err := exec.Command("ls", "-l", mp+"/abc/cd").Output(); err != nil {
		t.Fatalf("fail %v", err)
	}

	if _, err := exec.Command("ls", "-l", mp+"/abc/cd-no").Output(); err == nil {
		t.Fatalf("fail")
	}

	if goroot, ok := os.LookupEnv("GOROOT"); ok && len(goroot) > 1 {

		testPath := goroot + "/pkg/"
		syncPath := mp + "/goroot/"

		tn := time.Now()
		t.Logf("rsync %s to %s", testPath, syncPath)
		if out, err := exec.Command("rsync", "-av", testPath, syncPath).Output(); err != nil {
			t.Fatalf("fail %v, out %s", err, string(out))
		}
		t.Logf("rsync in %v", time.Since(tn))

		out, err := exec.Command("find", testPath, "-type", "f").Output()
		if err != nil {
			t.Fatalf("fail (%s) %v", testPath, err)
		}

		files := strings.Split(strings.TrimSpace(string(out)), "\n")

		sumCheck := func(path string) string {
			if out, err := exec.Command("md5sum", path).Output(); err == nil {
				sc := strings.TrimSpace(string(out))
				if len(sc) > 32 {
					return sc[:32]
				}
			}
			return ""
		}

		for i, v := range files {

			t.Logf("%8d  %s", i, syncPath+strings.TrimPrefix(v, testPath))

			sc0 := sumCheck(v)
			sc1 := sumCheck(syncPath + strings.TrimPrefix(v, testPath))

			if sc0 == sc1 {
				continue
			}

			t.Fatalf("fail sumcheck %s", v)
		}
	}
}
