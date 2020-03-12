/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @project grpczk
 * @author dave
 * @date 20. 3. 12. 오후 7:14
 */

package grpczk

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	zkIpList = "172.21.106.191"
	countFile = "/tmp/golang_lock_count"
	znodeBaseDir = "/godmusic/lock/artist_like"
	znodeKey = "30006413"
)

func TestZkLock(t *testing.T) {
	os.Remove(countFile)

	loopCount := 10
	threadCount := 10

	wg := sync.WaitGroup{}
	wg.Add(threadCount)

	for i:=0; i<threadCount; i++	{
		go startZkLockTest(i, &wg, loopCount)
	}

	wg.Wait()

	if readCount() != loopCount * threadCount	{
		t.Fatalf("mismatch result count")
	}
}

func startZkLockTest(id int, wg *sync.WaitGroup, loop int)	{
	defer wg.Done()

	zkServant := NewZkServant(zkIpList)
	err := zkServant.Connect()
	if err != nil {
		log.Printf("fail to connect zk : %s", err.Error())
		return
	}

	znodePath := filepath.Join(znodeBaseDir, znodeKey)

	log.Printf("znodePath:[%s]\n", znodePath)

	//mustSleep := time.Duration(60 - time.Now().Second())
	//log.Printf("[%d] wait %d seconds", id, mustSleep)
	//time.Sleep(time.Second * mustSleep)

	for i:=0; i<loop; i++	{
		createNodeWithLock(id, zkServant)
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(10) + 1))
	}
}


func createNodeWithLock(id int, zkServant *ZkServant)	{
	zkLock := zkServant.AcquireLock(znodeBaseDir, znodeKey)
	log.Printf("[%d] do my job...", id)
	if zkLock.Error() != nil {
		log.Printf("[%d] zklock error : %s", id, zkLock.Error().Error())
		return
	}
	defer zkLock.Close()

	doMyCustomJob(id)
}


func doMyCustomJob(id int)	{
	dat, err := ioutil.ReadFile(countFile)
	if err != nil {
		if os.IsNotExist(err)	{
			dat = []byte("1")
			err = ioutil.WriteFile(countFile, dat, 0644)
			if err != nil {
				log.Printf("fail to write initial file : %s", err.Error())
			}
			log.Printf("create new one...")
			return
		}
		log.Printf("ReadFile error : %s", err.Error())
		return
	}

	v, err := strconv.Atoi(string(dat))
	if err != nil {
		log.Printf("atoi conv fail [%s] : %s", string(dat), err.Error())
		return
	}

	dat = []byte(fmt.Sprintf("%d", v + 1))
	err = ioutil.WriteFile(countFile, dat, 0644)
	if err != nil {
		log.Printf("fail to write file : %s", err.Error())
		return
	}

	log.Printf("[%d] flush [%s] : %s", id, countFile, string(dat))
}

func readCount()	int 	{
	dat, err := ioutil.ReadFile(countFile)
	if err != nil {
		return -1
	}

	v, err := strconv.Atoi(string(dat))
	if err != nil {
		return -1
	}

	return v
}