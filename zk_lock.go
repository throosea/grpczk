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
 * @date 20. 3. 10. 오후 5:51
 */

package grpczk

import (
	"github.com/samuel/go-zookeeper/zk"
	"path/filepath"
)

type ZkLock struct {
	err			error
	zkServant    *ZkServant
	lockPath 	string
}

func (z ZkLock) Error()	error 	{
	return z.err
}


func (z *ZkLock) Close()	{
	if z.zkServant != nil && len(z.lockPath) > 0 {
		z.zkServant.deleteLockPath(z.lockPath)
	}
}

func (z *ZkServant) AcquireLock(znodeLock, key string)	ZkLock	{
	lockPath := filepath.Join(znodeLock, key)

	zk.DefaultLogger.Printf("AcquireLock : %s", lockPath)

	zkLock := ZkLock{}
	zkLock.lockPath = lockPath
	zkLock.zkServant = z

	for {
		err := z.createLockPath(lockPath)
		if err == nil {
			return zkLock
		}

		if err != zk.ErrNodeExists	{
			zkLock.err = err
			return zkLock
		}

		children, eventCh, err := z.ChildrenW(znodeLock)
		if err != nil {
			zkLock.err = err
			return zkLock
		}

		// check list has a key
		if !hasKeyInList(children, key)	{
			// 발견되지 않았다면 다시 생성을 시도한다
			continue
		}

		for {
			e := <-eventCh
			children, eventCh, err = z.ChildrenW(znodeLock)
			if err != nil {
				zkLock.err = err
				return zkLock
			}

			if e.Type & zk.EventNodeChildrenChanged != zk.EventNodeChildrenChanged {
				continue
			}

			// check list has a key
			if !hasKeyInList(children, key)	{
				// 발견되지 않았다면 다시 생성을 시도한다
				break
			}
		}
	}
}

func hasKeyInList(list []string, key string)	bool 	{
	for _, v := range list {
		if v == key {
			return true
		}
	}
	return false
}

