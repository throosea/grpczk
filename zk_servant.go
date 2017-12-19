//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @project throosea.com
// @author DeockJin Chung (jin.freestyle@gmail.com)
// @date 2017. 10. 1. PM 7:42
//

package grpczk

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"sync"
	"strings"
	"encoding/json"
	"fmt"
)


// logging preference delivery mode
const (
	TransportModeUnknown = iota
	TransportModePlain
	TransportModeSsl
)

type TransportMode int

const (
	zkVersionAll        = -1
	ServiceKeyTransport = "transport"
)

type ZkServant struct {
	ipList		[]string
	zkConn 		*zk.Conn
	mutex		sync.Mutex
	cond		*sync.Cond
	state		zk.State
	pathSet		map[string]struct{}
}

func NewZkServant(zkIpList string) *ZkServant {
	servant := ZkServant{}
	servant.ipList = parseIpList(zkIpList)
	servant.mutex = sync.Mutex{}
	servant.cond = sync.NewCond(&servant.mutex)
	servant.state = zk.StateDisconnected
	servant.pathSet = map[string]struct{}{}
	return &servant
}

func (z *ZkServant) SetLogger(logger zk.Logger) *ZkServant {
	zk.DefaultLogger = logger
	return z
}

func (z *ZkServant) Connect() error {
	if z.zkConn != nil {
		return nil
	}

	conn, eventChan, err := zk.Connect(z.ipList, time.Second)
	if err != nil {
		return err
	}
	z.zkConn = conn

	go func() {
		for elem := range eventChan {
			if !z.processZkEvent(elem) {
				return
			}
		}
	} ()

	return nil
}


func (z *ZkServant) processZkEvent(event zk.Event) bool	{
	zk.DefaultLogger.Printf("zk event : %v", event)

	if event.Type != zk.EventSession {
		return true
	}

	lastState := z.state
	z.state = event.State

	z.cond.Broadcast()

	switch event.State {
	case zk.StateHasSession :
		zk.DefaultLogger.Printf("zk has session")
		if lastState == zk.StateDisconnected {
			z.reCreateAllEphemerals()
		}
	case zk.StateConnected :
		zk.DefaultLogger.Printf("zk connected")
	case zk.StateDisconnected :
		zk.DefaultLogger.Printf("zk disconnected")
	case zk.StateExpired :
		zk.DefaultLogger.Printf("zk expired")
		z.zkConn.Close()
		z.zkConn = nil
		z.Connect()
		return false
	}

	return true
}

func (z *ZkServant) reCreateAllEphemerals() 	{
	for k, _ := range z.pathSet {
		err := z.createEphemeralOnce(k)
		if err != nil {
			zk.DefaultLogger.Printf("zk createEphemeralOnce fail : [%s] %s", k, err.Error())
		}
	}
}

func (z *ZkServant) ensureZkEnabled() {
	if z.state == zk.StateHasSession {
		return
	}

	z.mutex.Lock()
	defer z.mutex.Unlock()
	for z.state != zk.StateHasSession {
		z.cond.Wait()
	}
}

func (z *ZkServant) CreateEphemeral(path string) error {
	zk.DefaultLogger.Printf("CreateEphemeral : %s", path)
	z.ensureZkEnabled()

	z.pathSet[path] = struct{}{}
	acl := zk.WorldACL(zk.PermAll)
	for {
		_, err := z.zkConn.Create(path, nil, zk.FlagEphemeral, acl)
		if err != nil {
			zk.DefaultLogger.Printf("zk fail to createEphemeral : [%s] %s", path, err.Error())
			time.Sleep(time.Second * 5)
			continue
		}
		break
	}

	zk.DefaultLogger.Printf("zk create ephemeral path : %s", path)
	return nil
}


func (z *ZkServant) createEphemeralOnce(path string) error {
	zk.DefaultLogger.Printf("createEphemeralOnce : %s", path)
	z.ensureZkEnabled()

	z.pathSet[path] = struct{}{}
	acl := zk.WorldACL(zk.PermAll)
	_, err := z.zkConn.Create(path, nil, zk.FlagEphemeral, acl)
	if err != nil {
		return err
	}

	zk.DefaultLogger.Printf("zk create ephemeral path : %s", path)
	return nil
}

func (z *ZkServant) SetData(path string, data []byte) error {
	zk.DefaultLogger.Printf("zk setData [%s] : %d bytes", path, len(data))
	z.ensureZkEnabled()
	_, err := z.zkConn.Set(path, data, zkVersionAll)
	if err == zk.ErrNoNode {
		zk.DefaultLogger.Printf("[%s] node does not exist", path)
	}
	return err
}

func (z *ZkServant) GetTransportMode(path string) (TransportMode, error) {
	zk.DefaultLogger.Printf("GetTransportMode : %s", path)
	b, err := z.GetData(path)
	if err != nil {
		return TransportModeUnknown, err
	}

	var data map[string]string
	err = json.Unmarshal(b, &data)
	if err != nil {
		return TransportModeUnknown, err
	}

	// e.g {"transport":"h2c"}
	mode, ok := data[ServiceKeyTransport]
	if !ok {
		return TransportModeUnknown, fmt.Errorf("Invalid service description. not found transport key")
	}

	switch(strings.ToLower(mode))	{
	case "h2c" :
		return TransportModePlain, nil
	case "h2" :
		return TransportModeSsl, nil
	}

	return TransportModeUnknown, nil
}
func (z *ZkServant) GetData(path string) ([]byte, error) {
	zk.DefaultLogger.Printf("zk GetData : %s", path)
	z.ensureZkEnabled()
	data, _, err := z.zkConn.Get(path)
	return data, err
}

func (z *ZkServant) ChildrenW(path string) ([]string, <-chan zk.Event, error) {
	zk.DefaultLogger.Printf("zk ChildrenW : %s", path)
	z.ensureZkEnabled()
	data, _, ch, err := z.zkConn.ChildrenW(path)
	return data, ch, err
}

func (z *ZkServant) Delete(path string) error {
	zk.DefaultLogger.Printf("zk deleteNode [%s]", path)
	return z.zkConn.Delete(path, zkVersionAll)
}


func parseIpList(config string) []string {
	items := strings.Split(config, ",")

	list := make([]string, 0)
	for _, v := range items {
		list = append(list, strings.Trim(v, " "))
	}

	return list
}
