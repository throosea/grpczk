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
	"google.golang.org/grpc/naming"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
	"crypto/tls"
	"google.golang.org/grpc/credentials"
)

func NewZkClientServant(zkIpList string) *ZkClientServant {
	clientServant := &ZkClientServant{}
	clientServant.addressSet = map[string]struct{}{}
	clientServant.zkServant = NewZkServant(zkIpList)
	clientServant.w = &serverListWatcher{
		update:   make(chan *naming.Update, 1),
		side:     make(chan int, 1),
		readDone: make(chan int),
	}
	return clientServant
}

type ZkClientServant struct {
	zkServant    *ZkServant
	w    		*serverListWatcher
	addressSet	map[string]struct{}
	addr 		string
}

func (z *ZkClientServant) SetLogger(logger zk.Logger) *ZkClientServant {
	z.zkServant.SetLogger(logger)
	return z
}

func (z *ZkClientServant) Connect(znodePath string) (*grpc.ClientConn, error) {
	err := z.zkServant.Connect()
	if err != nil {
		return nil, err
	}

	mode, err := z.zkServant.GetTransportMode(znodePath)
	if err != nil {
		return nil, err
	}
	if mode == TransportModeUnknown {
		return nil, fmt.Errorf("unknown transport mode in service description : %s", znodePath)
	}

	children, ch, err := z.zkServant.ChildrenW(znodePath)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("there is no server")
	}
	zk.DefaultLogger.Printf("initial server list : %v", children)
	z.addr = children[0]

	go func() {
		z.watchNode(znodePath, children, ch)
	} ()

	grpc.EnableTracing = false

	var gConn *grpc.ClientConn
	switch mode {
	case TransportModePlain :
		gConn, err = grpc.Dial(
			z.addr,
			grpc.WithBlock(),
			grpc.WithBalancer(grpc.RoundRobin(z)),
			grpc.WithInsecure())
	case TransportModeSsl :
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		creds := credentials.NewTLS(conf)
		gConn, err = grpc.Dial(
			z.addr,
			grpc.WithBlock(),
			grpc.WithBalancer(grpc.RoundRobin(z)),
			grpc.WithTransportCredentials(creds))
	default:
		return nil, fmt.Errorf("invalid transport mode : %v", mode)
	}

	if len(children) > 1 {
		z.updateServerList(children)
	}

	return gConn, err
}

func (z *ZkClientServant) GetData(znodePath string) ([]byte, error) {
	return z.zkServant.GetData(znodePath)
}

func (z *ZkClientServant) SetData(znodePath string, data []byte) error {
	if data == nil {
		return nil
	}

	return z.zkServant.SetData(znodePath, data)
}

func (z *ZkClientServant) watchNode(znodePath string, children []string, ch <-chan zk.Event) {
	var err error

	defer func() {
		zk.DefaultLogger.Printf("stop watching node")
	}()

	for {
		e := <-ch
		children, ch, err = z.zkServant.ChildrenW(znodePath)
		if err != nil {
			zk.DefaultLogger.Printf("zk error : %s", err.Error())
			z.Connect(znodePath)
			return
		}

		if e.Type & zk.EventNodeChildrenChanged != zk.EventNodeChildrenChanged {
			continue
		}

		zk.DefaultLogger.Printf("updating server list : %v", children)
		z.updateServerList(children)
	}
}

func (z *ZkClientServant) updateServerList(children []string)	{
	var updates []*naming.Update
	updates = z.buildCreatedList(children, updates)
	updates = z.buildRemovedList(children, updates)
	z.w.inject(updates)
}

func (z *ZkClientServant) buildRemovedList(children []string, updates []*naming.Update) []*naming.Update {
	copiedSet := map[string]struct{}{}
	for key, _ := range z.addressSet {
		copiedSet[key] = struct{}{}
	}

	for _, server := range children {
		delete(copiedSet, server)
	}

	for server := range copiedSet {
		updates = append(updates, &naming.Update{
			Op:   naming.Delete,
			Addr: server,
		})
		delete(z.addressSet, server)
	}

	return updates
}


func (z *ZkClientServant) buildCreatedList(children []string, updates []*naming.Update) []*naming.Update {
	for _, server := range children {
		_, ok := z.addressSet[server]
		if !ok {
			updates = append(updates, &naming.Update{
				Op:   naming.Add,
				Addr: server,
			})
			z.addressSet[server] = struct{}{}
		}
	}

	return updates
}

func (z *ZkClientServant) Resolve(target string) (naming.Watcher, error) {
	z.w.side <- 1
	z.w.update <- &naming.Update{
		Op:   naming.Add,
		Addr: z.addr,
	}
	z.addressSet[z.addr] = struct{}{}
	go func() {
		<-z.w.readDone
	}()
	return z.w, nil
}

type serverListWatcher struct {
	// the channel to receives name resolution updates
	update chan *naming.Update
	// the side channel to get to know how many updates in a batch
	side chan int
	// the channel to notifiy update injector that the update reading is done
	readDone chan int
}

func (w *serverListWatcher) Next() (updates []*naming.Update, err error) {
	n := <-w.side
	if n == 0 {
		return nil, fmt.Errorf("w.side is closed")
	}
	for i := 0; i < n; i++ {
		u := <-w.update
		if u != nil {
			updates = append(updates, u)
		}
	}
	w.readDone <- 0
	return
}

func (w *serverListWatcher) Close() {
}


func (w *serverListWatcher) inject(updates []*naming.Update) {
	w.side <- len(updates)
	for _, u := range updates {
		zk.DefaultLogger.Printf("service node updating %v : %v", geUpdateOperationName(u.Op), u.Addr)
		w.update <- u
	}
	<-w.readDone
}

func geUpdateOperationName(op naming.Operation) string {
	if op == naming.Add {
		return "Add"
	}
	return "Delete"
}