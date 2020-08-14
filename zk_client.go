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
	"context"
	"crypto/tls"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/naming"
	"time"
)

func NewZkClientServant(zkIpList string) *ZkClientServant {
	clientServant := &ZkClientServant{}
	clientServant.zkServant = NewZkServant(zkIpList)
	clientServant.serverList = make(map[string]*serverListWatcher)
	return clientServant
}

type ZkClientServant struct {
	zkServant    *ZkServant
	serverList 	map[string]*serverListWatcher	// key는 znodepath
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
	zk.DefaultLogger.Printf("[%s] initial server list : %v", znodePath, children)
	defaultRouteAddr := children[0]

	grpc.EnableTracing = false
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	// znode service path 에 따른 serverListWatcher 생성
	w := &serverListWatcher{
		defaultRouteAddr: defaultRouteAddr,
		serviceName: znodePath,
		addressSet: make(map[string]struct{}),
		update:   make(chan *naming.Update, 1),
		side:     make(chan int, 1),
		readDone: make(chan int),
	}

	var gConn *grpc.ClientConn
	switch mode {
	case TransportModePlain :
		gConn, err = grpc.DialContext(
			ctx,
			defaultRouteAddr,
			grpc.WithBlock(),
			// Deprecated: use WithDefaultServiceConfig and WithDisableServiceConfig
			//grpc.WithDefaultServiceConfig("aaa"),
			//grpc.WithDisableServiceConfig(),
			//grpc.WithBalancerName(roundrobin.Name),
			//grpc.WithBalancerName("aaa"),
			grpc.WithBalancer(grpc.RoundRobin(w)),
			grpc.WithInsecure())
	case TransportModeSsl :
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		creds := credentials.NewTLS(conf)
		gConn, err = grpc.DialContext(
			ctx,
			defaultRouteAddr,
			grpc.WithBlock(),
			grpc.WithBalancer(grpc.RoundRobin(w)),
			grpc.WithTransportCredentials(creds))
	default:
		return nil, fmt.Errorf("invalid transport mode : %v", mode)
	}

	if err == nil {
		z.serverList[znodePath] = w

		go func() {
			z.watchNode(znodePath, children, ch)
		} ()

		if len(children) > 1 {
			z.updateServerList(znodePath, children)
		}
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
		zk.DefaultLogger.Printf("[%s] stop watching node", znodePath)
	}()

	for {
		e := <-ch
		children, ch, err = z.zkServant.ChildrenW(znodePath)
		if err != nil {
			zk.DefaultLogger.Printf("[%s] zk error : %s", znodePath, err.Error())
			z.Connect(znodePath)
			return
		}

		if e.Type & zk.EventNodeChildrenChanged != zk.EventNodeChildrenChanged {
			continue
		}

		zk.DefaultLogger.Printf("[%s] updating server list : %v", znodePath, children)
		z.updateServerList(znodePath, children)
	}
}

func (z *ZkClientServant) updateServerList(znodePath string, children []string)	{
	var updates []*naming.Update

	w, ok := z.serverList[znodePath]
	if !ok {
		zk.DefaultLogger.Printf("[%s] not found server list", znodePath)
		return
	}

	updates = w.buildCreatedList(children, updates)
	updates = w.buildRemovedList(children, updates)
	w.inject(updates)
}

type serverListWatcher struct {
	// service name
	serviceName 	string
	// default routing (server) address
	defaultRouteAddr 	string
	// server address ip set
	addressSet	map[string]struct{}
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

func (w *serverListWatcher) Resolve(target string) (naming.Watcher, error) {
	w.side <- 1
	w.update <- &naming.Update{
		Op:   naming.Add,
		Addr: w.defaultRouteAddr,
	}
	w.addressSet[w.defaultRouteAddr] = struct{}{}
	go func() {
		<-w.readDone
	}()
	return w, nil
}

func (w *serverListWatcher) Close() {
}


func (w *serverListWatcher) inject(updates []*naming.Update) {
	w.side <- len(updates)
	for _, u := range updates {
		zk.DefaultLogger.Printf("[%s] service node updating %v : %v", w.serviceName, geUpdateOperationName(u.Op), u.Addr)
		w.update <- u
	}
	<-w.readDone
}

func (w *serverListWatcher) buildCreatedList(children []string, updates []*naming.Update) []*naming.Update {
	for _, server := range children {
		_, ok := w.addressSet[server]
		if !ok {
			updates = append(updates, &naming.Update{
				Op:   naming.Add,
				Addr: server,
			})
			w.addressSet[server] = struct{}{}
		}
	}

	return updates
}

func (w *serverListWatcher) buildRemovedList(children []string, updates []*naming.Update) []*naming.Update {
	copiedSet := map[string]struct{}{}
	for key, _ := range w.addressSet {
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
		delete(w.addressSet, server)
	}

	return updates
}


func geUpdateOperationName(op naming.Operation) string {
	if op == naming.Add {
		return "Add"
	}
	return "Delete"
}