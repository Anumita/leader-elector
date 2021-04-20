/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gleez/leader-elector/election"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

var (

	// LDFLAGS should overwrite these variables in build time.
	Version  string
	Revision string

	name        = flag.String("election", "", "The name of the election")
	id          = flag.String("id", hostname(), "The id of this participant")
	namespace   = flag.String("election-namespace", apiv1.NamespaceDefault, "The Kubernetes namespace for this election")
	ttl         = flag.Duration("ttl", 10*time.Second, "The TTL for this election")
	inCluster   = flag.Bool("use-cluster-credentials", false, "Should this request use cluster credentials?")
	kubeconfig  = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	addr        = flag.String("http", "", "If non-empty, stand up a simple webserver that reports the leader state")
	initialWait = flag.Bool("initial-wait", false, "wait for the old lease being expired if no leader exist.")
	versionFlag = flag.Bool("version", false, "display version and exit")

	leader = &LeaderData{}
)

func makeClient() (*kubernetes.Clientset, error) {
	var cfg *rest.Config
	var err error

	if *inCluster {
		if cfg, err = rest.InClusterConfig(); err != nil {
			return nil, err
		}
	} else {
		if *kubeconfig != "" {
			if cfg, err = clientcmd.BuildConfigFromFlags("", *kubeconfig); err != nil {
				return nil, err
			}
		}
	}

	if cfg == nil {
		return nil, fmt.Errorf("k8 config is not set")
	}

	return kubernetes.NewForConfig(rest.AddUserAgent(cfg, "leader-election"))
}

// LeaderData represents information about the current leader
type LeaderData struct {
	Name string `json:"name"`
}

func webHandler(res http.ResponseWriter, req *http.Request) {
	data, err := json.Marshal(leader)
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		res.Write([]byte(err.Error()))
		return
	}

	res.WriteHeader(http.StatusOK)
	res.Write(data)
}

func validateFlags() {
	if len(*id) == 0 {
		klog.Fatal("--id cannot be empty")
	}

	if len(*name) == 0 {
		klog.Fatal("--election cannot be empty")
	}

	if *kubeconfig == "" && *inCluster == false {
		klog.Fatal("both --kubeconfig and --use-cluster-credentials cannot be empty")
	}
}

func main() {
	flag.Parse()

	if *versionFlag {
		fmt.Printf("leader-elector version=%s revision=%s\n", Version, Revision)
		os.Exit(0)
	}

	validateFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := makeClient()
	if err != nil {
		klog.Fatalf("error connecting to the client: %v", err)
	}

	// listen for interrupts or the Linux SIGTERM signal and cancel
	// our context, which the leader election code will observe and
	// step down
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		klog.Info("Received termination, signaling shutdown")
		cancel()
	}()

	fn := func(str string) {
		leader.Name = str
		klog.Infof("%s is the leader", leader.Name)
	}

	awaitElectionConfig, err := NewAwaitElectionConfig(Execute)
	if err != nil {
		klog.Fatal("failed to create runner: %v", err)
	}

	e, err := awaitElectionConfig.NewElection(*ttl, fn, client)
	if err != nil {
		klog.Fatal("failed to create election: %v", err)
	}

	if *initialWait {
		klog.Info("wait for the old lease being expired if no leader exist, duration(=lease-duration+renew-deadline)", (*ttl + *ttl/2).String())
		time.Sleep(*ttl + *ttl/2)
	}

	go awaitElectionConfig.RunElection(ctx, e)

	if len(*addr) > 0 {
		klog.Infof("http server starting at %s", *addr)
		http.HandleFunc("/", webHandler)
		klog.Fatal(http.ListenAndServe(*addr, nil))
	} else {
		select {}
	}
}

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}
