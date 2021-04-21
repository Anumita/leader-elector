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

package election

import (
	"context"
	"os"
//	"sync"
	"time"
	"fmt"
	"net"
	"net/http"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
//	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
//	"k8s.io/client-go/kubernetes/scheme"
//	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
//	"k8s.io/client-go/tools/record"
//	"k8s.io/klog"
)

const (
	startBackoff = time.Second
	maxBackoff   = time.Minute
	AwaitElectionNameKey           = "ELECTION_NAME"
	AwaitElectionLockNameKey       = "LOCK_NAME"
	AwaitElectionLockNamespaceKey  = "LOCK_NAMESPACE"
	AwaitElectionIdentityKey       = "IDENTITY"
	AwaitElectionStatusEndpointKey = "STATUS_ENDPOINT"
	AwaitElectionPodIP             = "POD_IP"
	AwaitElectionNodeName          = "NODE_NAME"
	AwaitElectionServiceName       = "SERVICE_NAME"
	AwaitElectionServiceNamespace  = "SERVICE_NAMESPACE"
	AwaitElectionServicePortsJson  = "SERVICE_PORTS_JSON"
)

var log = logrus.New()

type AwaitElection struct {
	Name             string
	LockName         string
	LockNamespace    string
	LeaderIdentity   string
	StatusEndpoint   string
	ServiceName      string
	ServiceNamespace string
	PodIP            string
	NodeName         *string
	ServicePorts     []apiv1.EndpointPort
}

type ConfigError struct {
	missingEnv string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config: missing required environment variable: '%s'", e.missingEnv)
}

func NewAwaitElectionConfig() (*AwaitElection, error) {
	name := os.Getenv(AwaitElectionNameKey)
	if name == "" {
		return nil, &ConfigError{missingEnv: AwaitElectionNameKey}
	}

	lockName := os.Getenv(AwaitElectionLockNameKey)
	if lockName == "" {
		return nil, &ConfigError{missingEnv: AwaitElectionLockNameKey}
	}

	lockNamespace := os.Getenv(AwaitElectionLockNamespaceKey)
	if lockNamespace == "" {
		return nil, &ConfigError{missingEnv: AwaitElectionLockNamespaceKey}
	}

	leaderIdentity := os.Getenv(AwaitElectionIdentityKey)
	if leaderIdentity == "" {
		return nil, &ConfigError{missingEnv: AwaitElectionIdentityKey}
	}

	// Optional
	statusEndpoint := os.Getenv(AwaitElectionStatusEndpointKey)
	podIP := os.Getenv(AwaitElectionPodIP)
	var nodeName *string
	if val, ok := os.LookupEnv(AwaitElectionNodeName); ok {
		nodeName = &val
	}

	serviceName := os.Getenv(AwaitElectionServiceName)
	serviceNamespace := os.Getenv(AwaitElectionServiceNamespace)

	servicePortsJson := os.Getenv(AwaitElectionServicePortsJson)
	var servicePorts []apiv1.EndpointPort
	err := json.Unmarshal([]byte(servicePortsJson), &servicePorts)
	if serviceName != "" && err != nil {
		return nil, fmt.Errorf("failed to parse ports from env: %w", err)
	}

	return &AwaitElection{
		Name:             name,
		LockName:         lockName,
		LockNamespace:    lockNamespace,
		LeaderIdentity:   leaderIdentity,
		StatusEndpoint:   statusEndpoint,
		PodIP:            podIP,
		NodeName:         nodeName,
		ServiceName:      serviceName,
		ServiceNamespace: serviceNamespace,
		ServicePorts:     servicePorts,
	}, nil
}


// NewSimpleElection creates an election, it defaults namespace to 'default' and ttl to 10s
//func (el *AwaitElection) NewSimpleElection(c *kubernetes.Clientset) (*leaderelection.LeaderElector, error) {
//	return el.NewElection(10*time.Second, c)
//}

func (el *AwaitElection) Run(ttl time.Duration, c *kubernetes.Clientset) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

//	// Not running with leader election/kubernetes context, just run the provided function
//	if !el.WithElection {
//		log.Info("not running with leader election")
//		return el.LeaderExec(ctx)
//	}

	// Create kubernetes client
//	kubeCfg, err := rest.InClusterConfig()
//	if err != nil {
//		return fmt.Errorf("failed to read cluster config: %w", err)
//	}
//
//	kubeClient, err := kubernetes.NewForConfig(kubeCfg)
//	if err != nil {
//		return fmt.Errorf("failed to create KubeClient for config: %w", err)
//	}

	// result of the LeaderExec(ctx) command will be send over this channel
	execResult := make(chan error)

	// Create lock for leader election using provided settings
	lock := resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      el.LockName,
			Namespace: el.LockNamespace,
		},
		Client: c.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: el.LeaderIdentity,
		},
	}

	leaderCfg := leaderelection.LeaderElectionConfig{
		Lock:            &lock,
		Name:            el.Name,
		ReleaseOnCancel: true,
		// Suggested default values
		LeaseDuration: ttl, //15 * time.Second,
		RenewDeadline: ttl / 2, //10 * time.Second,
		RetryPeriod:   ttl / 4, //2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// First we need to register our pod as the service endpoint
				err := el.setServiceEndpoint(ctx, c)
				if err != nil {
					execResult <- err
					return
				}
				// actual start the command here.
				// Note: this callback is started in a goroutine, so we can block this
				// execution path for as long as we want.
			},
			OnNewLeader: func(identity string) {
				log.Infof("long live our new leader: '%s'!", identity)
			},
			OnStoppedLeading: func() {
				log.Info("lost leader status")
			},
		},
	}
	elector, err := leaderelection.NewLeaderElector(leaderCfg)
	if err != nil {
		return fmt.Errorf("failed to create leader elector: %w", err)
	}

	statusServerResult := el.startStatusEndpoint(ctx, elector)

	go elector.Run(ctx)

	// the different end conditions:
	// 1. context was cancelled -> error
	// 2. command executed -> either error or nil, depending on return value
	// 3. status endpoint failed -> could not create status endpoint
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-execResult:
		return r
	case r := <-statusServerResult:
		return r
	}
}

// NewElection creates an election.  'namespace'/'election' should be an existing Kubernetes Service
// 'id' is the id if this leader, should be unique.
//func (el *AwaitElection) NewElection(ttl time.Duration, c *kubernetes.Clientset) (*leaderelection.LeaderElector, error) {
//
//	broadcaster := record.NewBroadcaster()
//	broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(c.CoreV1().RESTClient()).Events("")})
//
//	hostname, err := os.Hostname()
//	if err != nil {
//		return nil, err
//	}
//
//	// we use the Lease lock type since edits to Leases are less common
//	// and fewer objects in the cluster watch "all Leases".
//	lock := &resourcelock.LeaseLock{
//		LeaseMeta: metav1.ObjectMeta{
//			Name:      el.LockName,
//			Namespace: el.LockNamespace,
//		},
//		Client: c.CoordinationV1(),
//		LockConfig: resourcelock.ResourceLockConfig{
//			Identity:      el.LeaderIdentity,
//			EventRecorder: broadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "leader-elector", Host: hostname}),
//		},
//	}
//
//	execResult := make(chan error)
//
//	wg := &sync.WaitGroup{}
//	defer wg.Wait()
//
//	config := leaderelection.LeaderElectionConfig{
//		Lock: lock,
//		// IMPORTANT: you MUST ensure that any code you have that
//		// is protected by the lease must terminate **before**
//		// you call cancel. Otherwise, you could have a background
//		// loop still running and another process could
//		// get elected before your background loop finished, violating
//		// the stated goal of the lease.
//		ReleaseOnCancel: true,
//		LeaseDuration:   ttl,
//		RenewDeadline:   ttl / 2,
//		RetryPeriod:     ttl / 4,
//		Callbacks: leaderelection.LeaderCallbacks{
//			OnStartedLeading: func(ctx context.Context) {
//
//
//				// we're notified when we start - this is where you would
//				// usually put your code
//				err := el.setServiceEndpoint(ctx, c)
//				if err != nil {
//					execResult <- err
//					return
//				}
//
//				klog.Infof("endpoint created")
//
//				// leave()
//			},
//			OnStoppedLeading: func() {
//				// we can do cleanup here
//				klog.Infof("leader lost: %s",  el.LeaderIdentity)
//				// os.Exit(0)
//				// empty string means leader is unknown
//				
//			},
//			OnNewLeader: func(identity string) {
//				// we're notified when new leader elected
//				if identity ==  el.LeaderIdentity {
//					// I just got the lock
//					return
//				}
//				klog.Infof("may the force be with our new leader: %s", identity)
//			},
//		},
//	}
//
//        r := <-execResult
//	if r != nil {
//	    klog.Infof("erroror %v", r)
//	    return nil, r
//	}
//
//
//	return leaderelection.NewLeaderElector(config)
//}

func (el *AwaitElection) setServiceEndpoint(ctx context.Context, client *kubernetes.Clientset) error {
	if el.ServiceName == "" {
		return nil
	}

	endpoints := &apiv1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      el.ServiceName,
			Namespace: el.ServiceNamespace,
		},
		Subsets: []apiv1.EndpointSubset{
			{
				Addresses: []apiv1.EndpointAddress{{IP: el.PodIP, NodeName: el.NodeName}},
				Ports:     el.ServicePorts,
			},
		},
	}
	_, err := client.CoreV1().Endpoints(el.ServiceNamespace).Create(endpoints)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}

		_, err := client.CoreV1().Endpoints(el.ServiceNamespace).Update(endpoints)
		return err
	}

	return nil
}

func (el *AwaitElection) startStatusEndpoint(ctx context.Context, elector *leaderelection.LeaderElector) <-chan error {
	statusServerResult := make(chan error)

	if el.StatusEndpoint == "" {
		log.Info("no status endpoint specified, will not be created")
		return statusServerResult
	}

	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		err := elector.Check(2 * time.Second)
		if err != nil {
			log.WithField("err", err).Error("failed to step down gracefully, reporting unhealthy status")
			writer.WriteHeader(500)
			_, err := writer.Write([]byte("{\"status\": \"expired\"}"))
			if err != nil {
				log.WithField("err", err).Error("failed to serve request")
			}
			return
		}

		_, err = writer.Write([]byte("{\"status\": \"ok\"}"))
		if err != nil {
			log.WithField("err", err).Error("failed to serve request")
		}
	})

	statusServer := http.Server{
		Addr:    el.StatusEndpoint,
		Handler: serveMux,
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
	}
	go func() {
		statusServerResult <- statusServer.ListenAndServe()
	}()
	return statusServerResult
}

// RunElection runs an election given an leader elector.  Doesn't return.
//func (el *AwaitElection) RunElection(ctx context.Context, e *leaderelection.LeaderElector) {
//	wait.UntilWithContext(ctx, e.Run, 0)
//}
