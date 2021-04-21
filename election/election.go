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
	"time"
	"fmt"
	"net"
	"net/http"
	"encoding/json"
    "github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/errors"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

const (
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
	LeaseDuration    time.Duration
	RenewDeadline    time.Duration
	RetryPeriod      time.Duration
	ServicePorts     []apiv1.EndpointPort
}

type ConfigError struct {
	missingEnv string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config: missing required environment variable: '%s'", e.missingEnv)
}

func NewAwaitElectionConfig(leaseDuration time.Duration, renewDeadline time.Duration, retryPeriod time.Duration) (*AwaitElection, error) {
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
		LeaseDuration:    leaseDuration,
		RenewDeadline:    renewDeadline,
		RetryPeriod:      retryPeriod,
	}, nil
}

func (el *AwaitElection) Run(c *kubernetes.Clientset) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// result of the setServiceEndpoint command will be send over this channel
	execResult := make(chan error)

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(c.CoreV1().RESTClient()).Events("")})

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	// Create lock for leader election using provided settings
	lock := resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      el.LockName,
			Namespace: el.LockNamespace,
		},
		Client: c.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: el.LeaderIdentity,
			EventRecorder: broadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "leader-elector", Host: hostname}),
		},
	}

	leaderCfg := leaderelection.LeaderElectionConfig{
		Lock:            &lock,
		Name:            el.Name,
		ReleaseOnCancel: true,
		// Suggested default values
		LeaseDuration: el.LeaseDuration, //15 * time.Second,
		RenewDeadline: el.RenewDeadline, //10 * time.Second,
		RetryPeriod:   el.RetryPeriod, //2 * time.Second,
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
				glog.Infoln("long live our new leader: '%s'!", identity)
			},
			OnStoppedLeading: func() {
				glog.Infoln("lost leader status")
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
		glog.Infoln("no status endpoint specified, will not be created")
		return statusServerResult
	}

	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/", func(res http.ResponseWriter, request *http.Request) {
		err := elector.Check(2 * time.Second)
		if err != nil {
			glog.Errorf("Failed to step down gracefully, reporting unhealthy status: %v", err)
			res.WriteHeader(http.StatusInternalServerError)
			message := fmt.Sprintf("Lease has expired. Error: %v", err)
			_, err:= res.Write([]byte(message))
			if err != nil {
				glog.Errorf("Failed to serve request. Error: %v", err)
			}
			return
		}
	
		_, err = res.Write([]byte("{\"status\": \"ok\"}"))
		if err != nil {
			glog.Errorf("Failed to serve request. Error: %v", err)
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

