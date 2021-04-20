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
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
)

const (
	startBackoff = time.Second
	maxBackoff   = time.Minute
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
	ServicePorts     []corev1.EndpointPort
}

func NewAwaitElectionConfig() (*AwaitElection, error) {
	name := os.Getenv(consts.AwaitElectionNameKey)
	if name == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionNameKey}
	}

	lockName := os.Getenv(consts.AwaitElectionLockNameKey)
	if lockName == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionLockNameKey}
	}

	lockNamespace := os.Getenv(consts.AwaitElectionLockNamespaceKey)
	if lockNamespace == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionLockNamespaceKey}
	}

	leaderIdentity := os.Getenv(consts.AwaitElectionIdentityKey)
	if leaderIdentity == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionIdentityKey}
	}

	// Optional
	statusEndpoint := os.Getenv(consts.AwaitElectionStatusEndpointKey)
	podIP := os.Getenv(consts.AwaitElectionPodIP)
	var nodeName *string
	if val, ok := os.LookupEnv(consts.AwaitElectionNodeName); ok {
		nodeName = &val
	}

	serviceName := os.Getenv(consts.AwaitElectionServiceName)
	serviceNamespace := os.Getenv(consts.AwaitElectionServiceNamespace)

	servicePortsJson := os.Getenv(consts.AwaitElectionServicePortsJson)
	var servicePorts []corev1.EndpointPort
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
func (el *AwaitElection) NewSimpleElection(callback func(leader string), c *kubernetes.Clientset) (*leaderelection.LeaderElector, error) {
	return NewElection(10*time.Second, callback, c)
}

// NewElection creates an election.  'namespace'/'election' should be an existing Kubernetes Service
// 'id' is the id if this leader, should be unique.
func (el *AwaitElection) NewElection(ttl time.Duration, callback func(leader string), c *kubernetes.Clientset) (*leaderelection.LeaderElector, error) {

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(c.CoreV1().RESTClient()).Events("")})

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	// we use the Lease lock type since edits to Leases are less common
	// and fewer objects in the cluster watch "all Leases".
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      el.LockName,
			Namespace: el.LockNamespace,
		},
		Client: c.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      el.LeaderIdentity,
			EventRecorder: broadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "leader-elector", Host: hostname}),
		},
	}

	execResult := make(chan error)

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	config := leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		ReleaseOnCancel: true,
		LeaseDuration:   ttl,
		RenewDeadline:   ttl / 2,
		RetryPeriod:     ttl / 4,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				wg.Add(1)
				defer wg.Done()

				// we're notified when we start - this is where you would
				// usually put your code
				err := el.setServiceEndpoint(ctx, c)
				if err != nil {
					execResult <- err
					return
				}

				callback(id)
				// leave()
			},
			OnStoppedLeading: func() {
				// we can do cleanup here
				klog.Infof("leader lost: %s", id)
				// os.Exit(0)
				// empty string means leader is unknown
				callback("")
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if identity == id {
					// I just got the lock
					return
				}
				klog.Infof("new leader elected: %s", identity)
				callback(identity)
			},
		},
	}

	return leaderelection.NewLeaderElector(config)
}

func (el *AwaitElection) setServiceEndpoint(ctx context.Context, client *kubernetes.Clientset) error {
	if el.ServiceName == "" {
		return nil
	}

	endpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      el.ServiceName,
			Namespace: el.ServiceNamespace,
		},
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{{IP: el.PodIP, NodeName: el.NodeName}},
				Ports:     el.ServicePorts,
			},
		},
	}
	_, err := client.CoreV1().Endpoints(el.ServiceNamespace).Create(ctx, endpoints, metav1.CreateOptions{})
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}

		_, err := client.CoreV1().Endpoints(el.ServiceNamespace).Update(ctx, endpoints, metav1.UpdateOptions{})
		return err
	}

	return nil
}

// RunElection runs an election given an leader elector.  Doesn't return.
func (el *AwaitElection) RunElection(ctx context.Context, e *leaderelection.LeaderElector) {
	wait.UntilWithContext(ctx, e.Run, 0)
}
