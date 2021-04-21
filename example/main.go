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
	"flag"
	"fmt"
	"os"
	"time"
	//"log"
	//"github.com/spf13/pflag"
    "k8s.io/klog/v2"
//	logs "gomodules.xyz/x/log/golog"
	"github.com/gleez/leader-elector/election"
	//"github.com/spf13/cobra"
	apiv1 "k8s.io/api/core/v1"
	//v "gomodules.xyz/x/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
)

var (
	awaitElectionConfig *election.AwaitElection
	leaderElector *leaderelection.LeaderElector

	name  string
	id    string
	namespace string
	leaseDuration       time.Duration
	renewDeadline       time.Duration
	retryPeriod       time.Duration
	inCluster bool
	kubeconfig  string
	initialWait bool
)

func AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&name, "election", "", "The name of the election")
	fs.StringVar(&id, "id", hostname(), "The id of this participant")
	fs.StringVar(&namespace, "election-namespace", apiv1.NamespaceDefault, "The Kubernetes namespace for this election")
	fs.DurationVar(&leaseDuration, "lease-duration", 15*time.Second, "The leaseDuration for this election")
	fs.DurationVar(&renewDeadline, "renew-deadline", 10*time.Second, "The renewDeadline for this election")
	fs.DurationVar(&retryPeriod, "retry-period", 2*time.Second, "The retryPeriod for this election")
	fs.BoolVar(&inCluster, "use-cluster-credentials", false, "Should this request use cluster credentials?")
	fs.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	fs.BoolVar(&initialWait, "initial-wait", false, "wait for the old lease being expired if no leader exist.")
}

// func newCmd() *cobra.Command {
//     var cmd = &cobra.Command{
// 		Use:   "leader-elector",
// 		Short: "elect a leader to recieve the requests",
// 		Long: `elect a leader to recieve the requests`,
// 		Run: func(cmd *cobra.Command, args []string) {
// 			runElection()
// 		},
// 	}
// 	cmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

//     err := flag.CommandLine.Parse([]string{})
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	cmd.AddCommand(v.NewCmdVersion())

// 	AddFlags(cmd.Flags())

// 	return cmd
// }

func makeClient() (*kubernetes.Clientset, error) {
	var cfg *rest.Config
	var err error

	if inCluster {
		if cfg, err = rest.InClusterConfig(); err != nil {
			return nil, err
		}
	} else {
		if kubeconfig != "" {
			if cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig); err != nil {
				return nil, err
			}
		}
	}

	if cfg == nil {
		return nil, fmt.Errorf("k8 config is not set")
	}

	return kubernetes.NewForConfig(rest.AddUserAgent(cfg, "leader-election"))
}

func validateFlags() {
	klog.Infof("without v")

	klog.V(4).Infof("with v")
	if len(id) == 0 {
		klog.Fatalln("--id cannot be empty")
	}

	if len(name) == 0 {
		klog.Fatalln("--election cannot be empty")
	}

	if kubeconfig == "" && inCluster == false {
		klog.Fatalln("both --kubeconfig and --use-cluster-credentials cannot be empty")
	}
}

func runElection() {
	validateFlags()

	client, err := makeClient()
	if err != nil {
		klog.Fatalln("error connecting to the client: %v", err)
	}

	awaitElectionConfig, err := election.NewAwaitElectionConfig(leaseDuration, renewDeadline, retryPeriod)
	if err != nil {
		klog.Fatalln("failed to create runner: %v", err)
	}

	if initialWait {
		klog.Infoln("wait for the old lease being expired if no leader exist, duration(=lease-duration+renew-deadline)", (leaseDuration + renewDeadline/2).String())
		time.Sleep(leaseDuration + renewDeadline/2)
	}

	err = awaitElectionConfig.Run(client)
	if err != nil {
		klog.Fatalln("failed to run: %v", err)
	}
}

func main() {
	//flag.Set("v", "10")
	loggingFlags := flag.NewFlagSet("leader-elector", flag.ExitOnError)
	AddFlags(loggingFlags)
	klog.InitFlags(loggingFlags)
	flag.Set("v", "9")
	loggingFlags.Parse(os.Args[1:])
    //fmt.Println("%+v", loggingFlags)
	defer klog.Flush()
	
	runElection()
	//logs.InitLogs()
	//defer logs.FlushLogs()

	// if err := newCmd().Execute(); err != nil {
	// 	os.Exit(1)
	// }

	// os.Exit(0)
}

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}
