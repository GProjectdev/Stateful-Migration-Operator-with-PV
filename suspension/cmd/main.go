package main

import (
	"flag"
	"os"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/lehuannhatrang/stateful-migration-operator/suspension/internal"
)

// KARMADA_KUBECONFIG이 있으면 그걸로 karmada-apiserver에 붙는다.
func getConfig() *rest.Config {
	if kc := os.Getenv("KARMADA_KUBECONFIG"); kc != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kc)
		if err != nil {
			klog.Fatalf("failed to build config from KARMADA_KUBECONFIG=%s: %v", kc, err)
		}
		return cfg
	}
	return ctrl.GetConfigOrDie()
}

func main() {
	var (
		metricsAddr string
		healthAddr  string
		leaderElect bool
		interval    time.Duration
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Metrics bind address.")
	flag.StringVar(&healthAddr, "health-probe-bind-address", ":8081", "Health probe bind address.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election.")
	flag.DurationVar(&interval, "scan-interval", 5*time.Second, "Polling interval for scanning ResourceBindings.")
	flag.Parse()

	ctrl.SetLogger(klogr.New())

	cfg := getConfig()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: healthAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "suspension-controller.karmada.io",
	})
	if err != nil {
		klog.Fatalf("unable to start manager: %v", err)
	}

	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)

	poller := &internal.SuspensionPoller{
		KarmadaClient: mgr.GetClient(),
		ScanInterval:  interval,
	}

	if err := poller.SetupWithManager(mgr); err != nil {
		klog.Fatalf("unable to setup suspension poller: %v", err)
	}

	klog.Info("starting suspension-controller manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		klog.Fatalf("problem running manager: %v", err)
	}
}
