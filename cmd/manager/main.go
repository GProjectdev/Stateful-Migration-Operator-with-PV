package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"time"

	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/artifact"
	checkpoint "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/checkpoint"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/ctrlapi"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/kubelet"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/management"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/member"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/suspension"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func validateOptions(mode, kubeconfig, cluster, node string) error {
	switch mode {
	case "management":
		if kubeconfig == "" {
			return fmt.Errorf("management requires explicit --kubeconfig for Karmada")
		}
	case "member", "artifact", "checkpoint":
		if kubeconfig != "" {
			return fmt.Errorf("member modes use in-cluster credentials only; --kubeconfig is forbidden")
		}
	default:
		return fmt.Errorf("--mode must be management, checkpoint, member or artifact")
	}
	if (mode == "member" || mode == "artifact") && cluster == "" {
		return fmt.Errorf("--cluster-name is required")
	}
	if mode == "artifact" && node == "" {
		return fmt.Errorf("NODE_NAME or --node-name is required")
	}
	return nil
}
func main() {
	var mode, cluster, node, root, certDir, probe, ca string
	var leader, insecure bool
	flag.StringVar(&mode, "mode", "", "management, checkpoint, member or artifact")
	flag.StringVar(&cluster, "cluster-name", "", "Karmada member name")
	flag.StringVar(&node, "node-name", os.Getenv("NODE_NAME"), "local node name (artifact mode)")
	flag.StringVar(&root, "archive-root", "/host-checkpoints", "read-only mounted archive root")
	flag.StringVar(&certDir, "webhook-cert-dir", "/etc/webhook", "TLS certificate directory")
	flag.StringVar(&probe, "health-probe-bind-address", ":8081", "health address")
	flag.StringVar(&ca, "kubelet-ca-file", "", "CA file for kubelet serving certificates")
	flag.BoolVar(&insecure, "kubelet-insecure-skip-tls-verify", false, "LAB ONLY: disable kubelet TLS verification")
	flag.BoolVar(&leader, "leader-elect", true, "enable leader election")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	fatal := func(err error) { ctrl.Log.Error(err, "Manager failed"); os.Exit(1) }
	configPath := flag.Lookup("kubeconfig").Value.String()
	if err := validateOptions(mode, configPath, cluster, node); err != nil {
		fatal(err)
	}
	var cfg *rest.Config
	var err error
	if mode == "management" {
		cfg, err = clientcmd.BuildConfigFromFlags("", configPath)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		fatal(err)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, api.AddToScheme, fluidcr.AddToScheme} {
		if err = add(scheme); err != nil {
			fatal(err)
		}
	}
	options := ctrl.Options{Scheme: scheme, Metrics: metrics.Options{BindAddress: "0"}, HealthProbeBindAddress: probe, LeaderElection: leader && mode != "artifact", LeaderElectionNamespace: "stateful-migration-system", LeaderElectionID: "stateful-" + mode + ".migration.dcnlab.com"}
	if mode == "member" {
		options.WebhookServer = webhook.NewServer(webhook.Options{Port: 9443, CertDir: certDir, TLSOpts: []func(*tls.Config){func(c *tls.Config) { c.MinVersion = tls.VersionTLS12; c.NextProtos = []string{"http/1.1"} }}})
	}
	mgr, err := ctrl.NewManager(cfg, options)
	if err != nil {
		fatal(err)
	}
	switch mode {
	case "management":
		err = (&management.RestoreReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), PollInterval: 5 * time.Second}).SetupWithManager(mgr)
		if err == nil {
			err = (&suspension.Reconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader()}).SetupWithManager(mgr)
		}
	case "member":
		err = member.NewReconciler(mgr.GetClient(), mgr.GetAPIReader(), cluster).SetupWithManager(mgr)
		mgr.GetWebhookServer().Register("/mutate-restore", &admission.Webhook{Handler: member.NewWebhook(mgr.GetAPIReader(), cluster)})
		mgr.GetWebhookServer().Register("/validate-restore", &admission.Webhook{Handler: member.NewValidator(mgr.GetAPIReader(), cluster)})
	case "artifact":
		verifier := artifact.NewVerifier(mgr.GetClient(), mgr.GetAPIReader(), cluster, root)
		verifier.NodeName = node
		err = verifier.SetupWithManager(mgr)
	case "checkpoint":
		kc, e := kubelet.NewClientWithOptions(kubelet.Options{CAFile: ca, InsecureSkipVerify: insecure})
		if e != nil {
			fatal(e)
		}
		err = (&checkpoint.FluidCRMigrationReconciler{Client: mgr.GetClient(), Scheme: scheme, CtrlClient: ctrlapi.NewClient(), KubeletClient: kc}).SetupWithManager(mgr)
	}
	if err != nil {
		fatal(err)
	}
	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		fatal(err)
	}
	ready := healthz.Ping
	if mode == "member" {
		ready = mgr.GetWebhookServer().StartedChecker()
	}
	if err = mgr.AddReadyzCheck("readyz", ready); err != nil {
		fatal(err)
	}
	if err = mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		fatal(err)
	}
}
