// Command azure-node-termination-handler drains a Kubernetes node when Azure
// publishes a Scheduled Event for the VM it runs on.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/jabicho79/azure-node-termination-handler/pkg/capi"
	"github.com/jabicho79/azure-node-termination-handler/pkg/config"
	"github.com/jabicho79/azure-node-termination-handler/pkg/decision"
	"github.com/jabicho79/azure-node-termination-handler/pkg/handler"
	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
	"github.com/jabicho79/azure-node-termination-handler/pkg/uptime"
	"github.com/jabicho79/azure-node-termination-handler/pkg/webhook"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()

	cfg, err := config.Parse(os.Args[1:], os.Getenv)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "azure-node-termination-handler: configure with flags or the matching upper-case env vars (e.g. --node-name / NODE_NAME).")
		os.Exit(0)
	}
	if err != nil {
		log.Error().Err(err).Msg("Invalid configuration")
		os.Exit(1)
	}
	if lvl, err := zerolog.ParseLevel(cfg.LogLevel); err != nil {
		log.Error().Err(err).Str("level", cfg.LogLevel).Msg("Invalid log level")
		os.Exit(1)
	} else {
		zerolog.SetGlobalLevel(lvl)
	}

	if err := run(cfg); err != nil {
		log.Error().Err(err).Msg("Agent failed")
		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("building in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building clientset: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	// Azure IMDS must not be reached through HTTP(S)_PROXY.
	imdsClient := imds.NewClient(cfg.IMDSEndpoint, &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{Proxy: nil},
	})

	vmName, err := resolveVMName(ctx, cfg, imdsClient)
	if err != nil {
		return err
	}
	log.Info().Str("node", cfg.NodeName).Str("vm", vmName).Msg("Starting azure-node-termination-handler")

	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)

	notifiers := observability.Multi{observability.NewEventRecorder(clientset, cfg.NodeName)}
	var wh *webhook.Webhook
	if cfg.WebhookURL != "" {
		wh, err = webhook.New(cfg.WebhookURL, cfg.WebhookTemplate, nil, metrics)
		if err != nil {
			return err
		}
		notifiers = append(notifiers, wh)
	}

	h := handler.New(handler.Deps{
		IMDS:     imdsClient,
		Nodes:    node.NewOps(clientset, cfg.DryRun),
		Machines: capi.NewMachineChecker(dyn),
		Notifier: notifiers,
		Metrics:  metrics,
		Now:      time.Now,
		Uptime:   uptime.FromFile(cfg.UptimeFile),
	}, handler.Options{
		NodeName:           cfg.NodeName,
		VMName:             vmName,
		Decision:           decisionSettings(cfg),
		DeleteEmptyDirData: cfg.DeleteEmptyDirData,
		IgnoreDaemonSets:   cfg.IgnoreDaemonSets,
		DryRun:             cfg.DryRun,
		EventGoneGrace:     cfg.EventGoneGrace,
	})

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	probesMux := http.NewServeMux()
	probesMux.HandleFunc("/healthz", healthz(h.Ready))
	servers := []*http.Server{
		{Addr: fmt.Sprintf(":%d", cfg.MetricsPort), Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second},
		{Addr: fmt.Sprintf(":%d", cfg.ProbesPort), Handler: probesMux, ReadHeaderTimeout: 5 * time.Second},
	}
	for _, s := range servers {
		go func() {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error().Err(err).Str("addr", s.Addr).Msg("HTTP server failed")
			}
		}()
	}

	err = h.Run(ctx, cfg.PollInterval)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if wh != nil {
		if werr := wh.Close(shutdownCtx); werr != nil {
			log.Warn().Err(werr).Msg("Webhook notifications not delivered before shutdown")
		}
	}
	for _, s := range servers {
		if serr := s.Shutdown(shutdownCtx); serr != nil {
			log.Warn().Err(serr).Str("addr", s.Addr).Msg("HTTP server shutdown")
		}
	}
	return err
}

// resolveVMName returns cfg.VMName, or the IMDS compute name, retried every
// PollInterval until it succeeds or ctx ends.
func resolveVMName(ctx context.Context, cfg config.Config, c *imds.Client) (string, error) {
	if cfg.VMName != "" {
		return cfg.VMName, nil
	}
	for {
		name, err := c.ComputeName(ctx)
		if err == nil {
			return name, nil
		}
		log.Warn().Err(err).Msg("Resolving VM name from IMDS failed; retrying")
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("resolving VM name: %w", ctx.Err())
		case <-time.After(cfg.PollInterval):
		}
	}
}

// healthz returns 200 when ready() is true and 503 otherwise.
func healthz(ready func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if ready() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func decisionSettings(cfg config.Config) decision.Settings {
	return decision.Settings{
		Enabled: map[imds.EventType]bool{
			imds.EventReboot:    cfg.EnableReboot,
			imds.EventRedeploy:  cfg.EnableRedeploy,
			imds.EventPreempt:   cfg.EnablePreempt,
			imds.EventTerminate: cfg.EnableTerminate,
		},
		AutoApprove:           cfg.AutoApprove,
		SafetyMargin:          cfg.DrainSafetyMargin,
		PreemptSafetyMargin:   cfg.PreemptSafetyMargin,
		MaxDrainTime:          cfg.MaxDrainTime,
		PodGracePeriod:        cfg.PodGracePeriod,
		PreemptPodGracePeriod: cfg.PreemptPodGracePeriod,
	}
}
