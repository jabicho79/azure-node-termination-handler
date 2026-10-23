// Package config parses the agent configuration from flags and environment.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Config holds the agent configuration. A flag overrides its env var, and the
// env var overrides the default.
type Config struct {
	NodeName              string // --node-name, NODE_NAME (required)
	VMName                string // --vm-name, VM_NAME (overrides IMDS compute name)
	IMDSEndpoint          string
	PollInterval          time.Duration
	EnableReboot          bool
	EnableRedeploy        bool
	EnablePreempt         bool
	EnableTerminate       bool
	AutoApprove           bool
	DrainSafetyMargin     time.Duration
	MaxDrainTime          time.Duration
	PreemptSafetyMargin   time.Duration
	PodGracePeriod        int // seconds, -1 means the pod's own
	PreemptPodGracePeriod int
	DeleteEmptyDirData    bool
	IgnoreDaemonSets      bool
	EventGoneGrace        time.Duration
	WebhookURL            string
	WebhookTemplate       string // "" means webhook.DefaultTemplate
	MetricsPort           int
	ProbesPort            int
	UptimeFile            string
	DryRun                bool
	LogLevel              string
	KubeAPIQPS            float32 // client-go rate limit, requests per second
	KubeAPIBurst          int
}

// flagToEnv maps a flag name to its env var: uppercase, '-' to '_'.
func flagToEnv(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

// Parse builds a Config from args and getenv (e.g. os.Getenv).
func Parse(args []string, getenv func(string) string) (Config, error) {
	var c Config
	var errs []error
	fs := flag.NewFlagSet("azure-node-termination-handler", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	// lookup returns the env value for a flag name, if set.
	lookup := func(name string) (string, string, bool) {
		ev := flagToEnv(name)
		v := getenv(ev)
		return ev, v, v != ""
	}
	str := func(p *string, name, def string) {
		if _, v, ok := lookup(name); ok {
			def = v
		}
		fs.StringVar(p, name, def, "")
	}
	boolean := func(p *bool, name string, def bool) {
		if ev, v, ok := lookup(name); ok {
			b, err := strconv.ParseBool(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid %s=%q: %w", ev, v, err))
			} else {
				def = b
			}
		}
		fs.BoolVar(p, name, def, "")
	}
	integer := func(p *int, name string, def int) {
		if ev, v, ok := lookup(name); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid %s=%q: %w", ev, v, err))
			} else {
				def = n
			}
		}
		fs.IntVar(p, name, def, "")
	}
	float := func(p *float64, name string, def float64) {
		if ev, v, ok := lookup(name); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid %s=%q: %w", ev, v, err))
			} else {
				def = f
			}
		}
		fs.Float64Var(p, name, def, "")
	}
	duration := func(p *time.Duration, name string, def time.Duration) {
		if ev, v, ok := lookup(name); ok {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid %s=%q: %w", ev, v, err))
			} else {
				def = d
			}
		}
		fs.DurationVar(p, name, def, "")
	}

	str(&c.NodeName, "node-name", "")
	str(&c.VMName, "vm-name", "")
	str(&c.IMDSEndpoint, "imds-endpoint", "http://169.254.169.254")
	duration(&c.PollInterval, "poll-interval", 5*time.Second)
	boolean(&c.EnableReboot, "enable-reboot", true)
	boolean(&c.EnableRedeploy, "enable-redeploy", true)
	boolean(&c.EnablePreempt, "enable-preempt", true)
	boolean(&c.EnableTerminate, "enable-terminate", true)
	boolean(&c.AutoApprove, "auto-approve", true)
	duration(&c.DrainSafetyMargin, "drain-safety-margin", 60*time.Second)
	duration(&c.MaxDrainTime, "max-drain-time", 60*time.Minute)
	duration(&c.PreemptSafetyMargin, "preempt-safety-margin", 5*time.Second)
	integer(&c.PodGracePeriod, "pod-grace-period", -1)
	integer(&c.PreemptPodGracePeriod, "preempt-pod-grace-period", 20)
	boolean(&c.DeleteEmptyDirData, "delete-emptydir-data", true)
	boolean(&c.IgnoreDaemonSets, "ignore-daemonsets", true)
	duration(&c.EventGoneGrace, "event-gone-grace", 5*time.Minute)
	str(&c.WebhookURL, "webhook-url", "")
	str(&c.WebhookTemplate, "webhook-template", "")
	integer(&c.MetricsPort, "metrics-port", 9092)
	integer(&c.ProbesPort, "probes-port", 8080)
	str(&c.UptimeFile, "uptime-file", "/proc/uptime")
	boolean(&c.DryRun, "dry-run", false)
	str(&c.LogLevel, "log-level", "info")
	var qps float64
	float(&qps, "kube-api-qps", 50)
	integer(&c.KubeAPIBurst, "kube-api-burst", 100)

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	c.KubeAPIQPS = float32(qps)
	if c.KubeAPIQPS <= 0 || c.KubeAPIBurst <= 0 {
		return Config{}, fmt.Errorf("kube-api-qps and kube-api-burst must be positive, got %v and %d", c.KubeAPIQPS, c.KubeAPIBurst)
	}
	if c.NodeName == "" {
		return Config{}, errors.New("node-name is required (--node-name or NODE_NAME)")
	}
	for name, d := range map[string]time.Duration{
		"poll-interval":         c.PollInterval,
		"drain-safety-margin":   c.DrainSafetyMargin,
		"max-drain-time":        c.MaxDrainTime,
		"preempt-safety-margin": c.PreemptSafetyMargin,
		"event-gone-grace":      c.EventGoneGrace,
	} {
		if d <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %s", name, d)
		}
	}
	return c, nil
}
