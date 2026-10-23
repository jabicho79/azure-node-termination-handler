package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParseDefaults(t *testing.T) {
	c, err := Parse(nil, env(map[string]string{"NODE_NAME": "n1"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeName != "n1" {
		t.Errorf("NodeName=%q", c.NodeName)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"PollInterval", c.PollInterval, 5 * time.Second},
		{"DrainSafetyMargin", c.DrainSafetyMargin, 60 * time.Second},
		{"MaxDrainTime", c.MaxDrainTime, 60 * time.Minute},
		{"PreemptSafetyMargin", c.PreemptSafetyMargin, 5 * time.Second},
		{"PreemptPodGracePeriod", c.PreemptPodGracePeriod, 20},
		{"PodGracePeriod", c.PodGracePeriod, -1},
		{"EventGoneGrace", c.EventGoneGrace, 5 * time.Minute},
		{"AutoApprove", c.AutoApprove, true},
		{"EnableReboot", c.EnableReboot, true},
		{"EnableRedeploy", c.EnableRedeploy, true},
		{"EnablePreempt", c.EnablePreempt, true},
		{"EnableTerminate", c.EnableTerminate, true},
		{"DeleteEmptyDirData", c.DeleteEmptyDirData, true},
		{"IgnoreDaemonSets", c.IgnoreDaemonSets, true},
		{"IMDSEndpoint", c.IMDSEndpoint, "http://169.254.169.254"},
		{"MetricsPort", c.MetricsPort, 9092},
		{"ProbesPort", c.ProbesPort, 8080},
		{"UptimeFile", c.UptimeFile, "/proc/uptime"},
		{"DryRun", c.DryRun, false},
		{"LogLevel", c.LogLevel, "info"},
		{"VMName", c.VMName, ""},
		{"WebhookURL", c.WebhookURL, ""},
		{"WebhookTemplate", c.WebhookTemplate, ""},
		{"KubeAPIQPS", c.KubeAPIQPS, float32(50)},
		{"KubeAPIBurst", c.KubeAPIBurst, 100},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, ch.got, ch.want)
		}
	}
}

func TestParseEnvOverride(t *testing.T) {
	c, err := Parse(nil, env(map[string]string{"NODE_NAME": "n1", "POLL_INTERVAL": "2s", "AUTO_APPROVE": "false"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.PollInterval != 2*time.Second {
		t.Errorf("PollInterval=%v", c.PollInterval)
	}
	if c.AutoApprove {
		t.Error("AutoApprove should be false")
	}
}

func TestParseFlagBeatsEnv(t *testing.T) {
	c, err := Parse([]string{"--poll-interval=3s"}, env(map[string]string{"NODE_NAME": "n1", "POLL_INTERVAL": "2s"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.PollInterval != 3*time.Second {
		t.Errorf("PollInterval=%v", c.PollInterval)
	}
}

func TestParseNodeNameFromFlag(t *testing.T) {
	c, err := Parse([]string{"--node-name=n2"}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeName != "n2" {
		t.Errorf("NodeName=%q", c.NodeName)
	}
}

func TestParseRequiresNodeName(t *testing.T) {
	_, err := Parse(nil, env(nil))
	if err == nil || !strings.Contains(err.Error(), "node-name") {
		t.Fatalf("err=%v", err)
	}
}

func TestParseRejectsNonPositiveDurations(t *testing.T) {
	_, err := Parse(nil, env(map[string]string{"NODE_NAME": "n1", "POLL_INTERVAL": "0s"}))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseInvalidEnvNamesVariable(t *testing.T) {
	for _, kv := range [][2]string{{"AUTO_APPROVE", "maybe"}, {"POLL_INTERVAL", "soon"}, {"METRICS_PORT", "x"}} {
		_, err := Parse(nil, env(map[string]string{"NODE_NAME": "n1", kv[0]: kv[1]}))
		if err == nil || !strings.Contains(err.Error(), kv[0]) {
			t.Errorf("%s: err=%v", kv[0], err)
		}
	}
}

func TestParseKubeAPIRateLimits(t *testing.T) {
	c, err := Parse([]string{"--kube-api-burst=40"}, env(map[string]string{"NODE_NAME": "n1", "KUBE_API_QPS": "20.5", "KUBE_API_BURST": "30"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.KubeAPIQPS != 20.5 || c.KubeAPIBurst != 40 {
		t.Errorf("KubeAPIQPS=%v KubeAPIBurst=%v, want 20.5 and 40", c.KubeAPIQPS, c.KubeAPIBurst)
	}
	for _, kv := range [][2]string{{"KUBE_API_QPS", "fast"}, {"KUBE_API_QPS", "0"}, {"KUBE_API_BURST", "0"}} {
		if _, err := Parse(nil, env(map[string]string{"NODE_NAME": "n1", kv[0]: kv[1]})); err == nil {
			t.Errorf("%s=%s: expected error", kv[0], kv[1])
		}
	}
}
