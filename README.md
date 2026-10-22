# azure-node-termination-handler

A Kubernetes DaemonSet that watches the Azure Instance Metadata Service for Scheduled Events (reboot, redeploy, preempt, terminate) and gracefully cordons and drains the node before Azure acts on it. It needs no Azure credentials or SDK.

See the [design spec](docs/superpowers/specs/2026-10-22-azure-node-termination-handler-design.md) for details.
