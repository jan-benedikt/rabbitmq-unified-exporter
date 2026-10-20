package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/collector"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mapping"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/scheduler"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/selfmetrics"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/server"
)

var version = "1.0.0"

func main() {
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	showVersion := flag.Bool("version", false, "print version and exit")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: stringsToLevel(*logLevel)}))

	cfg, targets, err := config.Load(*configPath)
	if err != nil {
		log.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	warnInsecureTLS(targets, log)
	log.Info("config loaded", "groups", len(cfg.Groups), "targets", len(targets), "version", version)

	metrics := selfmetrics.New(version)
	store := model.NewSnapshotStore(0)
	applyTargetMetas(metrics, store, targets)

	coll := collector.New(metrics, log)
	sched := scheduler.New(targets, coll, store, int64(cfg.Defaults.MaxConcurrency), metrics, log)

	schema, _ := mapping.LoadEmbedded("3.9.15")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched.Start(ctx)

	var srv *server.Server
	ready := func() bool {
		return srv != nil && srv.SchedulerRunning() && srv.TargetCount() > 0
	}
	srv = server.New(cfg, targets, store, sched, metrics, schema, ready, log)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	var cfgModTime time.Time
	if fi, err := os.Stat(*configPath); err == nil {
		cfgModTime = fi.ModTime()
	}

	for {
		select {
		case err := <-errCh:
			if err != nil && err.Error() != "http: Server closed" {
				log.Error("server error", "err", err)
				os.Exit(1)
			}
			return
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				log.Info("SIGHUP received, reloading config")
				newCfg, newTargets, err2 := config.Load(*configPath)
				if err2 != nil {
					log.Error("reload failed", "err", err2)
					metrics.SetConfigReload(false)
					continue
				}
				if _, mt, err := config.Reloader(*configPath, cfgModTime); err == nil {
					cfgModTime = mt
				}
				warnInsecureTLS(newTargets, log)

				sched.Stop()

				cfg = newCfg
				targets = newTargets
				applyTargetMetas(metrics, store, targets)
				keep := make(map[string]struct{}, len(targets))
				for _, t := range targets {
					keep[t.ID] = struct{}{}
				}
				store.RetainOnly(keep)
				coll.ResetHTTPClients()

				sched = scheduler.New(targets, coll, store, int64(cfg.Defaults.MaxConcurrency), metrics, log)
				sched.Start(ctx)
				srv.UpdateRuntime(cfg, targets, sched)
				metrics.SetConfigReload(true)
				log.Info("config reloaded", "targets", len(targets))
				continue
			}
			log.Info("shutting down", "signal", sig.String())
			cancel()
			sched.Stop()
			shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = srv.Shutdown(shCtx)
			shCancel()
			return
		}
	}
}

func applyTargetMetas(metrics *selfmetrics.Registry, store *model.SnapshotStore, targets []config.ResolvedTarget) {
	maxAge := time.Duration(0)
	metas := make([]selfmetrics.TargetMeta, 0, len(targets))
	for _, t := range targets {
		if t.MaxStaleness > maxAge {
			maxAge = t.MaxStaleness
		}
		instance := t.Labels["instance"]
		if instance == "" {
			instance = t.Name
		}
		metas = append(metas, selfmetrics.MetaFromLabels(t.ID, t.Group, instance, t.Labels))
	}
	metrics.SetConfiguredTargets(metas)
	store.SetMaxAge(maxAge)
}

func warnInsecureTLS(targets []config.ResolvedTarget, log *slog.Logger) {
	for _, t := range targets {
		if t.Prometheus.TLS.InsecureSkipVerify {
			log.Warn("insecure_skip_verify enabled for prometheus TLS (lab only)", "target", t.ID)
		}
		if t.Management.TLS.InsecureSkipVerify {
			log.Warn("insecure_skip_verify enabled for management TLS (lab only)", "target", t.ID)
		}
		if t.Probes.AMQPAuth.TLS.InsecureSkipVerify || t.Probes.DeliveryTest.TLS.InsecureSkipVerify {
			log.Warn("insecure_skip_verify enabled for AMQP TLS (lab only)", "target", t.ID)
		}
	}
}

func stringsToLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
