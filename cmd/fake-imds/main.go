// Command fake-imds runs a fake Azure IMDS Scheduled Events server for e2e tests.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/jabicho79/azure-node-termination-handler/pkg/fakeimds"
)

func main() {
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	srv := &http.Server{Addr: ":8080", Handler: fakeimds.NewServer(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shCtx); err != nil {
			log.Error().Err(err).Msg("shutdown")
		}
	}()

	log.Info().Str("addr", srv.Addr).Msg("fake IMDS listening")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("serve")
	}
}
