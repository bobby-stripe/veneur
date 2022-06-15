package proxy

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stripe/veneur/v14/discovery"
	"github.com/stripe/veneur/v14/forwardrpc"
	"github.com/stripe/veneur/v14/proxy/destinations"
	"github.com/stripe/veneur/v14/proxy/grpcstats"
	"github.com/stripe/veneur/v14/proxy/handlers"
	"github.com/stripe/veneur/v14/scopedstatsd"
	"google.golang.org/grpc"
)

type CreateParams struct {
	Config       *Config
	Destinations destinations.Destinations
	Discoverer   discovery.Discoverer
	HttpHandler  *http.ServeMux
	Logger       *logrus.Entry
	Statsd       scopedstatsd.Client
}

type Proxy struct {
	destinations      destinations.Destinations
	dialTimeout       time.Duration
	discoverer        discovery.Discoverer
	discoveryInterval time.Duration
	forwardAddresses  []string
	forwardService    string
	grpcAddress       string
	grpcListener      net.Listener
	grpcServer        *grpc.Server
	handlers          *handlers.Handlers
	httpAddress       string
	httpListener      net.Listener
	httpServer        http.Server
	logger            *logrus.Entry
	shutdownTimeout   time.Duration
	statsd            scopedstatsd.Client
}

// Creates a new proxy server.
func Create(params *CreateParams) *Proxy {
	proxy := &Proxy{
		destinations:      params.Destinations,
		dialTimeout:       params.Config.DialTimeout,
		discoverer:        params.Discoverer,
		discoveryInterval: params.Config.DiscoveryInterval,
		forwardAddresses:  params.Config.ForwardAddresses,
		forwardService:    params.Config.ForwardService,
		grpcAddress:       params.Config.GrpcAddress,
		grpcServer: grpc.NewServer(grpc.StatsHandler(&grpcstats.StatsHandler{
			IsClient: false,
			Statsd:   params.Statsd,
		})),
		handlers: &handlers.Handlers{
			Destinations: params.Destinations,
			IgnoreTags:   params.Config.IgnoreTags,
			Logger:       params.Logger,
			Statsd:       params.Statsd,
		},
		httpAddress: params.Config.HttpAddress,
		httpServer: http.Server{
			Handler: params.HttpHandler,
		},
		logger:          params.Logger,
		shutdownTimeout: params.Config.ShutdownTimeout,
		statsd:          params.Statsd,
	}

	params.HttpHandler.HandleFunc(
		"/healthcheck", proxy.handlers.HandleHealthcheck)
	params.HttpHandler.HandleFunc("/import", proxy.handlers.HandleJsonMetrics)
	forwardrpc.RegisterForwardServer(proxy.grpcServer, proxy.handlers)

	return proxy
}

// Starts discovery, and the HTTP and gRPC servers. This method stops polling
// discovery and shuts down the servers and returns when the provided context is
// cancelled or if either server stops due to an error. This method waits for
// the servers to shut down gracefully, but shuts them down immediately if this
// takes more than `shutdownTimeout`.
func (proxy *Proxy) Start(ctx context.Context) error {
	// Listen on HTTP address
	httpListener, err := net.Listen("tcp", proxy.httpAddress)
	if err != nil {
		return err
	}
	defer httpListener.Close()
	proxy.httpListener = httpListener

	// Listen on gRPC address
	grpcListener, err := net.Listen("tcp", proxy.grpcAddress)
	if err != nil {
		return err
	}
	defer grpcListener.Close()
	proxy.grpcListener = grpcListener

	// Add static destinations
	proxy.destinations.Add(ctx, proxy.forwardAddresses)

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Poll discovery for available destinations.
	if proxy.forwardService != "" {
		go proxy.pollDiscovery(ctx)
	}

	// Start HTTP server
	httpExit := make(chan error, 1)
	wg.Add(1)
	go func() {
		proxy.logger.WithField("address", proxy.GetHttpAddress()).
			Debug("serving http")
		err := proxy.httpServer.Serve(httpListener)
		if err != http.ErrServerClosed {
			proxy.logger.Errorf("http server error: %v", err)
		}
		httpExit <- err
		cancel()
		wg.Done()
	}()

	// Handle HTTP shutdown
	httpErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		select {
		case err := <-httpExit:
			httpErr <- err
			break
		case <-ctx.Done():
			proxy.logger.Debug("shutting down http server")
			ctx, shutdownCancel :=
				context.WithTimeout(context.Background(), proxy.shutdownTimeout)
			defer shutdownCancel()

			err := proxy.httpServer.Shutdown(ctx)
			if err != nil {
				proxy.logger.Errorf("error shuting down http server: %v", err)
				httpErr <- err
			}
		}
		wg.Done()
	}()

	// Start gRPC server
	grpcExit := make(chan error, 1)
	wg.Add(1)
	go func() {
		proxy.logger.WithField("address", proxy.GetGrpcAddress()).
			Debug("serving grpc")
		err := proxy.grpcServer.Serve(grpcListener)
		if err != nil {
			proxy.logger.Errorf("grpc server error: %v", err)
		}
		grpcExit <- err
		wg.Done()
	}()

	// Handle gRPC shutdown
	grpcErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		select {
		case err := <-grpcExit:
			grpcErr <- err
			break
		case <-ctx.Done():
			proxy.logger.Debug("shutting down grpc server")
			ctx, shutdownCancel :=
				context.WithTimeout(context.Background(), proxy.shutdownTimeout)
			defer shutdownCancel()

			done := make(chan struct{})
			go func() {
				proxy.grpcServer.GracefulStop()
				close(done)
			}()

			select {
			case <-done:
				break
			case <-ctx.Done():
				proxy.grpcServer.Stop()
				err := ctx.Err()
				proxy.logger.Errorf("error shuting down grpc server: %v", err)
				grpcErr <- err
			}
		}

		proxy.destinations.Clear()

		wg.Done()
	}()

	wg.Wait()
	proxy.destinations.Wait()

	select {
	case err := <-httpErr:
		return err
	case err := <-grpcErr:
		return err
	default:
		return nil
	}
}

// The address at which the HTTP server is listening.
func (proxy *Proxy) GetHttpAddress() net.Addr {
	return proxy.httpListener.Addr()
}

// The address at which the gRPC server is listening.
func (proxy *Proxy) GetGrpcAddress() net.Addr {
	return proxy.grpcListener.Addr()
}

// Poll discovery immediately, and every `discoveryInterval`. This method stops
// polling and exits when the provided context is cancelled.
func (proxy *Proxy) pollDiscovery(ctx context.Context) {
	proxy.HandleDiscovery(ctx)
	discoveryTicker := time.NewTicker(proxy.discoveryInterval)
	for {
		select {
		case <-discoveryTicker.C:
			proxy.HandleDiscovery(ctx)
		case <-ctx.Done():
			discoveryTicker.Stop()
			return
		}
	}
}

// Handles a single discovery query and updates the consistent hash.
func (proxy *Proxy) HandleDiscovery(ctx context.Context) {
	proxy.logger.Debug("discovering destinations")
	startTime := time.Now()

	// Query the discovery service.
	proxy.logger.WithField("service", proxy.forwardService).
		Debug("discovering service")
	newDestinations, err :=
		proxy.discoverer.GetDestinationsForService(proxy.forwardService)
	if err != nil {
		proxy.logger.WithField("error", err).Error("failed discover destinations")
		proxy.statsd.Count(
			"veneur_proxy.discovery.count", 1, []string{"status:fail"}, 1.0)
		return
	}
	proxy.statsd.Gauge(
		"veneur_proxy.discovery.destinations", float64(len(newDestinations)),
		[]string{}, 1.0)

	// Update the consistent hash.
	proxy.destinations.Add(ctx, newDestinations)

	proxy.statsd.Count(
		"veneur_proxy.discovery.duration_total_ms",
		time.Since(startTime).Milliseconds(), []string{}, 1.0)
	proxy.statsd.Count(
		"veneur_proxy.discovery.count", 1, []string{"status:success"}, 1.0)
}
