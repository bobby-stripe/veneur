package destinations

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"github.com/stripe/veneur/v14/forwardrpc"
	"github.com/stripe/veneur/v14/proxy/grpcstats"
	"github.com/stripe/veneur/v14/scopedstatsd"
	"google.golang.org/grpc"
	"stathat.com/c/consistent"
)

type Destinations interface {
	Add(ctx context.Context, destinations []string)
	Clear()
	Get(key string) (*Destination, error)
	Size() int
	Wait()
}

type Destination struct {
	Client forwardrpc.Forward_SendMetricsV2Client
}

var _ Destinations = &destinations{}

type destinations struct {
	connectionWaitGroup sync.WaitGroup
	destinations        map[string]Destination
	destinationsHash    *consistent.Consistent
	dialTimeout         time.Duration
	logger              *logrus.Entry
	mutex               sync.Mutex
	statsd              scopedstatsd.Client
}

// Create a new set of destinations to forward metrics to.
func Create(
	dialTimeout time.Duration, logger *logrus.Entry, statsd scopedstatsd.Client,
) Destinations {
	return &destinations{
		destinations:     map[string]Destination{},
		destinationsHash: consistent.New(),
		dialTimeout:      dialTimeout,
		logger:           logger,
		statsd:           statsd,
	}
}

// Adds and connects to a destination. Destinations will be automatically
// removed if the connection to them closes.
func (d *destinations) Add(
	ctx context.Context, destinations []string,
) {
	// Filter new destinations.
	addedDestinations := []string{}
	for _, destination := range destinations {
		_, ok := d.destinations[destination]
		if !ok {
			addedDestinations = append(addedDestinations, destination)
		}
	}

	// Connect to each new destination in parallel.
	waitGroup := sync.WaitGroup{}
	for _, addedDestination := range addedDestinations {
		waitGroup.Add(1)
		go func(addedDestination string) {
			defer waitGroup.Done()

			// Connect to the destination.
			d.logger.WithField("destination", addedDestination).
				Debug("dialing destination")
			dialContext, cancel := context.WithTimeout(ctx, d.dialTimeout)
			connection, err := grpc.DialContext(
				dialContext, addedDestination, grpc.WithBlock(), grpc.WithInsecure(),
				grpc.WithStatsHandler(&grpcstats.StatsHandler{
					IsClient: true,
					Statsd:   d.statsd,
				}))
			cancel()
			if err != nil {
				d.logger.WithError(err).WithField("destination", addedDestination).
					Error("failed dial destination")
				d.statsd.Count(
					"veneur_proxy.forward.connect", 1,
					[]string{"status:failed_dial"}, 1.0)
				return
			}

			// Open a streaming gRPC connection to the destination.
			d.logger.WithField("destination", addedDestination).
				Debug("connecting to destination")
			forwardClient := forwardrpc.NewForwardClient(connection)
			client, err := forwardClient.SendMetricsV2(ctx)
			if err != nil {
				d.logger.WithError(err).WithField("destination", addedDestination).
					Error("failed to connect to destination")
				d.statsd.Count(
					"veneur_proxy.forward.connect", 1,
					[]string{"status:failed_connect"}, 1.0)
				return
			}

			// Add the destination to the consistent hash.
			d.logger.WithField("destination", addedDestination).
				Debug("adding destination")
			func() {
				d.mutex.Lock()
				defer d.mutex.Unlock()

				d.connectionWaitGroup.Add(1)
				d.destinations[addedDestination] = Destination{
					Client: client,
				}
				d.destinationsHash.Add(addedDestination)
			}()
			d.statsd.Count(
				"veneur_proxy.forward.connect", 1, []string{"status:success"}, 1.0)

			// Wait for the connection to disconnect.
			go d.waitForDisconnect(addedDestination, connection, client)
		}(addedDestination)
	}
	waitGroup.Wait()
}

// Wait for the streaming gRPC connection to the destination to close, and
// remove the the destination once it does.
func (d *destinations) waitForDisconnect(
	addedDestination string, connection *grpc.ClientConn,
	client forwardrpc.Forward_SendMetricsV2Client,
) {
	var empty empty.Empty
	err := client.RecvMsg(&empty)
	if err == nil || err == io.EOF {
		d.logger.WithField("destination", addedDestination).
			Debug("disconnected from destination")
		d.statsd.Count(
			"veneur_proxy.forward.disconnect", 1, []string{"error:false"}, 1.0)
	} else {
		d.logger.WithError(err).
			WithField("destination", addedDestination).
			Error("disconnected from destination")
		d.statsd.Count(
			"veneur_proxy.forward.disconnect", 1, []string{"error:true"}, 1.0)
	}

	func() {
		d.mutex.Lock()
		defer d.mutex.Unlock()

		_, ok := d.destinations[addedDestination]
		if !ok {
			return
		}
		d.destinationsHash.Remove(addedDestination)
		delete(d.destinations, addedDestination)
		d.connectionWaitGroup.Done()
	}()

	connection.Close()
}

// Removes all destinations, and closes connections to them.
func (d *destinations) Clear() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.destinationsHash.Set([]string{})
	for key, destination := range d.destinations {
		destination.Client.CloseSend()
		delete(d.destinations, key)
		d.connectionWaitGroup.Done()
	}
}

// Gets a destination for a given key.
func (d *destinations) Get(key string) (*Destination, error) {
	destinationAddress, err := d.destinationsHash.Get(key)
	if err != nil {
		return nil, err
	}
	destination, ok := d.destinations[destinationAddress]
	if !ok {
		return nil, fmt.Errorf("unknown destination: %s", destinationAddress)
	}
	return &destination, nil
}

// Returns the current number of destinations.
func (d *destinations) Size() int {
	return len(d.destinations)
}

// Waits for all current connections to be closed and removed.
func (d *destinations) Wait() {
	d.connectionWaitGroup.Wait()
}
