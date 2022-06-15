package destinations_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/veneur/v14/forwardrpc"
	"github.com/stripe/veneur/v14/proxy/destinations"
	"github.com/stripe/veneur/v14/samplers/metricpb"
	"github.com/stripe/veneur/v14/scopedstatsd"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type TestDestinations struct {
	destinations destinations.Destinations
	logger       *logrus.Logger
	statsd       *scopedstatsd.MockClient
}

func CreateTestDestinations(
	ctrl *gomock.Controller, dialTimeout time.Duration,
) *TestDestinations {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	mockStatsd := scopedstatsd.NewMockClient(ctrl)

	return &TestDestinations{
		destinations: destinations.Create(
			dialTimeout, logrus.NewEntry(logger), mockStatsd),
		logger: logger,
		statsd: mockStatsd,
	}
}

func TestAddEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fixture := CreateTestDestinations(ctrl, 30*time.Second)
	fixture.destinations.Add(context.Background(), []string{})

	assert.Zero(t, fixture.destinations.Size())
}

func TestAddDialTimeoutExpired(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fixture := CreateTestDestinations(ctrl, 0)
	fixture.statsd.EXPECT().Count(
		"veneur_proxy.forward.connect", int64(1),
		[]string{"status:failed_dial"}, 1.0)
	fixture.destinations.Add(
		context.Background(), []string{"localhost:3000"})

	assert.Zero(t, fixture.destinations.Size())
}

type FakeServer struct {
	grpcListener net.Listener
	handler      *forwardrpc.MockForwardServer
	serveError   chan error
	server       *grpc.Server
}

func CreateFakeServer(
	t *testing.T, ctrl *gomock.Controller,
) *FakeServer {
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	mockHandler := forwardrpc.NewMockForwardServer(ctrl)
	forwardrpc.RegisterForwardServer(server, mockHandler)

	serveError := make(chan error)
	go func() {
		serveError <- server.Serve(grpcListener)
	}()

	return &FakeServer{
		grpcListener: grpcListener,
		handler:      mockHandler,
		serveError:   serveError,
		server:       server,
	}
}

func (server *FakeServer) WaitForConnect(
	timeout time.Duration, closeConnection chan struct{},
) (forwardrpc.Forward_SendMetricsV2Server, error) {
	connectionChannel := make(chan forwardrpc.Forward_SendMetricsV2Server)

	server.handler.EXPECT().SendMetricsV2(gomock.Any()).Times(1).DoAndReturn(func(
		connection forwardrpc.Forward_SendMetricsV2Server,
	) error {
		connectionChannel <- connection
		<-closeConnection
		connection.SendAndClose(&emptypb.Empty{})
		return nil
	})

	select {
	case connection := <-connectionChannel:
		return connection, nil
	case <-time.After(timeout):
		return nil, errors.New("timed out waiting for connect")
	}
}

func (server *FakeServer) Close(t *testing.T) {
	server.server.GracefulStop()
	server.grpcListener.Close()

	serveError := <-server.serveError
	assert.NoError(t, serveError)
}

func TestAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fixture := CreateTestDestinations(ctrl, time.Second)
	server := CreateFakeServer(t, ctrl)
	defer server.Close(t)

	metric := &metricpb.Metric{
		Name: "metric-name",
		Tags: []string{"tag1:value1"},
		Type: metricpb.Type_Counter,
		Value: &metricpb.Metric_Counter{
			Counter: &metricpb.CounterValue{
				Value: 1,
			},
		},
	}

	fixture.statsd.EXPECT().Count(
		"veneur_proxy.grpc.conn_begin", int64(1),
		[]string{"client:true"}, 1.0)
	fixture.statsd.EXPECT().Count(
		"veneur_proxy.forward.connect", int64(1),
		[]string{"status:success"}, 1.0)
	fixture.statsd.EXPECT().Count(
		"veneur_proxy.forward.disconnect", int64(1),
		[]string{"error:false"}, 1.0)
	fixture.statsd.EXPECT().Count(
		"veneur_proxy.grpc.conn_end", int64(1),
		[]string{"client:true"}, 1.0)

	fixture.destinations.Add(
		context.Background(), []string{server.grpcListener.Addr().String()})

	closeConnection := make(chan struct{})
	connection, err := server.WaitForConnect(time.Second, closeConnection)
	assert.NoError(t, err)

	assert.Equal(t, 1, fixture.destinations.Size())
	destination, err := fixture.destinations.Get("metric-namecountertag1:value1")
	assert.NoError(t, err)

	destination.Client.Send(metric)
	actualMetric, err := connection.Recv()
	assert.NoError(t, err)
	assert.Equal(t, metric, actualMetric)

	close(closeConnection)

	fixture.destinations.Wait()
	assert.Equal(t, 0, fixture.destinations.Size())
}

func TestGetEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fixture := CreateTestDestinations(ctrl, 30*time.Second)
	destination, err := fixture.destinations.Get("sample-key")

	assert.Nil(t, destination)
	if assert.Error(t, err) {
		assert.Equal(t, "empty circle", err.Error())
	}
}

func TestClear(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fixture := CreateTestDestinations(ctrl, 1*time.Second)
	server := CreateFakeServer(t, ctrl)
	defer server.Close(t)

	fixture.statsd.EXPECT().Count(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	fixture.destinations.Add(
		context.Background(), []string{server.grpcListener.Addr().String()})

	assert.Equal(t, 1, fixture.destinations.Size())
	_, err := fixture.destinations.Get("any")
	assert.NoError(t, err)

	closeConnection := make(chan struct{})
	server.WaitForConnect(time.Second, closeConnection)

	fixture.destinations.Clear()
	assert.Equal(t, 0, fixture.destinations.Size())
	fixture.destinations.Wait()

	close(closeConnection)
}
