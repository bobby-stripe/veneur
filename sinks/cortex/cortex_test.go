package cortex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stripe/veneur/v14/samplers"
	"github.com/stripe/veneur/v14/trace"
	"github.com/stripe/veneur/v14/util"
)

func TestName(t *testing.T) {
	sink, err := NewCortexMetricSink("https://localhost/", 30, "", logrus.NewEntry(logrus.New()), "cortex", map[string]string{}, map[string]string{}, nil, 0, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.Equal(t, "cortex", sink.Name())
}

func TestFlush(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{}, nil, 0, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server
	assert.NoError(t, sink.Flush(context.Background(), metrics))

	// Retrieve the data which the server received
	data, headers, err := server.Latest()
	assert.NoError(t, err)

	// Check standard headers
	assert.True(t, hasHeader(*headers, "Content-Encoding", "snappy"), "missing required Content-Encoding header")
	assert.True(t, hasHeader(*headers, "Content-Type", "application/x-protobuf"), "missing required Content-Type header")
	assert.True(t, hasHeader(*headers, "User-Agent", "veneur/cortex"), "missing required User-Agent header")
	assert.True(t, hasHeader(*headers, "X-Prometheus-Remote-Write-Version", "0.1.0"), "missing required version header")

	// The underlying method to convert metric -> timeseries does not
	// preserve order, so we're sorting the data here
	for k := range data.Timeseries {
		sort.Slice(data.Timeseries[k].Labels, func(i, j int) bool {
			val := strings.Compare(data.Timeseries[k].Labels[i].Name, data.Timeseries[k].Labels[j].Name)
			return val == -1
		})

	}

	// Pretty-print output for readability, and to match expected
	actual, err := json.MarshalIndent(data, "", "  ")
	assert.NoError(t, err)

	//  Load in the expected data and compare
	expected, err := ioutil.ReadFile("testdata/expected.json")
	assert.NoError(t, err)
	assert.Equal(t, string(expected), string(actual))
}

func TestChunkedWrites(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{}, nil, 3, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/chunked_input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server
	assert.NoError(t, sink.Flush(context.Background(), metrics))

	// There are 12 writes in input and our batch size is 3 so we expect 4 write requests
	assert.Equal(t, 4, len(server.History()))
}

func TestChunkNumOfMetricsLessThanBatchSize(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{}, nil, 15, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/chunked_input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server
	assert.NoError(t, sink.Flush(context.Background(), metrics))

	// There are 12 writes in input and our batch size is 15 so we expect 1 write request
	assert.Equal(t, 1, len(server.History()))
}

func TestLeftOverBatchGetsWritten(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{}, nil, 5, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/chunked_input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server
	assert.NoError(t, sink.Flush(context.Background(), metrics))

	// There are 12 writes in input and our batch size is 5 so we expect 3 write requests
	assert.Equal(t, 3, len(server.History()))
}

func TestChunkedWritesRespectContextCancellation(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{}, nil, 3, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/chunked_input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	ctx, cancel := context.WithCancel(context.Background())
	requestCount := 0

	server.onRequest(func() {
		requestCount++
		if requestCount == 2 {
			cancel()
		}
	})

	// Perform the flush to the test server
	assert.Error(t, sink.Flush(ctx, metrics))

	// we're cancelling after 2 so we should only see 2 chunks written
	assert.Equal(t, 2, len(server.History()))
}

func TestRetry(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{"Cortex-Test-Force-Fail-N": "1"}, nil, 0, true, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server, which should return 500
	assert.Error(t, sink.Flush(context.Background(), metrics))
	_, _, err = server.Latest()
	assert.Error(t, err)

	// Flush again, which should re-write the samples that previously failed,
	// in addition to one new sample
	newMetrics := []samplers.InterMetric{{}}
	assert.NoError(t, sink.Flush(context.Background(), newMetrics))
	data, _, err := server.Latest()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Timeseries))

	// Flush again, with one new sample
	assert.NoError(t, sink.Flush(context.Background(), newMetrics))
	data, _, err = server.Latest()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Timeseries))

	// Flush again, observe that no more metrics are re-flushed
	var emptyMetrics []samplers.InterMetric
	assert.NoError(t, sink.Flush(context.Background(), emptyMetrics))
	_, _, err = server.Latest()
	assert.Error(t, err)
}

func TestRetryHTTP429Enabled(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{"Cortex-Test-Force-Fail-N": "1", "Cortex-Test-Retry-Return-Status-Code": "429"}, nil, 0, true, true, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server, which should return 429
	assert.Error(t, sink.Flush(context.Background(), metrics))
	_, _, err = server.Latest()
	assert.Error(t, err)

	// Flush again, which should retry + write one new sample
	newMetrics := []samplers.InterMetric{{}}
	assert.NoError(t, sink.Flush(context.Background(), newMetrics))
	data, _, err := server.Latest()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Timeseries))
}

func TestRetryOtherStatusCodes(t *testing.T) {
	statusCodes := []string{"300", "400", "429"}
	for _, statusCode := range statusCodes {
		// Listen for prometheus writes
		server := NewTestServer(t)

		// Set up a sink
		sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{"Cortex-Test-Force-Fail-N": "1", "Cortex-Test-Retry-Return-Status-Code": statusCode}, nil, 0, true, false, DefaultCountBackoffAbandonmentThreshold)
		assert.NoError(t, err)
		assert.NoError(t, sink.Start(trace.DefaultClient))

		// input.json contains three timeseries samples in InterMetrics format
		jsInput, err := ioutil.ReadFile("testdata/input.json")
		assert.NoError(t, err)
		var metrics []samplers.InterMetric
		assert.NoError(t, json.Unmarshal(jsInput, &metrics))

		// Perform the flush to the test server, which should return a non-retryable status coe
		assert.Error(t, sink.Flush(context.Background(), metrics))
		_, _, err = server.Latest()
		assert.Error(t, err)

		// Flush again, which should write only one new sample
		newMetrics := []samplers.InterMetric{{}}
		assert.NoError(t, sink.Flush(context.Background(), newMetrics))
		data, _, err := server.Latest()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(data.Timeseries))

		// Cleanup
		server.Close()
	}
}

func TestRetryCountBackoff(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Set up a sink
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{"Cortex-Test-Force-Fail-N": "4"}, nil, 0, true, false, 4)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server, which should return 500
	assert.Error(t, sink.Flush(context.Background(), metrics))
	_, _, err = server.Latest()
	assert.Error(t, err)

	// Try again 3 more times, to pass the backoff threshold
	for i := 0; i < 3; i++ {
		emptyMetrics := []samplers.InterMetric{}
		assert.Error(t, sink.Flush(context.Background(), emptyMetrics))
		_, _, err = server.Latest()
		assert.Error(t, err)
	}

	// Flush again, with one new sample. Observe only one sample is written.
	newMetrics := []samplers.InterMetric{{}}
	assert.NoError(t, sink.Flush(context.Background(), newMetrics))
	data, _, err := server.Latest()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Timeseries))
}

func TestCustomHeaders(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Define custom headers
	customHeaders := map[string]string{
		"Authorization":    "Bearer 12345",
		"My-Custom-Header": "testing-123",
		"Another-Header":   "foobar",
	}

	// Set up a sink with custom headers
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, customHeaders, nil, 0, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server
	assert.NoError(t, sink.Flush(context.Background(), metrics))

	// Retrieve the headers which the server received
	_, headers, err := server.Latest()
	assert.NoError(t, err)

	// Check custom headers
	for name, value := range customHeaders {
		assert.True(t, hasHeader(*headers, name, value), "Missing header "+name)
	}
}

func TestBasicAuth(t *testing.T) {
	// Listen for prometheus writes
	server := NewTestServer(t)
	defer server.Close()

	// Define custom headers
	customHeaders := map[string]string{
		"My-Custom-Header": "testing-456",
		"Another-Header":   "bazzoo",
	}
	auth := BasicAuthType{
		Username: util.StringSecret{Value: "user1"},
		Password: util.StringSecret{Value: "p@ssWerd"},
	}

	// Set up a sink with custom headers
	sink, err := NewCortexMetricSink(server.URL, 30*time.Second, "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, customHeaders, &auth, 0, false, false, DefaultCountBackoffAbandonmentThreshold)
	assert.NoError(t, err)
	assert.NoError(t, sink.Start(trace.DefaultClient))

	// input.json contains three timeseries samples in InterMetrics format
	jsInput, err := ioutil.ReadFile("testdata/input.json")
	assert.NoError(t, err)
	var metrics []samplers.InterMetric
	assert.NoError(t, json.Unmarshal(jsInput, &metrics))

	// Perform the flush to the test server
	assert.NoError(t, sink.Flush(context.Background(), metrics))

	// Retrieve the headers which the server received
	_, headers, err := server.Latest()
	assert.NoError(t, err)

	// Check custom headers
	for name, value := range customHeaders {
		assert.True(t, hasHeader(*headers, name, value), "Missing or incorrect "+name+" header")
	}
	authString := auth.Username.Value + ":" + auth.Password.Value
	assert.True(t, hasHeader(*headers, "Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(authString))),
		"Missing or invalid Authorization header")
}

func TestParseConfig(t *testing.T) {
	testConfigValues := map[string]interface{}{
		"url":            "this://is.a.url",
		"remote_timeout": "90s",
		"proxy_url":      "http://another.url:8000",
		"headers":        map[string]string{"My-Header": "a-header-value"},
		"authorization": map[string]interface{}{
			"credentials": "the-credential",
		},
	}

	parsedConfig, err := ParseConfig("cortex", testConfigValues)
	assert.NoError(t, err)
	cortexConfig := parsedConfig.(CortexMetricSinkConfig)
	assert.Equal(t, cortexConfig.URL, testConfigValues["url"])
	assert.Equal(t, cortexConfig.RemoteTimeout, time.Duration(90*time.Second))
	assert.Equal(t, cortexConfig.ProxyURL, testConfigValues["proxy_url"])
	assert.Equal(t, cortexConfig.Headers, testConfigValues["headers"])
	assert.NotNil(t, cortexConfig.Authorization)
	assert.Equal(t, cortexConfig.Authorization.Type, DefaultAuthorizationType)
	assert.Equal(t, cortexConfig.Authorization.Credential.Value, "the-credential")
	assert.Empty(t, cortexConfig.BasicAuth)
}

func TestParseConfigBasicAuth(t *testing.T) {
	testConfigValues := map[string]interface{}{
		"url":            "this://is.a.url",
		"remote_timeout": "90s",
		"proxy_url":      "http://another.url:8000",
		"basic_auth": map[string]interface{}{
			"username": "user",
			"password": "pwd",
		},
	}

	parsedConfig, err := ParseConfig("cortex", testConfigValues)
	assert.NoError(t, err)
	cortexConfig := parsedConfig.(CortexMetricSinkConfig)
	assert.Equal(t, cortexConfig.URL, testConfigValues["url"])
	assert.Equal(t, cortexConfig.RemoteTimeout, time.Duration(90*time.Second))
	assert.Equal(t, cortexConfig.ProxyURL, testConfigValues["proxy_url"])
	assert.Empty(t, cortexConfig.Headers)
	assert.Empty(t, cortexConfig.Authorization)
	assert.NotNil(t, cortexConfig.BasicAuth)
	assert.Equal(t, cortexConfig.BasicAuth.Username.Value, "user")
	assert.Equal(t, cortexConfig.BasicAuth.Password.Value, "pwd")
}

func TestParseConfigDuplicateAuth(t *testing.T) {
	testConfigValues := map[string]interface{}{
		"url":            "this://is.a.url",
		"remote_timeout": "90s",
		"proxy_url":      "http://another.url:8000",
		"basic_auth": map[string]interface{}{
			"username": "user",
			"password": "pwd",
		},
		"authorization": map[string]interface{}{
			"credentials": "the-credential",
		},
	}

	_, err := ParseConfig("cortex", testConfigValues)
	assert.Error(t, err)
}

func TestParseConfigBadBasicAuth(t *testing.T) {
	testConfigValues := map[string]interface{}{
		"url":            "this://is.a.url",
		"remote_timeout": "90s",
		"proxy_url":      "http://another.url:8000",
		"basic_auth": map[string]interface{}{
			"username": "user",
		},
	}

	_, err := ParseConfig("cortex", testConfigValues)
	assert.Error(t, err)
}

func TestCorrectlySetTimeout(t *testing.T) {
	timeouts := []int{10, 20, 30, 17, 21}
	for to := range timeouts {
		sink, err := NewCortexMetricSink("http://noop", time.Duration(to), "", logrus.NewEntry(logrus.New()), "test", map[string]string{"corge": "grault"}, map[string]string{}, nil, 0, false, false, DefaultCountBackoffAbandonmentThreshold)
		assert.NoError(t, err)

		err = sink.Start(&trace.Client{})
		assert.NoError(t, err)

		assert.Equal(t, time.Duration(to), sink.Client.Timeout)
	}
}

func TestMetricToTimeSeries(t *testing.T) {
	expectedHostValue := "val2"
	expectedHostContactValue := "baz"

	metric := samplers.InterMetric{
		Name:      "test_metric",
		Timestamp: 0,
		Value:     1,
		Tags: []string{
			"host:val1",
			"team:obs",
			"host:" + expectedHostValue,
			"another:tag",
			"host_contact:foo",
		},
		Type: samplers.CounterMetric,
	}

	tags := map[string]string{
		"host_contact": expectedHostContactValue,
	}

	ts := metricToTimeSeries(metric, tags)

	for _, label := range ts.Labels {
		if label.Name == "host" {
			assert.Equal(t, expectedHostValue, label.Value)
		}

		if label.Name == "host_contact" {
			assert.Equal(t, expectedHostContactValue, label.Value)
		}
	}
}

func TestSanitise(t *testing.T) {
	data := map[string]string{
		"foo_bar": "foo_bar",
		"FOO_BAR": "FOO_BAR",
		"foo:bar": "foo:bar",
		"foo!bar": "foo_bar",
		"123_foo": "_123_foo",
	}
	for input, expected := range data {
		assert.Equal(t, expected, sanitise(input))
	}
}

func BenchmarkSanitise(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sanitise("123_the_leith_police_123_dismisseth_$%89_us")
	}
}

type RequestHistory struct {
	data    *prompb.WriteRequest
	headers *http.Header
}

// TestServer wraps an internal httptest.Server and provides a convenience
// method for retrieving the most recently written series
type TestServer struct {
	URL       string
	headers   *http.Header
	data      *prompb.WriteRequest
	server    *httptest.Server
	history   []*RequestHistory
	requestFn func()
}

// Close closes the internal test server
func (t *TestServer) Close() {
	t.server.Close()
}

// Latest returns the most recent write request, or errors if there was none
func (t *TestServer) Latest() (*prompb.WriteRequest, *http.Header, error) {
	if t.data == nil || len(t.data.Timeseries) == 0 {
		return nil, nil, errors.New("no data received")
	}
	return t.data, t.headers, nil
}

func (t *TestServer) History() []*RequestHistory {
	return t.history
}

func (t *TestServer) onRequest(fn func()) {
	t.requestFn = fn
}

// NewTestServer starts a test server instance. Ensure calls are followed by
// defer server.Close()
// to avoid hanging connections
func NewTestServer(t *testing.T) *TestServer {
	result := TestServer{}

	failedCount := 0

	router := http.NewServeMux()
	router.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		wr, err := readpb(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		returnRetryableError := false
		retryableErrorStatusCode := http.StatusInternalServerError
		forceFailThreshold := 0
		if forceFailThresholdStr, ok := r.Header["Cortex-Test-Force-Fail-N"]; ok {
			forceFailThreshold, err = strconv.Atoi(forceFailThresholdStr[0])
			if err != nil {
				t.Errorf("bad Cortex-Test-Force-Fail-N value")
			}
			returnRetryableError = true
		}
		if statusCode, ok := r.Header["Cortex-Test-Retry-Return-Status-Code"]; ok {
			if statusCode[0] == "300" {
				retryableErrorStatusCode = http.StatusMultipleChoices
			} else if statusCode[0] == "400" {
				retryableErrorStatusCode = http.StatusBadRequest
			} else if statusCode[0] == "429" {
				retryableErrorStatusCode = http.StatusTooManyRequests
			} else {
				t.Errorf("unknown Cortex-Test-Retry-Return-Status-Code value")
			}
		}
		if returnRetryableError && failedCount < forceFailThreshold {
			failedCount += 1
			t.Logf("failing, iteration=%d, successThreshold=%d", failedCount, forceFailThreshold)

			http.Error(w, "failing", retryableErrorStatusCode)
			return
		}

		if !hasHeader(r.Header, "Content-Encoding", "snappy") ||
			!hasHeader(r.Header, "Content-Type", "application/x-protobuf") ||
			!hasHeader(r.Header, "User-Agent", "veneur/cortex") ||
			!hasHeader(r.Header, "X-Prometheus-Remote-Write-Version", "0.1.0") {
			http.Error(w, "missing headers", http.StatusBadRequest)
			return
		}
		// keep a record of the most recently received headers, request
		result.headers = &r.Header
		result.data = wr
		result.history = append(result.history, &RequestHistory{
			data:    wr,
			headers: &r.Header,
		})

		if result.requestFn != nil {
			result.requestFn()
		}
	})

	server := httptest.NewServer(router)
	result.URL = server.URL + "/receive"
	result.server = server
	t.Log("test server listening on", server.URL)

	return &result
}

// hasHeader checks for the existence of the specified header
func hasHeader(h http.Header, key string, value string) bool {
	for _, val := range h[key] {
		if val == value {
			return true
		}
	}
	return false
}

// readpb reads, decompresses and unmarshals a WriteRequest from a reader
func readpb(r io.Reader) (*prompb.WriteRequest, error) {
	cdata, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, cdata)
	if err != nil {
		return nil, err
	}

	var wr prompb.WriteRequest
	if err := proto.Unmarshal(data, &wr); err != nil {
		return nil, err
	}

	return &wr, nil
}
