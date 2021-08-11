package attribution

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/sirupsen/logrus"

	"github.com/stripe/veneur/v14/plugins"
	"github.com/stripe/veneur/v14/samplers"
)

const Delimiter = '\t'
const FileExtension = "tsv.gz"
const S3ClientUninitializedError = errors.New("s3 client has not been initialized")

type AttributionSink struct {
	traceClient *trace.Client
	log         *logrus.Logger
	hostname    string
	s3Svc       s3iface.S3API
	s3Bucket    string
}

// NewAttributionSink creates a new sink to flush ownership/usage (attribution) data to S3
func NewAttributionSink(log *logrus.Logger, hostname string, s3Svc s3iface.S3API, s3Bucket string) (*AttributionSink, error) {
	// NOTE: We don't need to account for config.Tags because they do not, generally speaking,
	// increase cardinality on a per-Veneur level. The metric attribution schemas are designed for
	// to account for config.Tags increasing cardinality across a fleet of Veneurs, such that many
	// attribution dumps can be batch aggregated together accurately.
	return &AttributionSink{nil, log, hostname, s3Svc, s3Bucket}, nil
}

// Start starts the AttributionSink
func (s *AttributionSink) Start(traceClient *trace.Client) error {
	s.traceClient = traceClient
	return nil
}

func (s *AttributionSink) Name() string {
	return "attribution"
}

func (s *AttributionSink) Flush(ctx context.Context, metrics []samplers.InterMetric) error {
	span, subCtx := trace.StartSpanFromContext(ctx, "")
	defer span.ClientFinish(s.traceClient)

	flushStart := time.Now()

	if !sinks.IsAcceptableMetric(metric, s) {
		continue
	}

	csv, err := encodeInterMetricsCSV(metrics, flushStart, s.Hostname)
	if err != nil {
		s.Logger.WithFields(logrus.Fields{
			logrus.ErrorKey: err,
			"metrics":       len(metrics),
		}).Error("Could not marshal metrics before posting to s3")
		return err
	}

	err = s.s3Post(csv)
	if err != nil {
		s.Logger.WithFields(logrus.Fields{
			logrus.ErrorKey: err,
			"metrics":       len(metrics),
		}).Error("Error posting to s3")
		return err
	}

	s.Logger.WithField("metrics", len(metrics)).Debug("Completed flush to s3")
	return nil
}

// encodeInterMetricsCSV returns a reader containing the gzipped CSV representation of the
// InterMetric data, one row per InterMetric. The AWS sdk requires seekable input, so we return a ReadSeeker here.
func encodeInterMetricsCSV(metrics []samplers.InterMetric, ts time.Time) (io.ReadSeeker, error) {
	b := &bytes.Buffer{}
	gzw := gzip.NewWriter(b)
	w := csv.NewWriter(gzw)
	w.Comma = Delimiter

	for _, metric := range metrics {
		encodeInterMetricCSV(metric, w, ts)
	}

	w.Flush()
	err := gzw.Close()
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b.Bytes()), w.Error()
}

func (s *AttributionSink) s3Post(data io.ReadSeeker) error {
	if s.Svc == nil {
		return S3ClientUninitializedError
	}
	params := &s3.PutObjectInput{
		Bucket: aws.String(s.S3Bucket),
		Key:    s3Path(s.Hostname),
		Body:   data,
	}

	_, err := s.S3Svc.PutObject(params)
	return err
}

func s3Path(hostname string) *string {
	t := time.Now()
	filename := strconv.FormatInt(t.Unix(), 10) + "." + FileExtension
	return aws.String(path.Join(t.Format("2006/01/02"), hostname, filename))
}