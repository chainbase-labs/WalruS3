package main

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	httppprof "net/http/pprof"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3walrus"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

type fakeS3Flags struct {
	host            string
	initialBucket   string
	fixedTimeStr    string
	noIntegrity     bool
	hostBucket      bool
	hostBucketBases HostList
	autoBucket      bool
	quiet           bool

	// Walrus configuration
	epochs        int
	publisherURL  string
	aggregatorURL string

	// Postgres configuration
	pgHost     string
	pgPort     string
	pgUser     string
	pgPassword string
	pgDBName   string
	pgSSLMode  string

	debugCPU  string
	debugHost string
}

func (f *fakeS3Flags) attach(flagSet *flag.FlagSet) {
	flagSet.StringVar(&f.host, "host", ":9000", "Host to run the service")
	flagSet.StringVar(&f.fixedTimeStr, "time", "", "RFC3339 format. If passed, the server's clock will always see this time")
	flagSet.StringVar(&f.initialBucket, "initialbucket", "", "If passed, this bucket will be created on startup")
	flagSet.BoolVar(&f.noIntegrity, "no-integrity", false, "Pass this flag to disable Content-MD5 validation")
	flagSet.BoolVar(&f.autoBucket, "autobucket", false, "Create nonexistent buckets on first use")
	flagSet.BoolVar(&f.hostBucket, "hostbucket", false, "Extract bucket name from hostname")
	flagSet.Var(&f.hostBucketBases, "hostbucketbase", "Host bucket base domains (comma separated)")

	// Postgres flags
	flagSet.StringVar(&f.pgHost, "pg.host", "localhost", "PostgreSQL host")
	flagSet.StringVar(&f.pgPort, "pg.port", "5432", "PostgreSQL port")
	flagSet.StringVar(&f.pgUser, "pg.user", "postgres", "PostgreSQL user")
	flagSet.StringVar(&f.pgPassword, "pg.password", "", "PostgreSQL password")
	flagSet.StringVar(&f.pgDBName, "pg.dbname", "walrus3", "PostgreSQL database name")
	flagSet.StringVar(&f.pgSSLMode, "pg.sslmode", "disable", "PostgreSQL SSL mode")

	// Walrus flags
	flagSet.IntVar(&f.epochs, "epochs", 128, "Number of epochs to store objects for")
	flagSet.StringVar(&f.publisherURL, "publisher", "", "Walrus publisher url")
	flagSet.StringVar(&f.aggregatorURL, "aggregator", "", "Walrus aggregator url")

	// Logging
	flagSet.BoolVar(&f.quiet, "quiet", false, "Disable logging to stderr")

	// Debugging
	flagSet.StringVar(&f.debugHost, "debug.host", "", "Run the debug server on this host")
	flagSet.StringVar(&f.debugCPU, "debug.cpu", "", "Create CPU profile in this file")
}

func (f *fakeS3Flags) timeOptions() (source gofakes3.TimeSource, skewLimit time.Duration, err error) {
	skewLimit = gofakes3.DefaultSkewLimit

	if f.fixedTimeStr != "" {
		fixedTime, err := time.Parse(time.RFC3339Nano, f.fixedTimeStr)
		if err != nil {
			return nil, 0, err
		}
		source = gofakes3.FixedTimeSource(fixedTime)
		skewLimit = 0
	}

	return source, skewLimit, nil
}

func debugServer(host string) {
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)

	srv := &http.Server{Addr: host}
	srv.Handler = mux
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

func run() error {
	var values fakeS3Flags

	flagSet := flag.NewFlagSet("", 0)
	values.attach(flagSet)

	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return err
	}

	stopper, err := profile(values)
	if err != nil {
		return err
	}
	defer stopper()

	if values.debugHost != "" {
		log.Println("starting debug server at", fmt.Sprintf("http://%s/debug/pprof", values.debugHost))
		go debugServer(values.debugHost)
	}

	timeSource, timeSkewLimit, err := values.timeOptions()
	if err != nil {
		return err
	}

	// Build Postgres DSN
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		values.pgHost,
		values.pgPort,
		values.pgUser,
		values.pgPassword,
		values.pgDBName,
		values.pgSSLMode,
	)

	backend, err := s3walrus.New(dsn,
		s3walrus.WithTimeSource(timeSource),
		s3walrus.WithEpochs(values.epochs),
		s3walrus.WithPublisherURL(values.publisherURL),
		s3walrus.WithAggregatorURL(values.aggregatorURL),
	)
	if err != nil {
		return err
	}
	log.Printf("using walrus backend with postgres at %s:%s", values.pgHost, values.pgPort)

	if values.initialBucket != "" {
		if err := backend.CreateBucket(values.initialBucket); err != nil && !gofakes3.IsAlreadyExists(err) {
			return fmt.Errorf("gofakes3: could not create initial bucket %q: %v", values.initialBucket, err)
		}
		log.Println("created -initialbucket", values.initialBucket)
	}

	logger := gofakes3.GlobalLog()
	if values.quiet {
		logger = gofakes3.DiscardLog()
	}

	faker := gofakes3.New(backend,
		gofakes3.WithIntegrityCheck(!values.noIntegrity),
		gofakes3.WithTimeSkewLimit(timeSkewLimit),
		gofakes3.WithTimeSource(timeSource),
		gofakes3.WithLogger(logger),
		gofakes3.WithHostBucket(values.hostBucket),
		gofakes3.WithHostBucketBase(values.hostBucketBases.Values...),
		gofakes3.WithAutoBucket(values.autoBucket),
	)

	return listenAndServe(values.host, faker.Server())
}

func listenAndServe(addr string, handler http.Handler) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	log.Println("using port:", listener.Addr().(*net.TCPAddr).Port)
	server := &http.Server{Addr: addr, Handler: handler}

	return server.Serve(listener)
}

func profile(values fakeS3Flags) (func(), error) {
	fn := func() {}

	if values.debugCPU != "" {
		f, err := os.Create(values.debugCPU)
		if err != nil {
			return fn, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return fn, err
		}
		return pprof.StopCPUProfile, nil
	}

	return fn, nil
}

type HostList struct {
	Values []string
}

func (sl HostList) String() string {
	return strings.Join(sl.Values, ",")
}

func (sl HostList) Type() string { return "[]string" }

func (sl *HostList) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		part = strings.Trim(strings.TrimSpace(part), ".")
		if part == "" {
			return fmt.Errorf("host is empty")
		}
		sl.Values = append(sl.Values, part)
	}
	return nil
}
