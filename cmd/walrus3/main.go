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
	"github.com/johannesboyne/gofakes3/migration"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "server":
		if err := runServer(); err != nil {
			log.Fatal(err)
		}
	case "migration":
		if err := runMigration(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Printf("Unknown command: %s", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: ./main <command> [options]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  server     Start WalruS3 server")
	fmt.Println("  migration  Run data migration from S3-compatible storage")
	fmt.Println("")
	fmt.Println("Use './main <command> -h' for command-specific help")
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

type migrationFlags struct {
	// Source S3-compatible configuration
	sourceEndpoint       string
	sourceRegion         string
	sourceAccessKey      string
	sourceSecretKey      string
	sourceBucket         string
	sourcePrefix         string
	sourceUseSSL         bool
	sourceForcePathStyle bool

	// Destination WalruS3 configuration
	destEndpoint  string
	destAccessKey string
	destSecretKey string
	destBucket    string
	destUseSSL    bool

	// Migration options
	workers      int
	batchSize    int
	dryRun       bool
	maxRetries   int
	skipExisting bool
	verbose      bool
}

func (f *migrationFlags) attach(flagSet *flag.FlagSet) {
	// Source configuration
	flagSet.StringVar(&f.sourceEndpoint, "source.endpoint", "", "Source S3 endpoint (e.g., s3.amazonaws.com, storage.googleapis.com)")
	flagSet.StringVar(&f.sourceRegion, "source.region", "us-east-1", "Source region")
	flagSet.StringVar(&f.sourceAccessKey, "source.access-key", "", "Source access key (required)")
	flagSet.StringVar(&f.sourceSecretKey, "source.secret-key", "", "Source secret key (required)")
	flagSet.StringVar(&f.sourceBucket, "source.bucket", "", "Source bucket name (required)")
	flagSet.StringVar(&f.sourcePrefix, "source.prefix", "", "Source object prefix filter")
	flagSet.BoolVar(&f.sourceUseSSL, "source.use-ssl", true, "Use SSL for source connection")
	flagSet.BoolVar(&f.sourceForcePathStyle, "source.force-path-style", false, "Force path-style URLs for source")

	// Destination configuration
	flagSet.StringVar(&f.destEndpoint, "dest.endpoint", "http://localhost:9000", "Destination WalruS3 endpoint")
	flagSet.StringVar(&f.destAccessKey, "dest.access-key", "", "Destination access key (leave empty for anonymous)")
	flagSet.StringVar(&f.destSecretKey, "dest.secret-key", "", "Destination secret key (leave empty for anonymous)")
	flagSet.StringVar(&f.destBucket, "dest.bucket", "", "Destination bucket name (default: same as source)")
	flagSet.BoolVar(&f.destUseSSL, "dest.use-ssl", false, "Use SSL for destination connection")

	// Migration options
	flagSet.IntVar(&f.workers, "workers", 10, "Number of concurrent workers")
	flagSet.IntVar(&f.batchSize, "batch-size", 100, "Batch size for listing objects")
	flagSet.BoolVar(&f.dryRun, "dry-run", false, "Dry run mode (don't actually migrate)")
	flagSet.IntVar(&f.maxRetries, "max-retries", 3, "Maximum number of retries per object")
	flagSet.BoolVar(&f.skipExisting, "skip-existing", false, "Skip objects that already exist in destination")
	flagSet.BoolVar(&f.verbose, "verbose", false, "Verbose logging")
}

func (f *migrationFlags) validate() error {
	if f.sourceBucket == "" {
		return fmt.Errorf("source.bucket is required")
	}
	if f.destBucket == "" {
		f.destBucket = f.sourceBucket
	}
	return nil
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
	flagSet.IntVar(&f.epochs, "epochs", 32, "Number of epochs to store objects for")
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

func runServer() error {
	var values fakeS3Flags

	flagSet := flag.NewFlagSet("", 0)
	values.attach(flagSet)

	if err := flagSet.Parse(os.Args[2:]); err != nil {
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

func runMigration() error {
	var flags migrationFlags

	flagSet := flag.NewFlagSet("migration", flag.ExitOnError)
	flags.attach(flagSet)

	if err := flagSet.Parse(os.Args[2:]); err != nil {
		return err
	}

	if err := flags.validate(); err != nil {
		return err
	}

	// Create migration config
	config := &migration.Config{
		SourceEndpoint:       flags.sourceEndpoint,
		SourceRegion:         flags.sourceRegion,
		SourceAccessKey:      flags.sourceAccessKey,
		SourceSecretKey:      flags.sourceSecretKey,
		SourceBucket:         flags.sourceBucket,
		SourcePrefix:         flags.sourcePrefix,
		SourceUseSSL:         flags.sourceUseSSL,
		SourceForcePathStyle: flags.sourceForcePathStyle,

		DestEndpoint:  flags.destEndpoint,
		DestAccessKey: flags.destAccessKey,
		DestSecretKey: flags.destSecretKey,
		DestBucket:    flags.destBucket,
		DestUseSSL:    flags.destUseSSL,

		Workers:      flags.workers,
		BatchSize:    flags.batchSize,
		DryRun:       flags.dryRun,
		MaxRetries:   flags.maxRetries,
		SkipExisting: flags.skipExisting,
		Verbose:      flags.verbose,
	}

	// Create and run migrator
	migrator, err := migration.NewMigrator(config)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}
	defer migrator.Close()

	log.Printf("Starting migration from %s/%s to %s/%s",
		flags.sourceEndpoint, flags.sourceBucket,
		flags.destEndpoint, flags.destBucket)

	return migrator.Run()
}
