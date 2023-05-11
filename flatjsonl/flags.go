package flatjsonl

import (
	"flag"
	"github.com/alecthomas/kingpin/v2"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Flags contains command-line flags.
type Flags struct {
	Verbosity        int
	ProgressInterval time.Duration
	Input            []string
	Output           string

	CSV string

	SQLite     string
	SQLMaxCols int
	SQLTable   string

	PGDump string

	Raw      string
	RawDelim string

	MaxLines     int
	OffsetLines  int
	MaxLinesKeys int
	FieldLimit   int
	KeyLimit     int
	BufSize      int

	Config            string
	GetKey            string
	ReplaceKeys       bool
	ExtractStrings    bool
	SkipZeroCols      bool
	AddSequence       bool
	MatchLinePrefix   string
	CaseSensitiveKeys bool

	ShowKeysFlat bool
	ShowKeysHier bool
	ShowKeysInfo bool

	Concurrency int
}

// Register registers command-line flags.
func (f *Flags) Register() {
	kingpin.Flag("input", "Input from JSONL files, comma-separated.").StringsVar(&f.Input)

	kingpin.Flag("output", "Output to a file (default <input>.csv).").StringVar(&f.Output)
	kingpin.Flag("csv", "Output to CSV file (gzip encoded if ends with .gz).").StringVar(&f.CSV)

	kingpin.Flag("sqlite", "Output to SQLite file.").StringVar(&f.SQLite)
	kingpin.Flag("sql-max-cols", "Maximum columns in single SQL table (SQLite will fail with more than 2000).").
		Default("500").IntVar(&f.SQLMaxCols)
	kingpin.Flag("sql-table", "Table name.").Default("flatjsonl").StringVar(&f.SQLTable)
	kingpin.Flag("pg-dump", "Output to PostgreSQL dump file.").StringVar(&f.PGDump)

	kingpin.Flag("raw", "Output to RAW file (column values are written as is without escaping, gzip encoded if ends with .gz).").StringVar(&f.Raw)
	kingpin.Flag("raw-delim", "RAW file column delimiter.").StringVar(&f.RawDelim)

	kingpin.Flag("verbosity", "Show progress in STDERR, 0 disables status, 2 adds more metrics.").Default("1").IntVar(&f.Verbosity)
	kingpin.Flag("progress-interval", "Progress update interval.").Default("5s").DurationVar(&f.ProgressInterval)

	kingpin.Flag("replace-keys", "Use unique tail segment converted to snake_case as key.").BoolVar(&f.ReplaceKeys)
	kingpin.Flag("extract-strings", "Check string values for JSON content and extract when available.").BoolVar(&f.ExtractStrings)
	kingpin.Flag("get-key", "Add a single key to list of included keys.").StringVar(&f.GetKey)
	kingpin.Flag("config", "Configuration JSON or YAML file.").StringVar(&f.Config)
	kingpin.Flag("show-keys-flat", "Show all available keys as flat list.").BoolVar(&f.ShowKeysFlat)
	kingpin.Flag("show-keys-hier", "Show all available keys as hierarchy.").BoolVar(&f.ShowKeysHier)
	kingpin.Flag("show-keys-info", "Show keys, their replaces and types.").BoolVar(&f.ShowKeysInfo)
	kingpin.Flag("skip-zero-cols", "Skip columns with zero values.").BoolVar(&f.SkipZeroCols)
	kingpin.Flag("add-sequence", "Add auto incremented sequence number.").BoolVar(&f.AddSequence)
	kingpin.Flag("case-sensitive-keys", "Use case-sensitive keys (can fail for SQLite).").BoolVar(&f.CaseSensitiveKeys)
	kingpin.Flag("match-line-prefix", "Regular expression to capture parts of line prefix (preceding JSON).").StringVar(&f.MatchLinePrefix)
	kingpin.Flag("max-lines", "Max number of lines to process.").IntVar(&f.MaxLines)
	kingpin.Flag("offset-lines", "Skip a number of first lines.").IntVar(&f.OffsetLines)
	kingpin.Flag("max-lines-keys", "Max number of lines to process when scanning keys.").IntVar(&f.MaxLinesKeys)
	kingpin.Flag("field-limit", "Max length of field value, exceeding tail is truncated, 0 for unlimited.").IntVar(&f.FieldLimit)
	kingpin.Flag("key-limit", "Max length of key, exceeding tail is truncated, 0 for unlimited.").IntVar(&f.KeyLimit)
	kingpin.Flag("buf-size", "Buffer size (max length of file line) in bytes.").Default("1e7").IntVar(&f.BufSize)

	kingpin.Flag("concurrency", "Number of concurrent routines in reader.").Default(strconv.Itoa(2 * runtime.NumCPU())).IntVar(&f.Concurrency)
}

// Parse parses and prepares command-line flags.
func (f *Flags) Parse() {
	kingpin.Parse()

	if f.Output == "" && !f.ShowKeysHier && !f.ShowKeysFlat && !f.ShowKeysInfo {
		inputs := f.Inputs()

		if len(inputs) > 0 && f.CSV == "" && f.SQLite == "" && f.Raw == "" && f.PGDump == "" {
			f.Output = inputs[0].FileName + ".csv"
		}
	}

	f.PrepareOutput()
}

// PrepareOutput parses output flag.
func (f *Flags) PrepareOutput() {
	if f.Output == "" {
		return
	}

	for _, output := range strings.Split(f.Output, ",") {
		outputLow := strings.ToLower(output)

		if strings.HasSuffix(outputLow, ".csv") ||
			strings.HasSuffix(outputLow, ".csv.gz") ||
			strings.HasSuffix(outputLow, ".csv.zst") {
			if f.CSV != "" {
				println("CSV output is already enabled, skipping", output)

				continue
			}

			f.CSV = output

			continue
		}

		if strings.HasSuffix(outputLow, ".raw") ||
			strings.HasSuffix(outputLow, ".raw.gz") ||
			strings.HasSuffix(outputLow, ".raw.zst") {
			if f.Raw != "" {
				println("RAW output is already enabled, skipping", output)

				continue
			}

			f.Raw = output

			continue
		}

		if strings.HasSuffix(outputLow, ".sqlite") {
			if f.SQLite != "" {
				println("CSV output is already enabled, skipping", output)

				continue
			}

			f.SQLite = output

			continue
		}

		println("unexpected output", output)
	}
}

// Inputs returns list of file names to read.
func (f *Flags) Inputs() []Input {
	inputs := flag.Args()

	if len(f.Input) > 0 {
		if len(f.Input) == 0 {
			inputs = append(inputs, strings.Split(f.Input[0], ",")...)
		} else {
			inputs = append(inputs, f.Input...)
		}
	}

	res := make([]Input, 0, len(inputs))
	for _, fn := range inputs {
		res = append(res, Input{FileName: fn})
	}

	return res
}
