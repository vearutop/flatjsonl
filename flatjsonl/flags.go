package flatjsonl

import (
	"flag"
	"strings"
)

// Flags contains command-line flags.
type Flags struct {
	HideProgress bool
	Input        string
	Output       string
	CSV          string
	SQLite       string
	SQLTable     string
	MaxLines     int
	MaxLinesKeys int

	Config          string
	ReplaceKeys     bool
	SkipZeroCols    bool
	MatchLinePrefix string

	ShowKeysFlat bool
	ShowKeysHier bool
}

// Register registers command-line flags.
func (f *Flags) Register() {
	flag.StringVar(&f.Input, "input", "", "Input from JSONL files, comma-separated.")
	flag.StringVar(&f.Output, "output", "", "Output to a file (default <input>.csv).")
	flag.StringVar(&f.CSV, "csv", "", "Output to CSV file.")
	flag.StringVar(&f.SQLite, "sqlite", "", "Output to SQLite file.")
	flag.StringVar(&f.SQLTable, "sql-table", "flatjsonl", "Table name.")

	flag.BoolVar(&f.HideProgress, "hide-progress", false, "Do not show progress in STDERR.")
	flag.BoolVar(&f.ReplaceKeys, "replace-keys", false, "Use unique tail segment converted to snake_case as key.")
	flag.StringVar(&f.Config, "config", "", "Configuration JSON file.")
	flag.BoolVar(&f.ShowKeysFlat, "show-keys-flat", false, "Show all available keys as flat list.")
	flag.BoolVar(&f.ShowKeysHier, "show-keys-hier", false, "Show all available keys as hierarchy.")
	flag.BoolVar(&f.SkipZeroCols, "skip-zero-cols", false, "Skip columns with zero values.")
	flag.StringVar(&f.MatchLinePrefix, "match-line-prefix", "", "Regular expression to capture parts of line prefix (preceding JSON).")
	flag.IntVar(&f.MaxLines, "max-lines", 0, "Max number of lines to process.")
	flag.IntVar(&f.MaxLinesKeys, "max-lines-keys", 0, "Max number of lines to process when scanning keys.")
}

// Parse parses and prepares command-line flags.
func (f *Flags) Parse() {
	flag.Parse()

	if f.Output == "" && !f.ShowKeysHier && !f.ShowKeysFlat {
		inputs := f.Inputs()

		if len(inputs) > 0 {
			f.Output = inputs[0] + ".csv"
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

		if strings.HasSuffix(outputLow, ".csv") {
			if f.CSV != "" {
				println("CSV output is already enabled, skipping", output)

				continue
			}

			f.CSV = output

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
func (f *Flags) Inputs() []string {
	inputs := flag.Args()

	if f.Input != "" {
		inputs = append(inputs, strings.Split(f.Input, ",")...)
	}

	return inputs
}
