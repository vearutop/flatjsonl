# flatjsonl

[![Build Status](https://github.com/vearutop/flatjsonl/workflows/test-unit/badge.svg)](https://github.com/vearutop/flatjsonl/actions?query=branch%3Amaster+workflow%3Atest-unit)
[![Coverage Status](https://codecov.io/gh/vearutop/flatjsonl/branch/master/graph/badge.svg)](https://codecov.io/gh/vearutop/flatjsonl)
[![GoDevDoc](https://img.shields.io/badge/dev-doc-00ADD8?logo=go)](https://pkg.go.dev/github.com/vearutop/flatjsonl)
[![Time Tracker](https://wakatime.com/badge/github/vearutop/flatjsonl.svg)](https://wakatime.com/badge/github/vearutop/flatjsonl)
![Code lines](https://sloc.xyz/github/vearutop/flatjsonl/?category=code)
![Comments](https://sloc.xyz/github/vearutop/flatjsonl/?category=comments)

`flatjsonl` renders structured logs as table.

## Why?

Logs, structured as [`JSON Lines`](https://jsonlines.org/) (and sometimes prefixed with non-JSON message), are very 
common source of information for ad-hoc analytics and investigations. 

They can be processed with `jq` and grepped for a variety of data checks, however there are much more powerful and 
convenient tools that operate on columnar table data, rather than hierarchical structures.

This tool converts structured logs into tabular data (`CSV`, `SQLite`) with flexible mapping options.

## How it works?

In simplest case this tool iterates log file two times, first pass to collect all available keys and 
second pass to actually fill the table with already known keys (columns).

During each pass, each line is decoded and traversed recursively.
Keys for nested elements are declared with dot-separated syntax (same as in `jq`), array indexes are enclosed in `[x]`, 
e.g. `.deeper.subProperty.[0].foo`.

String values are checked for JSON contents and are also traversed if JSON is found.

If `includeKeys` is not empty in [configuration file](#configuration-file), first pass is skipped.

## Install

```
go install github.com/vearutop/flatjsonl@latest
$(go env GOPATH)/bin/flatjsonl --help
```

Or download binary from [releases](https://github.com/vearutop/flatjsonl/releases).

```
wget https://github.com/vearutop/flatjsonl/releases/latest/download/linux_amd64.tar.gz && tar xf linux_amd64.tar.gz && rm linux_amd64.tar.gz
./flatjsonl -version
```

## Usage

```
flatjsonl -help
```
```
Usage of flatjsonl:
  -config string
        Configuration JSON file.
  -csv string
        Output to CSV file.
  -hide-progress
        Do not show progress in STDERR.
  -input string
        Input from JSONL files, comma-separated.
  -match-line-prefix string
        Regular expression to capture parts of line prefix (preceding JSON).
  -max-lines int
        Max number of lines to process.
  -max-lines-keys int
        Max number of lines to process when scanning keys.
  -output string
        Output to a file (default <input>.csv).
  -replace-keys
        Use unique tail segment converted to snake_case as key.
  -show-keys-flat
        Show all available keys as flat list.
  -show-keys-hier
        Show all available keys as hierarchy.
  -skip-zero-cols
        Skip columns with zero values.
  -sql-table string
        Table name. (default "flatjsonl")
  -sqlite string
        Output to SQLite file.
  -version
        Show version and exit.
```

### Configuration file

```json
{
  "includeKeys": [".key1", ".key2", ".keyGroup.[0].key3"],
  "replaceKeys": {".key1": "key1", ".key2": "created_at"}
}
```

## Examples

Import data from `events.jsonl` as columns described in `events.json` config file to 
SQLite table `events` in file `report.sqlite`.

```
flatjsonl -sqlite report.sqlite -sql-table events -config events.json events.jsonl
```

Show flat list of keys found in first 100 (or less) lines of `events.jsonl`.
```
flatjsonl -max-lines 100 -show-keys-flat events.jsonl
```

Import data from `part1.log`, `part2.log`, `part3.log` into `part1.log.csv` with keys converted to snake_case 
unique tails and with columns matched from line prefix (for lines formatted as `<prefix> {<json>}`).
```
flatjsonl -match-line-prefix '([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)' -replace-keys part1.log part2.log part3.log
```
