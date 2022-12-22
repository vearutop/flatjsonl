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
  -add-sequence
        Add auto incremented sequence number.
  -case-sensitive-keys
        Use case-sensitive keys (can fail for SQLite).
  -concurrency int
        Number of concurrent routines in reader. (default 24)
  -config string
        Configuration JSON or YAML file.
  -cpu-prof string
        Write CPU profile to file.
  -csv string
        Output to CSV file.
  -field-limit int
        Max length of field value, exceeding tail is truncated, 0 for unlimited. (default 1000)
  -hide-progress
        Do not show progress in STDERR.
  -input string
        Input from JSONL files, comma-separated.
  -key-limit int
        Max length of key, exceeding tail is truncated, 0 for unlimited.
  -match-line-prefix string
        Regular expression to capture parts of line prefix (preceding JSON).
  -max-lines int
        Max number of lines to process.
  -max-lines-keys int
        Max number of lines to process when scanning keys.
  -mem-prof string
        Write mem profile to file.
  -output string
        Output to a file (default <input>.csv).
  -progress-interval duration
        Progress update interval. (default 5s)
  -replace-keys
        Use unique tail segment converted to snake_case as key.
  -show-keys-flat
        Show all available keys as flat list.
  -show-keys-hier
        Show all available keys as hierarchy.
  -show-keys-info
        Show keys, their replaces and types.
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

```yaml
includeKeys:
  - ".key1"
  - ".key2"
  - "const:my-value"
  - ".keyGroup.[0].key3"
replaceKeys:
  ".key1": key1
  ".key2": created_at
parseTime:
  "._prefix.[1]": 2006/01/02 15:04:05.99999
outputTimeFormat: '2006-01-02 15:04:05'
outputTZ: UTC
concatDelimiter: "::"
```

Parse time is a map of original key to time pattern. See https://pkg.go.dev/time#pkg-constants for pattern rules.

Output time format is used to write parsed timestamps.

List of `includeKeys` can also declare columns with constant values in form of `"const:<value>"`, `<value>` would
be used as column value.

Configuration file can also have [regexp replaces](https://pkg.go.dev/regexp#Regexp.ReplaceAllString) as a map of 
regular expression as keys and replace patterns as values.

It is also possible to use simplified syntax with `*`, where `*` means key segment between two dots.

```json
{
  "replaceKeysRegex": {
    "^\\.foo\\.([^.]+)$": "f00_${1} VARCHAR(255)",
    ".foo.*.*": "f00_${2}_${1} VARCHAR(255)"
  }
}
```
This example would produce such transformation.
```
.foo.bar => f00_bar VARCHAR(255)
.foo.baz.qux => f00_qux_baz VARCHAR(255)
```

Regular expression replaces are applied to keys that have no matches in `replaceKeys`.

Regular expressions are checked in no particular order, when replaced key is different from original checks are 
stopped and replaced key is used.

Multiple regular expression could match and replace a key, this can lead to undefined behavior, to avoid it is 
recommended to use mutually exclusive expressions and match against full key by having `^` and `$` at the edges of exp.

If multiple keys are replaced into similar key, coalesce function is used for resulting column value, or if 
`concatDelimiter` is defined those values would be concatenated.

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
