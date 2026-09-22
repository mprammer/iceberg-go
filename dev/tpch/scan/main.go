// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Command scan registers one generated TPC-H file, commits and reloads its
// Iceberg table, then exports the public scan API's results as Arrow IPC.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
)

type options struct {
	input, schema, warehouse, name, backend, output, scanCase string
	columns, receiptPath                                      string
	reuse, registerOnly, stream                               bool
}

type receipt struct {
	SelectedFields      []string           `json:"selected_fields,omitempty"`
	PeakRSSKiB          int64              `json:"peak_rss_kib,omitempty"`
	ScanCase            string             `json:"scan_case"`
	SourceRows          int64              `json:"source_rows"`
	Rows                int64              `json:"rows"`
	SnapshotID          int64              `json:"snapshot_id"`
	Format              iceberg.FileFormat `json:"format"`
	Backend             string             `json:"backend"`
	TableLoadSeconds    float64            `json:"table_load_seconds"`
	RegistrationSeconds float64            `json:"registration_seconds"`
	ScanIPCSeconds      float64            `json:"scan_ipc_seconds"`
}

// scanOptions deliberately names a few fixed workload predicates. This exercises
// Iceberg-Go directly; it is not a translation layer for DuckDB SQL.
func scanOptions(name string) ([]table.ScanOption, error) {
	switch name {
	case "":
		return nil, nil
	case "orders-key-range":
		return []table.ScanOption{
			table.WithSelectedFields("o_custkey", "o_orderstatus"),
			table.WithRowFilter(iceberg.NewAnd(
				iceberg.GreaterThanEqual(iceberg.Reference("o_orderkey"), int64(5000)),
				iceberg.LessThan(iceberg.Reference("o_orderkey"), int64(10000)))),
		}, nil
	case "orders-empty":
		return []table.ScanOption{
			table.WithSelectedFields("o_custkey", "o_orderstatus"),
			table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("o_orderkey"), int64(-1))),
		}, nil
	case "lineitem-q6":
		return []table.ScanOption{
			table.WithSelectedFields("l_extendedprice", "l_discount"),
			table.WithRowFilter(iceberg.NewAnd(
				iceberg.GreaterThanEqual(iceberg.Reference("l_shipdate"), "1994-01-01"),
				iceberg.LessThan(iceberg.Reference("l_shipdate"), "1995-01-01"),
				iceberg.GreaterThanEqual(iceberg.Reference("l_discount"), "0.05"),
				iceberg.LessThanEqual(iceberg.Reference("l_discount"), "0.07"),
				iceberg.LessThan(iceberg.Reference("l_quantity"), "24.00"))),
		}, nil
	default:
		return nil, fmt.Errorf("unknown scan case %q", name)
	}
}

// resolveInput accepts either a local path or an object-store URI. Iceberg
// records whichever form it is given and opens it through the matching FileIO,
// so a gs:// or s3:// input reaches the readers the same way a file does.
// Only local paths are made absolute; a URI is already fully qualified and
// filepath.Abs would mangle its scheme.
func resolveInput(input string) (string, error) {
	if parsed, err := url.Parse(input); err == nil && len(parsed.Scheme) > 1 {
		return input, nil
	}
	return filepath.Abs(input)
}

func run(ctx context.Context, opts options) (result receipt, err error) {
	if opts.input == "" || opts.schema == "" || opts.warehouse == "" || opts.name == "" || (opts.output == "" && !opts.registerOnly && !opts.stream) {
		return result, errors.New("input, schema, warehouse, table and output are required")
	}
	if opts.reuse && opts.registerOnly {
		return result, errors.New("reuse and register-only are mutually exclusive")
	}
	if opts.stream && (opts.output != "" || opts.registerOnly || opts.receiptPath == "") {
		return result, errors.New("stream requires receipt and excludes output/register-only")
	}
	if opts.columns != "" && opts.scanCase != "" {
		return result, errors.New("columns and scan-case are mutually exclusive")
	}
	scanOpts, err := scanOptions(opts.scanCase)
	if err != nil {
		return result, err
	}
	if opts.columns != "" {
		var columns []string
		if err := json.Unmarshal([]byte(opts.columns), &columns); err != nil {
			return result, fmt.Errorf("columns must be a JSON string array: %w", err)
		}
		if len(columns) == 0 {
			return result, errors.New("columns must select at least one field")
		}
		seen := make(map[string]bool, len(columns))
		for _, name := range columns {
			if name == "" || name == "*" || seen[name] {
				return result, fmt.Errorf("invalid or duplicate selected field %q", name)
			}
			seen[name] = true
		}
		scanOpts = append(scanOpts, table.WithSelectedFields(columns...))
	}
	backend := vortex.Backend(opts.backend)
	if !slices.Contains(vortex.AvailableBackends(), backend) {
		return result, fmt.Errorf("backend %q is unavailable; compiled backends: %v", backend, vortex.AvailableBackends())
	}
	ctx = vortex.WithBackend(ctx, backend)
	input, err := resolveInput(opts.input)
	if err != nil {
		return result, err
	}
	var format iceberg.FileFormat
	switch filepath.Ext(input) {
	case ".parquet":
		format = iceberg.ParquetFile
	case ".vortex":
		format = iceberg.VortexFile
	default:
		return result, errors.New("input must end in .parquet or .vortex")
	}
	raw, err := os.ReadFile(opts.schema)
	if err != nil {
		return result, err
	}
	var schema iceberg.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return result, err
	}
	warehouse, err := resolveInput(opts.warehouse)
	if err != nil {
		return result, err
	}
	// A warehouse in object storage makes the catalog use that store's IO for
	// data files too, which is what lets a gs:// input be read at all. The
	// Hadoop catalog is not atomic across concurrent commits; this harness
	// registers serially, one writer per table, so accept that here.
	var props iceberg.Properties
	if u, parseErr := url.Parse(warehouse); parseErr == nil && len(u.Scheme) > 1 {
		props = iceberg.Properties{"allow-unsafe-commits": "true"}
	}
	cat, err := hadoop.NewCatalog("tpch", warehouse, props)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, cat.Close()) }()
	started := time.Now()
	ident := table.Identifier{"tpch", opts.name}
	var tbl *table.Table
	if opts.reuse {
		tbl, err = cat.LoadTable(ctx, ident)
		if err != nil {
			return result, err
		}
		result.TableLoadSeconds = time.Since(started).Seconds()
	} else {
		if err := cat.CreateNamespace(ctx, table.Identifier{"tpch"}, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return result, err
		}
		tbl, err = cat.CreateTable(ctx, ident, &schema, catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: "2"}))
		if err != nil {
			return result, err
		}
		tx := tbl.NewTransaction()
		if err := tx.AddFiles(ctx, []string{input}, nil, false); err != nil {
			return result, err
		}
		if _, err := tx.Commit(ctx); err != nil {
			return result, err
		}
		// Force the durable metadata/manifests path rather than scanning staged state.
		tbl, err = cat.LoadTable(ctx, ident)
		if err != nil {
			return result, err
		}
		result.RegistrationSeconds = time.Since(started).Seconds()
	}
	if tbl.CurrentSnapshot() == nil {
		return result, errors.New("committed table has no snapshot")
	}
	result.SnapshotID = tbl.CurrentSnapshot().SnapshotID
	result.Format, result.Backend = format, opts.backend
	scan := tbl.Scan()
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return result, err
	}
	if len(tasks) != 1 || tasks[0].File.FileFormat() != format || tasks[0].File.FilePath() != input {
		return result, fmt.Errorf("expected one committed %s data file, got %v", format, tasks)
	}
	result.ScanCase, result.SourceRows = opts.scanCase, tasks[0].File.Count()
	if opts.registerOnly {
		return result, nil
	}
	scan = tbl.Scan(scanOpts...)
	started = time.Now()
	file := os.Stdout
	if !opts.stream {
		file, err = os.OpenFile(opts.output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return result, err
		}
		defer func() {
			err = errors.Join(err, file.Close())
			if err != nil {
				_ = os.Remove(opts.output)
			}
		}()
	}
	arrowSchema, records, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return result, err
	}
	for _, field := range arrowSchema.Fields() {
		result.SelectedFields = append(result.SelectedFields, field.Name)
	}
	writer := ipc.NewWriter(file, ipc.WithSchema(arrowSchema))
	defer func() {
		if writer != nil {
			err = errors.Join(err, writer.Close())
		}
	}()
	for record, readErr := range records {
		if readErr != nil {
			return result, readErr
		}
		err = writer.Write(record)
		result.Rows += record.NumRows()
		record.Release()
		if err != nil {
			return result, err
		}
	}
	if opts.scanCase == "" && result.Rows != tasks[0].File.Count() {
		return result, fmt.Errorf("scan returned %d rows; manifest records %d", result.Rows, tasks[0].File.Count())
	}
	if err := writer.Close(); err != nil {
		writer = nil

		return result, err
	}
	writer = nil
	result.ScanIPCSeconds = time.Since(started).Seconds()

	return result, nil
}

// Linux resets VmHWM on exec. wait4 can include the Python parent's RSS at
// fork, which would misattribute verification/import memory to the scanner.
func peakRSSKiB() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				value, _ := strconv.ParseInt(fields[1], 10, 64)

				return value
			}
		}
	}

	return 0
}

func writeReceipt(path string, value receipt) (err error) {
	if path == "" {
		return json.NewEncoder(os.Stdout).Encode(value)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()

	return json.NewEncoder(file).Encode(value)
}

func main() {
	var opts options
	flag.StringVar(&opts.input, "input", "", "generated Parquet or Vortex file")
	flag.StringVar(&opts.schema, "schema", "", "Iceberg schema JSON from the generator")
	flag.StringVar(&opts.warehouse, "warehouse", "", "local Hadoop catalog directory")
	flag.StringVar(&opts.name, "table", "", "new table name")
	flag.StringVar(&opts.backend, "backend", "native", "Vortex backend: native or ffi")
	flag.StringVar(&opts.output, "output", "", "new Arrow IPC stream file")
	flag.StringVar(&opts.scanCase, "scan-case", "", "optional fixed predicate: orders-key-range, orders-empty, lineitem-q6")
	flag.BoolVar(&opts.reuse, "reuse", false, "scan the existing committed table without registering again")
	flag.BoolVar(&opts.registerOnly, "register-only", false, "register and commit the file without scanning or exporting it")
	flag.BoolVar(&opts.stream, "stream", false, "write Arrow IPC to stdout; requires a separate receipt path")
	flag.StringVar(&opts.columns, "columns", "", "selected columns as a JSON array of field names")
	flag.StringVar(&opts.receiptPath, "receipt", "", "new JSON receipt file instead of stdout")
	flag.Parse()
	result, err := run(context.Background(), opts)
	if err == nil {
		result.PeakRSSKiB = peakRSSKiB()
		err = writeReceipt(opts.receiptPath, result)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
