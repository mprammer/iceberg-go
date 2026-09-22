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

package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
)

// The shared adapter handles Iceberg schemas and metadata for both backends.
// Backends return ordered Arrow batches; Iceberg applies exact row predicates
// and deletes after reading. Only the FFI backend requires cgo.

type vortexFormat struct{}

func vortexFileFormat() FileFormat { return vortexFormat{} }

func vortexFileSource(fs iceio.IO, dataFile iceberg.DataFile) FileSource {
	return &vortexFileSourceImpl{fs: fs, file: dataFile}
}

type vortexFileSourceImpl struct {
	fs   iceio.IO
	file iceberg.DataFile
}

func (v *vortexFileSourceImpl) GetReader(ctx context.Context) (FileReader, error) {
	return vortexFormat{}.Open(ctx, v.fs, v.file.FilePath())
}

// Open uses the caller's storage implementation for every read. The file stays
// open until the backend is closed so range readers never outlive their I/O.
func (vortexFormat) Open(ctx context.Context, fs iceio.IO, path string) (_ FileReader, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, f.Close())
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	input := contextVortexReaderAt{ctx: ctx, ReaderAt: f}
	var backend vortexBackendReader
	switch vortex.BackendFromContext(ctx) {
	case vortex.Native:
		backend, err = openNativeVortex(ctx, input, info.Size())
	case vortex.FFI:
		backend, err = openFFIVortex(ctx, input, info.Size(), path)
	default:
		return nil, fmt.Errorf("%w: unknown Vortex backend %q", iceberg.ErrInvalidArgument, vortex.BackendFromContext(ctx))
	}
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	sc, err := backend.Schema()
	if err != nil {
		return nil, errors.Join(err, backend.Close())
	}
	count, exact := backend.RowCount()
	if !exact || count > math.MaxInt64 {
		return nil, errors.Join(fmt.Errorf("%w: Vortex reader must report an exact int64 row count", iceberg.ErrInvalidArgument), backend.Close())
	}
	owned = false

	return &vortexFileReader{
		backend: backend, file: f, fileSize: info.Size(),
		schema: normalizeViewSchema(sc), rowCount: count,
	}, nil
}

// vortexBackendReader is the read-only boundary shared by native and FFI adapters.
// The caller owns the input file. Returned record readers must be released before
// Close; batches follow Arrow's reference-counting contract.
type vortexBackendReader interface {
	io.Closer
	Schema() (*arrow.Schema, error)
	RowCount() (uint64, bool)
	Scan(context.Context, []string, *VortexScanFilter) (array.RecordReader, error)
}

type contextVortexReaderAt struct {
	io.ReaderAt
	ctx context.Context
}

func (r contextVortexReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.ReaderAt.ReadAt(p, off)
	if err == nil {
		err = r.ctx.Err()
	}

	return n, err
}

// VortexScanFilter is an optional pruning hint. Iceberg still evaluates the
// complete residual predicate. NeedPositions requests the original physical row
// positions alongside batches so pruning can coexist with deletes and lineage.
type VortexScanFilter struct {
	Expr          iceberg.BooleanExpression
	FileSchema    *iceberg.Schema
	NeedPositions bool
}

func NewVortexScanFilter(expr iceberg.BooleanExpression, sc *iceberg.Schema, needPositions bool) any {
	return &VortexScanFilter{Expr: expr, FileSchema: sc, NeedPositions: needPositions}
}

// VortexMetadata contains the verified file metadata used by registration.
type VortexMetadata struct {
	Schema     *arrow.Schema
	RowCount   uint64
	ExactCount bool
}

type vortexFileReader struct {
	backend  vortexBackendReader
	file     io.Closer
	schema   *arrow.Schema
	rowCount uint64
	fileSize int64

	// projected holds the annotated top-level fields selected by the last
	// PrunedSchema call, indexed by the column indices that call returned.
	projected map[int]arrow.Field
}

func (r *vortexFileReader) Close() error {
	if r.backend == nil {
		return nil
	}
	err := errors.Join(r.backend.Close(), r.file.Close())
	r.backend = nil
	r.file = nil

	return err
}

func (r *vortexFileReader) SourceFileSize() int64 { return r.fileSize }

func (r *vortexFileReader) Metadata() Metadata {
	return &VortexMetadata{Schema: r.schema, RowCount: r.rowCount, ExactCount: true}
}

func (r *vortexFileReader) Schema() (*arrow.Schema, error) { return r.schema, nil }

// PrunedSchema selects the top-level columns whose field IDs, or any of whose
// descendants' field IDs, appear in projectedIDs.
//
// Pruning is top-level only: a struct with one projected child is read whole
// rather than partially. Vortex can project into nested layouts, so this is a
// scope choice rather than a format limit; it keeps the field-ID resolution
// here to one unambiguous case while the in-file field-ID mechanism is still in
// flight upstream.
func (r *vortexFileReader) PrunedSchema(projectedIDs map[int]struct{}, mapping iceberg.NameMapping) (*arrow.Schema, []int, error) {
	sc, err := r.Schema()
	if err != nil {
		return nil, nil, err
	}

	var (
		fields  []arrow.Field
		indices = make([]int, 0)
	)
	r.projected = make(map[int]arrow.Field)

	for i, field := range sc.Fields() {
		mapped := findMappedField(mapping, field.Name)

		id, ok := resolveFieldID(field, mapped)
		if !ok {
			// No field ID from either the name mapping or the file itself: the
			// column cannot be matched to the Iceberg schema, so it cannot be
			// projected. Skipping beats guessing an ID.
			continue
		}

		if !subtreeIsProjected(field, mapped, id, projectedIDs) {
			continue
		}

		annotated, err := annotateFieldIDs(field, mapped, id)
		if err != nil {
			return nil, nil, err
		}

		fields = append(fields, annotated)
		indices = append(indices, i)
		r.projected[i] = annotated
	}

	meta := sc.Metadata()

	return arrow.NewSchema(fields, &meta), indices, nil
}

// GetRecords scans the file, returning only the requested columns. cols holds
// indices into the file's top-level fields, as returned by PrunedSchema; nil
// reads every column.
func (r *vortexFileReader) GetRecords(ctx context.Context, cols []int, tester any) (array.RecordReader, error) {
	var filter *VortexScanFilter
	if tester != nil {
		var ok bool
		filter, ok = tester.(*VortexScanFilter)
		if !ok {
			// Row-group testers are Parquet-specific. Silently ignoring one
			// would hide a wiring mistake rather than surface it.
			return nil, fmt.Errorf("%w: the Vortex reader takes only a *VortexScanFilter tester",
				iceberg.ErrInvalidArgument)
		}
	}

	var names []string
	var projectedSchema *arrow.Schema
	if cols != nil {
		fields := make([]arrow.Field, 0, len(cols))
		names = make([]string, 0, len(cols))
		for _, idx := range cols {
			field, ok := r.projected[idx]
			if !ok {
				return nil, fmt.Errorf("%w: column index %d was not returned by PrunedSchema",
					iceberg.ErrInvalidArgument, idx)
			}
			names = append(names, field.Name)
			fields = append(fields, field)
		}
		meta := r.schema.Metadata()
		projectedSchema = arrow.NewSchema(fields, &meta)
	}

	return r.scan(ctx, names, filter, projectedSchema)
}

func (r *vortexFileReader) scan(ctx context.Context, names []string, filter *VortexScanFilter, projectedSchema *arrow.Schema) (array.RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.backend == nil {
		return nil, fmt.Errorf("%w: Vortex reader is closed", iceberg.ErrInvalidArgument)
	}
	// The FFI scan API interprets an empty projection as all columns. A
	// zero-column Iceberg projection must preserve row count without decoding.
	emptyProjection := names != nil && len(names) == 0
	unfiltered := filter == nil || filter.Expr == nil || filter.Expr.Equals(iceberg.AlwaysTrue{})
	var rdr array.RecordReader
	if emptyProjection && unfiltered {
		rr := &vortexCountReader{ctx: ctx, remaining: int64(r.rowCount), schema: arrow.NewSchema(nil, nil)}
		rr.refs.Store(1)
		rdr = rr
	} else {
		// A filter may reference columns absent from an empty projection. Read
		// candidates through the backend before stripping those columns.
		if emptyProjection {
			names = nil
		}
		var err error
		rdr, err = r.backend.Scan(ctx, names, filter)
		if err != nil {
			return nil, err
		}
		if emptyProjection {
			rdr = newVortexEmptyProjectionReader(rdr)
		}
	}
	if filter != nil && filter.NeedPositions {
		positioned, err := newVortexPositionReader(rdr, int64(r.rowCount))
		if err != nil {
			rdr.Release()

			return nil, err
		}
		rdr = positioned
	}

	normalized := newViewNormalizingReader(ctx, rdr)
	if projectedSchema != nil {
		// The mapped field IDs must be present on batches too: lineage
		// synthesis resolves stored metadata columns by ID before projection.
		normalized.schema = projectedSchema
		normalized.anyCast = true // rebuild the batch with annotated fields
	}

	return normalized, nil
}

type vortexCountReader struct {
	ctx        context.Context
	schema     *arrow.Schema
	remaining  int64
	nextOffset int64
	positions  [1]RowGroupSpan
	current    arrow.RecordBatch
	refs       atomic.Int64
}

func (r *vortexCountReader) RowPositions() []RowGroupSpan { return r.positions[:] }

func (r *vortexCountReader) Schema() *arrow.Schema { return r.schema }
func (r *vortexCountReader) Err() error            { return r.ctx.Err() }
func (r *vortexCountReader) Retain()               { r.refs.Add(1) }
func (r *vortexCountReader) Release() {
	if r.refs.Add(-1) == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}
func (r *vortexCountReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *vortexCountReader) Record() arrow.RecordBatch      { return r.current }
func (r *vortexCountReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.remaining == 0 || r.ctx.Err() != nil {
		return false
	}
	n := min(r.remaining, int64(65536))
	r.remaining -= n
	r.positions[0] = RowGroupSpan{FirstRowPos: r.nextOffset, NumRows: n}
	r.nextOffset += n
	r.current = array.NewRecordBatch(r.schema, nil, n)

	return true
}

func (r *vortexFileReader) ReadTable(ctx context.Context) (arrow.Table, error) {
	rdr, err := r.scan(ctx, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer rdr.Release()

	var batches []arrow.RecordBatch
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	for rdr.Next() {
		batch := rdr.RecordBatch()
		batch.Retain()
		batches = append(batches, batch)
	}
	if err := rdr.Err(); err != nil {
		return nil, err
	}

	return array.NewTableFromRecords(rdr.Schema(), batches), nil
}

// Vortex hands Arrow its string and binary columns as view types
// (arrow.STRING_VIEW / arrow.BINARY_VIEW), which nothing else in Iceberg
// expects: the Arrow-to-Iceberg schema conversion has no case for them and
// panics, and the same mismatch has already had to be worked around at the
// PyIceberg boundary. So views are converted to their contiguous equivalents
// here, once, at the edge of the reader, rather than left for each consumer to
// discover. The conversion copies, which is the honest cost of the mismatch;
// teaching Iceberg's Arrow conversion about view types upstream is what would
// remove it.

// normalizeViewType rewrites view types to their contiguous equivalents,
// descending into nested types. It reports whether anything changed.
func normalizeViewType(dt arrow.DataType) (arrow.DataType, bool) {
	switch t := dt.(type) {
	case *arrow.StringViewType:
		return arrow.BinaryTypes.String, true
	case *arrow.BinaryViewType:
		return arrow.BinaryTypes.Binary, true
	case *arrow.StructType:
		fields, changed := normalizeViewFields(t.Fields())
		if !changed {
			return dt, false
		}

		return arrow.StructOf(fields...), true
	case *arrow.ListType:
		fields, changed := normalizeViewFields([]arrow.Field{t.ElemField()})
		if !changed {
			return dt, false
		}

		return arrow.ListOfField(fields[0]), true
	case *arrow.LargeListType:
		fields, changed := normalizeViewFields([]arrow.Field{t.ElemField()})
		if !changed {
			return dt, false
		}

		return arrow.LargeListOfField(fields[0]), true
	case *arrow.FixedSizeListType:
		fields, changed := normalizeViewFields([]arrow.Field{t.ElemField()})
		if !changed {
			return dt, false
		}

		return arrow.FixedSizeListOfField(t.Len(), fields[0]), true
	case *arrow.MapType:
		fields, changed := normalizeViewFields([]arrow.Field{t.KeyField(), t.ItemField()})
		if !changed {
			return dt, false
		}
		out := arrow.MapOfFields(fields[0], fields[1])
		out.KeysSorted = t.KeysSorted

		return out, true
	default:
		return dt, false
	}
}

func normalizeViewFields(fields []arrow.Field) ([]arrow.Field, bool) {
	out := make([]arrow.Field, len(fields))
	changed := false

	for i, f := range fields {
		dt, c := normalizeViewType(f.Type)
		f.Type = dt
		out[i] = f
		changed = changed || c
	}

	return out, changed
}

// normalizeViewSchema returns sc with every view type replaced. It returns sc
// itself when there is nothing to replace.
func normalizeViewSchema(sc *arrow.Schema) *arrow.Schema {
	fields, changed := normalizeViewFields(sc.Fields())
	if !changed {
		return sc
	}

	meta := sc.Metadata()

	return arrow.NewSchema(fields, &meta)
}

// viewNormalizingReader casts a Vortex scan's view-typed columns to their
// contiguous equivalents and carries mapped field IDs onto emitted batches.
// It passes batches through when neither types nor schema metadata change.
type viewNormalizingReader struct {
	ctx    context.Context
	inner  array.RecordReader
	schema *arrow.Schema

	// castCol[i] is true when column i needs converting; false columns are
	// passed through by reference.
	castCol  []bool
	anyCast  bool
	current  arrow.RecordBatch
	err      error
	refCount atomic.Int64
}

func newViewNormalizingReader(ctx context.Context, inner array.RecordReader) *viewNormalizingReader {
	src := inner.Schema()
	normalized := normalizeViewSchema(src)

	r := &viewNormalizingReader{
		ctx:     ctx,
		inner:   inner,
		schema:  normalized,
		castCol: make([]bool, len(src.Fields())),
	}

	r.refCount.Store(1)

	for i := range src.Fields() {
		if !arrow.TypeEqual(src.Field(i).Type, normalized.Field(i).Type) {
			r.castCol[i] = true
			r.anyCast = true
		}
	}

	return r
}

func (r *viewNormalizingReader) RowPositions() []RowGroupSpan {
	if positioned, ok := r.inner.(RowPositionedRecordReader); ok {
		return positioned.RowPositions()
	}

	return nil
}

func (r *viewNormalizingReader) Schema() *arrow.Schema { return r.schema }

func (r *viewNormalizingReader) Err() error {
	if r.err != nil {
		return r.err
	}

	return r.inner.Err()
}

func (r *viewNormalizingReader) Retain() { r.refCount.Add(1) }

func (r *viewNormalizingReader) Release() {
	if r.refCount.Add(-1) > 0 {
		return
	}

	r.releaseCurrent()
	r.inner.Release()
}

func (r *viewNormalizingReader) releaseCurrent() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

func (r *viewNormalizingReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is the pre-RecordBatch spelling, still required by array.RecordReader.
func (r *viewNormalizingReader) Record() arrow.RecordBatch { return r.current }

func (r *viewNormalizingReader) Next() bool {
	if r.err == nil {
		r.err = r.ctx.Err()
	}
	if r.err != nil {
		return false
	}

	r.releaseCurrent()

	if !r.inner.Next() {
		r.err = r.ctx.Err()

		return false
	}
	if r.err = r.ctx.Err(); r.err != nil {
		return false
	}

	batch := r.inner.RecordBatch()
	if !r.anyCast {
		batch.Retain()
		r.current = batch

		return true
	}

	cols := make([]arrow.Array, 0, len(r.castCol))
	release := func() {
		for _, c := range cols {
			c.Release()
		}
	}

	for i := range r.castCol {
		col := batch.Column(i)
		if !r.castCol[i] {
			col.Retain()
			cols = append(cols, col)

			continue
		}

		cast, err := normalizeVortexArray(r.ctx, col, r.schema.Field(i).Type)
		if err != nil {
			release()
			r.err = fmt.Errorf("normalizing column %s: %w", r.schema.Field(i).Name, err)

			return false
		}
		cols = append(cols, cast)
	}

	r.current = array.NewRecordBatch(r.schema, cols, batch.NumRows())
	// NewRecordBatch retains the columns, so drop the references taken above.
	release()

	return true
}

// normalizeVortexArray casts view leaves and rebuilds nested Arrow arrays using
// their existing validity/offset buffers. Arrow's generic cast does not support
// map-to-map casts, including maps nested inside lists or structs.
func normalizeVortexArray(ctx context.Context, src arrow.Array, target arrow.DataType) (arrow.Array, error) {
	if arrow.TypeEqual(src.DataType(), target) {
		src.Retain()

		return src, nil
	}
	nested, ok := target.(arrow.NestedType)
	if !ok {
		return compute.CastArray(ctx, src, compute.SafeCastOptions(target))
	}
	fields := nested.Fields()
	children := src.Data().Children()
	if len(fields) != len(children) || src.DataType().ID() != target.ID() {
		return nil, fmt.Errorf("%w: incompatible nested Vortex array %s and %s", iceberg.ErrInvalidSchema, src.DataType(), target)
	}
	normalized := make([]arrow.Array, 0, len(children))
	defer func() {
		for _, child := range normalized {
			child.Release()
		}
	}()
	childData := make([]arrow.ArrayData, len(children))
	for i, data := range children {
		child := array.MakeFromData(data)
		out, err := normalizeVortexArray(ctx, child, fields[i].Type)
		child.Release()
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, out)
		childData[i] = out.Data()
	}
	data := array.NewData(target, src.Len(), src.Data().Buffers(), childData, src.NullN(), src.Data().Offset())
	defer data.Release()

	return array.MakeFromData(data), nil
}

// findMappedField looks up a top-level name in an Iceberg name mapping.
func findMappedField(mapping iceberg.NameMapping, name string) *iceberg.MappedField {
	for i := range mapping {
		for _, n := range mapping[i].Names {
			if n == name {
				return &mapping[i]
			}
		}
	}

	return nil
}

// resolveFieldID uses an in-file ID when available. Name mappings identify
// fields in files that do not carry IDs; they cannot reassign an existing ID.
func resolveFieldID(field arrow.Field, mapped *iceberg.MappedField) (int, bool) {
	if id := getFieldID(field); id != nil {
		return *id, true
	}

	if mapped != nil && mapped.FieldID != nil {
		return *mapped.FieldID, true
	}

	return 0, false
}

// subtreeIsProjected reports whether a top-level field or any of its
// descendants is in the projected set.
func subtreeIsProjected(field arrow.Field, mapped *iceberg.MappedField, id int, projectedIDs map[int]struct{}) bool {
	if _, ok := projectedIDs[id]; ok {
		return true
	}

	nested, ok := field.Type.(arrow.NestedType)
	if !ok {
		return false
	}

	for i, child := range vortexChildFields(nested) {
		var childMapped *iceberg.MappedField
		if mapped != nil {
			childMapped = findMappedField(mapped.Fields, vortexChildMappingName(nested, i, child))
		}

		childID, ok := resolveFieldID(child, childMapped)
		if !ok {
			continue
		}

		if subtreeIsProjected(child, childMapped, childID, projectedIDs) {
			return true
		}
	}

	return false
}

// annotateFieldIDs stamps PARQUET:field_id onto a field and its descendants so
// that the Arrow schema PrunedSchema returns carries IDs whether they came from
// the name mapping or from the file. Downstream conversion back to an Iceberg
// schema then takes its by-ID path rather than re-deriving names.
func annotateFieldIDs(field arrow.Field, mapped *iceberg.MappedField, id int) (arrow.Field, error) {
	field.Metadata = withFieldID(field.Metadata, id)

	nested, ok := field.Type.(arrow.NestedType)
	if !ok {
		return field, nil
	}

	children := vortexChildFields(nested)
	annotated := make([]arrow.Field, 0, len(children))
	for i, child := range children {
		var childMapped *iceberg.MappedField
		if mapped != nil {
			childMapped = findMappedField(mapped.Fields, vortexChildMappingName(nested, i, child))
		}

		childID, ok := resolveFieldID(child, childMapped)
		if !ok {
			return field, fmt.Errorf("%w: no field ID for %s.%s in the file or the name mapping",
				iceberg.ErrInvalidSchema, field.Name, child.Name)
		}

		annotatedChild, err := annotateFieldIDs(child, childMapped, childID)
		if err != nil {
			return field, err
		}
		annotated = append(annotated, annotatedChild)
	}

	rebuilt, err := rebuildNested(field.Type, annotated)
	if err != nil {
		return field, err
	}
	field.Type = rebuilt

	return field, nil
}

// withFieldID returns metadata with PARQUET:field_id set to id, preserving any
// other keys.
func withFieldID(meta arrow.Metadata, id int) arrow.Metadata {
	keys := make([]string, 0, meta.Len()+1)
	values := make([]string, 0, meta.Len()+1)

	for i := range meta.Len() {
		if meta.Keys()[i] == "PARQUET:field_id" {
			continue
		}
		keys = append(keys, meta.Keys()[i])
		values = append(values, meta.Values()[i])
	}

	keys = append(keys, "PARQUET:field_id")
	values = append(values, strconv.Itoa(id))

	return arrow.NewMetadata(keys, values)
}

// Arrow map fields contain a synthetic entries struct; Iceberg maps identify
// the key and value directly. Lists identify their child as element regardless
// of the physical Arrow child name.
func vortexChildFields(dt arrow.NestedType) []arrow.Field {
	if m, ok := dt.(*arrow.MapType); ok {
		return []arrow.Field{m.KeyField(), m.ItemField()}
	}

	return dt.Fields()
}

func vortexChildMappingName(dt arrow.NestedType, i int, child arrow.Field) string {
	switch dt.(type) {
	case *arrow.MapType:
		if i == 0 {
			return "key"
		}

		return "value"
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType:
		return "element"
	default:
		return child.Name
	}
}

// rebuildNested reconstructs a nested Arrow type with new child fields, which
// Arrow's types require rather than allowing in-place mutation.
func rebuildNested(dt arrow.DataType, children []arrow.Field) (arrow.DataType, error) {
	switch t := dt.(type) {
	case *arrow.StructType:
		return arrow.StructOf(children...), nil
	case *arrow.ListType:
		return arrow.ListOfField(children[0]), nil
	case *arrow.LargeListType:
		return arrow.LargeListOfField(children[0]), nil
	case *arrow.FixedSizeListType:
		return arrow.FixedSizeListOfField(t.Len(), children[0]), nil
	case *arrow.MapType:
		out := arrow.MapOfFields(children[0], children[1])
		out.KeysSorted = t.KeysSorted

		return out, nil
	default:
		return nil, fmt.Errorf("%w: cannot rebuild nested Arrow type %s with field IDs",
			iceberg.ErrNotImplemented, dt)
	}
}

// Producing Vortex files is not implemented; registering existing ones is.
// AddFiles resolves field IDs and uses CollectVortexRegistrationStatistics to
// register a .vortex file somebody else wrote.
// Creating one from Iceberg needs a writer, which this does not provide.

var errVortexWriteUnsupported = fmt.Errorf(
	"%w: writing %s data files is not supported", iceberg.ErrNotImplemented, iceberg.VortexFile)

// PathToIDMapping maps each field's dotted path to its Iceberg field ID.
//
// Vortex names struct fields plainly, so paths are the field names joined by
// ".", without the synthetic levels Parquet interposes for lists and maps
// (`list.element`, `key_value.key`). Only struct nesting is walked; list and
// map element IDs are not addressable by path here, which is consistent with
// pruning being top-level only on the read side.
func (vortexFormat) PathToIDMapping(sc *iceberg.Schema) (map[string]int, error) {
	result := make(map[string]int)

	var walk func(prefix string, fields []iceberg.NestedField)
	walk = func(prefix string, fields []iceberg.NestedField) {
		for _, f := range fields {
			path := f.Name
			if prefix != "" {
				path = prefix + "." + f.Name
			}
			result[path] = f.ID

			if st, ok := f.Type.(*iceberg.StructType); ok {
				walk(path, st.FieldList)
			}
		}
	}
	walk("", sc.Fields())

	return result, nil
}

// DataFileStatsFromMeta reports only the metadata's verified row count.
// Registration uses CollectVortexRegistrationStatistics to obtain file-wide
// metrics from an unfiltered scan. Backend zone pruning is independent of these
// file metrics.
func (vortexFormat) DataFileStatsFromMeta(rdr Metadata, _ map[int]StatisticsCollector,
	_ map[string]int, _ map[int]struct{}, _ *arrow.Schema,
) *DataFileStatistics {
	meta, ok := rdr.(*VortexMetadata)
	if !ok || !meta.ExactCount || meta.RowCount > math.MaxInt64 {
		panic(fmt.Errorf("%w: invalid Vortex registration metadata", iceberg.ErrInvalidArgument))
	}

	return &DataFileStatistics{
		RecordCount:     int64(meta.RowCount),
		ColSizes:        map[int]int64{},
		ValueCounts:     map[int]int64{},
		NullValueCounts: map[int]int64{},
		NanValueCounts:  map[int]int64{},
		ColAggs:         map[int]StatsAgg{},
	}
}

func (vortexFormat) GetWriteProperties(iceberg.Properties) any { return nil }

func (vortexFormat) WriteDataFile(context.Context, iceio.WriteFileIO, map[int]any, WriteFileInfo,
	[]arrow.RecordBatch,
) (iceberg.DataFile, error) {
	return nil, errVortexWriteUnsupported
}

func (vortexFormat) NewFileWriter(context.Context, iceio.WriteFileIO, map[int]any, WriteFileInfo,
	*arrow.Schema,
) (FileWriter, error) {
	return nil, errVortexWriteUnsupported
}
