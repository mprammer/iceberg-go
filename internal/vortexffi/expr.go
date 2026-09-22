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

//go:build vortex && cgo

//nolint:nlreturn // cgo generates return statements inside its call wrappers.
package vortexffi

/*
#include <stdlib.h>
#include "vortex.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"math/big"
	"unsafe"
)

// BinaryOp is a Vortex binary operator.
type BinaryOp int

const (
	OpEqual BinaryOp = iota
	OpNotEqual
	OpGreater
	OpGreaterEqual
	OpLess
	OpLessEqual
)

func (op BinaryOp) cValue() (C.vx_binary_operator, error) {
	switch op {
	case OpEqual:
		return C.VX_OPERATOR_EQ, nil
	case OpNotEqual:
		return C.VX_OPERATOR_NOT_EQ, nil
	case OpGreater:
		return C.VX_OPERATOR_GT, nil
	case OpGreaterEqual:
		return C.VX_OPERATOR_GTE, nil
	case OpLess:
		return C.VX_OPERATOR_LT, nil
	case OpLessEqual:
		return C.VX_OPERATOR_LTE, nil
	default:
		return 0, fmt.Errorf("vortex: unknown binary operator %d", op)
	}
}

// Expr is a Vortex expression owned by the ExprArena that built it.
type Expr struct {
	ptr *C.vx_expression
}

// ExprArena owns every expression and scalar built through it and frees them
// all at once.
//
// Vortex expressions are reference counted and a parent retains its children,
// so freeing an intermediate node while a parent still holds it is safe. That
// makes an arena the simplest correct ownership model here: build a tree, hand
// the root to a scan, then release the whole arena. It must not be released
// before the scan that uses its expressions has been started.
type ExprArena struct {
	exprs   []*C.vx_expression
	scalars []*C.vx_scalar
}

func NewExprArena() *ExprArena { return &ExprArena{} }

// Close frees every expression and scalar built through the arena.
func (a *ExprArena) Close() {
	for _, e := range a.exprs {
		C.vx_expression_free(e)
	}
	for _, s := range a.scalars {
		C.vx_scalar_free(s)
	}
	a.exprs, a.scalars = nil, nil
}

func (a *ExprArena) track(p *C.vx_expression, what string) (*Expr, error) {
	if p == nil {
		return nil, fmt.Errorf("vortex: building %s returned NULL", what)
	}
	a.exprs = append(a.exprs, p)

	return &Expr{ptr: p}, nil
}

func (a *ExprArena) trackScalar(p *C.vx_scalar, cerr *C.vx_error, what string) (*C.vx_scalar, error) {
	if err := takeErr(cerr, "build "+what+" scalar"); err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("vortex: building %s scalar returned NULL", what)
	}
	a.scalars = append(a.scalars, p)

	return p, nil
}

// Root returns the expression denoting the scan's root scope.
func (a *ExprArena) Root() (*Expr, error) {
	return a.track(C.vx_expression_root(), "root")
}

// GetItem selects a named field from a struct-valued expression, so nested
// columns are reached by chaining calls.
func (a *ExprArena) GetItem(child *Expr, name string) (*Expr, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	view := C.vx_view{ptr: cname, len: C.size_t(len(name))}

	return a.track(C.vx_expression_get_item(view, child.ptr), "get_item "+name)
}

// Column reaches a column by its path from the root, e.g. ["addr", "city"].
func (a *ExprArena) Column(path []string) (*Expr, error) {
	if len(path) == 0 {
		return nil, errors.New("vortex: empty column path")
	}

	expr, err := a.Root()
	if err != nil {
		return nil, err
	}

	for _, name := range path {
		expr, err = a.GetItem(expr, name)
		if err != nil {
			return nil, err
		}
	}

	return expr, nil
}

// RowIdx returns an expression yielding each row's index in the file.
//
// Projected alongside the data, it survives a scan filter, so a caller can
// recover a row's original position even though the filter dropped the rows
// around it. That is what allows predicate pushdown to coexist with Iceberg
// features defined in terms of physical positions - positional deletes,
// deletion vectors, row-lineage _row_id.
//
// Added to the Vortex C FFI for this integration; the JNI bindings have had it
// as Expression.rowIdx().
func (a *ExprArena) RowIdx() (*Expr, error) {
	return a.track(C.vx_expression_row_idx(), "row_idx")
}

// Pack builds a struct-valued expression from named child expressions.
//
// Unlike Select, which can only narrow one struct to some of its fields, Pack
// assembles a struct out of arbitrary expressions. Pruning fields inside a
// nested struct needs it: select the wanted leaves of each subtree, then pack
// the results back into the expected shape.
//
// Added to the Vortex C FFI for this integration; the JNI bindings have had it
// as Expression.pack().
func (a *ExprArena) Pack(names []string, exprs []*Expr, nullable bool) (*Expr, error) {
	if len(names) != len(exprs) {
		return nil, fmt.Errorf("vortex: pack got %d names for %d expressions",
			len(names), len(exprs))
	}
	if len(names) == 0 {
		return nil, errors.New("vortex: empty pack")
	}

	views := make([]C.vx_view, len(names))
	cstrs := make([]*C.char, len(names))
	defer func() {
		for _, s := range cstrs {
			C.free(unsafe.Pointer(s))
		}
	}()

	for i, name := range names {
		cstrs[i] = C.CString(name)
		views[i] = C.vx_view{ptr: cstrs[i], len: C.size_t(len(name))}
	}

	ptrs := make([]*C.vx_expression, len(exprs))
	for i, e := range exprs {
		ptrs[i] = e.ptr
	}

	return a.track(C.vx_expression_pack(
		&views[0],
		(**C.vx_expression)(unsafe.Pointer(&ptrs[0])),
		C.size_t(len(ptrs)),
		C.bool(nullable),
	), "pack")
}

// Select projects the named top-level fields of child.
func (a *ExprArena) Select(names []string, child *Expr) (*Expr, error) {
	if len(names) == 0 {
		return nil, errors.New("vortex: empty selection")
	}

	views := make([]C.vx_view, len(names))
	cstrs := make([]*C.char, len(names))
	defer func() {
		for _, s := range cstrs {
			C.free(unsafe.Pointer(s))
		}
	}()

	for i, name := range names {
		cstrs[i] = C.CString(name)
		views[i] = C.vx_view{ptr: cstrs[i], len: C.size_t(len(name))}
	}

	return a.track(C.vx_expression_select(&views[0], C.size_t(len(views)), child.ptr), "select")
}

func (a *ExprArena) literal(scalar *C.vx_scalar, what string) (*Expr, error) {
	var cerr *C.vx_error
	p := C.vx_expression_literal(scalar, &cerr)
	if err := takeErr(cerr, "build "+what+" literal"); err != nil {
		return nil, err
	}

	return a.track(p, what+" literal")
}

func (a *ExprArena) LiteralBool(v bool) (*Expr, error) {
	s, err := a.trackScalar(C.vx_scalar_new_bool(C.bool(v), false), nil, "bool")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "bool")
}

func (a *ExprArena) LiteralInt32(v int32) (*Expr, error) {
	s, err := a.trackScalar(C.vx_scalar_new_i32(C.int32_t(v), false), nil, "i32")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "i32")
}

func (a *ExprArena) LiteralInt64(v int64) (*Expr, error) {
	s, err := a.trackScalar(C.vx_scalar_new_i64(C.int64_t(v), false), nil, "i64")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "i64")
}

func (a *ExprArena) LiteralFloat32(v float32) (*Expr, error) {
	s, err := a.trackScalar(C.vx_scalar_new_f32(C.float(v), false), nil, "f32")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "f32")
}

func (a *ExprArena) LiteralFloat64(v float64) (*Expr, error) {
	s, err := a.trackScalar(C.vx_scalar_new_f64(C.double(v), false), nil, "f64")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "f64")
}

func (a *ExprArena) LiteralString(v string) (*Expr, error) {
	cstr := C.CString(v)
	defer C.free(unsafe.Pointer(cstr))

	var cerr *C.vx_error
	view := C.vx_view{ptr: cstr, len: C.size_t(len(v))}
	s, err := a.trackScalar(C.vx_scalar_new_utf8(view, false, &cerr), cerr, "utf8")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "utf8")
}

func (a *ExprArena) LiteralBinary(v []byte) (*Expr, error) {
	var (
		ptr  *C.uint8_t
		cbuf unsafe.Pointer
	)
	if len(v) > 0 {
		cbuf = C.malloc(C.size_t(len(v)))
		if cbuf == nil {
			return nil, errors.New("vortex: failed to allocate a binary literal")
		}
		defer C.free(cbuf)

		copy(unsafe.Slice((*byte)(cbuf), len(v)), v)
		ptr = (*C.uint8_t)(cbuf)
	}

	var cerr *C.vx_error
	s, err := a.trackScalar(C.vx_scalar_new_binary(ptr, C.size_t(len(v)), false, &cerr), cerr, "binary")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "binary")
}

// LiteralDecimal builds a decimal literal from an unscaled value. Vortex takes
// the unscaled integer plus precision and scale, matching Iceberg's own
// representation.
func (a *ExprArena) LiteralDecimal(unscaled *big.Int, precision, scale int) (*Expr, error) {
	if unscaled == nil || precision < 1 || precision > 38 || scale < 0 || scale > precision {
		return nil, errors.New("vortex: invalid Iceberg decimal value, precision, or scale")
	}
	if !unscaled.IsInt64() {
		bytes16, err := twosComplementLE(unscaled, 16)
		if err != nil {
			return nil, err
		}

		var cerr *C.vx_error
		s, err2 := a.trackScalar(C.vx_scalar_new_decimal_i128_le(
			(*C.uint8_t)(unsafe.Pointer(&bytes16[0])),
			C.uint8_t(precision), C.int8_t(scale), false, &cerr), cerr, "decimal128")
		if err2 != nil {
			return nil, err2
		}

		return a.literal(s, "decimal128")
	}

	var cerr *C.vx_error
	s, err := a.trackScalar(C.vx_scalar_new_decimal_i64(
		C.int64_t(unscaled.Int64()), C.uint8_t(precision), C.int8_t(scale), false, &cerr),
		cerr, "decimal64")
	if err != nil {
		return nil, err
	}

	return a.literal(s, "decimal64")
}

// twosComplementLE encodes v as a little-endian two's-complement integer of
// exactly n bytes.
func twosComplementLE(v *big.Int, n int) ([]byte, error) {
	if v == nil || n <= 0 {
		return nil, errors.New("vortex: invalid signed integer encoding")
	}
	limit := new(big.Int).Lsh(big.NewInt(1), uint(n*8-1))
	minimum := new(big.Int).Neg(new(big.Int).Set(limit))
	if v.Cmp(minimum) < 0 || v.Cmp(limit) >= 0 {
		return nil, fmt.Errorf("vortex: decimal value does not fit in %d bytes", n)
	}
	out := make([]byte, n)

	mod := new(big.Int).Lsh(big.NewInt(1), uint(n*8))
	norm := new(big.Int).Mod(v, mod)

	be := norm.Bytes()
	if len(be) > n {
		return nil, fmt.Errorf("vortex: decimal value does not fit in %d bytes", n)
	}

	// Reverse into little-endian, right-aligning the big-endian magnitude.
	for i, b := range be {
		out[len(be)-1-i] = b
	}

	return out, nil
}

func (a *ExprArena) Binary(op BinaryOp, lhs, rhs *Expr) (*Expr, error) {
	cop, err := op.cValue()
	if err != nil {
		return nil, err
	}

	return a.track(C.vx_expression_binary(cop, lhs.ptr, rhs.ptr), "binary")
}

func (a *ExprArena) Not(child *Expr) (*Expr, error) {
	return a.track(C.vx_expression_not(child.ptr), "not")
}

func (a *ExprArena) IsNull(child *Expr) (*Expr, error) {
	return a.track(C.vx_expression_is_null(child.ptr), "is_null")
}

func (a *ExprArena) And(exprs []*Expr) (*Expr, error) { return a.nary(exprs, true) }

func (a *ExprArena) Or(exprs []*Expr) (*Expr, error) { return a.nary(exprs, false) }

func (a *ExprArena) nary(exprs []*Expr, isAnd bool) (*Expr, error) {
	switch len(exprs) {
	case 0:
		return nil, errors.New("vortex: empty n-ary expression")
	case 1:
		return exprs[0], nil
	}

	ptrs := make([]*C.vx_expression, len(exprs))
	for i, e := range exprs {
		ptrs[i] = e.ptr
	}

	// The slice holds only C pointers, so passing its address to C is allowed.
	arr := (**C.vx_expression)(unsafe.Pointer(&ptrs[0]))
	if isAnd {
		return a.track(C.vx_expression_and(arr, C.size_t(len(ptrs))), "and")
	}

	return a.track(C.vx_expression_or(arr, C.size_t(len(ptrs))), "or")
}
