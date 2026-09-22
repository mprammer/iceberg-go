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

package internal

import (
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/vortexffi"
)

// Translating an Iceberg predicate into a Vortex one is an optimization, never a
// correctness requirement: the scan applies the row filter again in Go after
// reading. So anything that cannot be translated degrades to "match everything"
// rather than failing the scan. The degradation is per-subtree, mirroring the
// Java integration's converter: under AND, an untranslatable side can simply be
// dropped because the other side still narrows the result; under OR it cannot,
// because dropping one arm would narrow the result incorrectly, so the whole OR
// is abandoned.

// setPredicateLimit caps how many literals an IN predicate may carry before the
// translation is abandoned. Vortex has no set-membership primitive over scalars,
// so IN expands to a chain of ORed equalities, and a very wide chain costs more
// to evaluate than the pruning it buys. The Java converter uses the same bound.
const setPredicateLimit = 200

// convKind distinguishes a translated expression from the two constant outcomes
// and from a failure to translate.
type convKind int

const (
	convOK convKind = iota
	convAlwaysTrue
	convAlwaysFalse
	// convUnconvertible marks a subtree Vortex cannot express. It is distinct
	// from convAlwaysTrue: both match everything, but only this one may be
	// dropped from an AND without changing what the filter means.
	convUnconvertible
)

type convResult struct {
	kind convKind
	expr *vortexffi.Expr
}

var (
	resAlwaysTrue    = convResult{kind: convAlwaysTrue}
	resAlwaysFalse   = convResult{kind: convAlwaysFalse}
	resUnconvertible = convResult{kind: convUnconvertible}
)

// converted wraps a successfully translated expression.
func converted(e *vortexffi.Expr) convResult { return convResult{kind: convOK, expr: e} }

// vortexFilterConverter walks a bound Iceberg expression and builds the
// equivalent Vortex expression in an arena.
type vortexFilterConverter struct {
	arena *vortexffi.ExprArena
	sc    *iceberg.Schema
}

// BuildVortexFilter translates a bound Iceberg expression into a Vortex
// expression. It returns nil when nothing useful could be translated, in which
// case the caller should scan without a pushed-down filter.
//
// The returned expression belongs to arena and is valid until the arena closes.
func BuildVortexFilter(arena *vortexffi.ExprArena, sc *iceberg.Schema,
	expr iceberg.BooleanExpression,
) (*vortexffi.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	// Negation must be normalized before a subtree can be weakened for
	// pruning: NOT(A AND unsupported) cannot safely become NOT(A).
	expr, err := iceberg.RewriteNotExpr(expr)
	if err != nil {
		return nil, nil
	}

	conv := &vortexFilterConverter{arena: arena, sc: sc}

	res, err := iceberg.VisitExpr(expr, conv)
	if err != nil {
		// A visitor error means the expression shape was not understood, which
		// is a reason to skip pushdown rather than to fail the scan.
		return nil, nil
	}

	switch res.kind {
	case convOK:
		return res.expr, nil
	case convAlwaysFalse:
		// Worth pushing: it lets Vortex skip the file's data entirely.
		lit, err := arena.LiteralBool(false)
		if err != nil {
			return nil, err
		}

		return lit, nil
	default:
		return nil, nil
	}
}

func (c *vortexFilterConverter) VisitTrue() convResult { return resAlwaysTrue }

func (c *vortexFilterConverter) VisitFalse() convResult { return resAlwaysFalse }

func (c *vortexFilterConverter) VisitNot(convResult) convResult {
	// BuildVortexFilter normalizes NOT before visiting. Keep this conservative
	// if the visitor is ever used directly with a partially converted child.
	return resUnconvertible
}

func (c *vortexFilterConverter) VisitAnd(left, right convResult) convResult {
	// AlwaysFalse short-circuits; AlwaysTrue and Unconvertible both drop out of
	// a conjunction without changing its meaning.
	if left.kind == convAlwaysFalse || right.kind == convAlwaysFalse {
		return resAlwaysFalse
	}

	l, lUsable := left.usable()
	r, rUsable := right.usable()

	switch {
	case lUsable && rUsable:
		e, err := c.arena.And([]*vortexffi.Expr{l, r})
		if err != nil {
			return resUnconvertible
		}

		return converted(e)
	case lUsable:
		return converted(l)
	case rUsable:
		return converted(r)
	default:
		// Neither side contributed. Preserve AlwaysTrue only if that is what
		// both sides actually were.
		if left.kind == convAlwaysTrue && right.kind == convAlwaysTrue {
			return resAlwaysTrue
		}

		return resUnconvertible
	}
}

func (c *vortexFilterConverter) VisitOr(left, right convResult) convResult {
	if left.kind == convAlwaysTrue || right.kind == convAlwaysTrue {
		return resAlwaysTrue
	}
	// A disjunction is only as good as its weakest arm: if either side cannot be
	// expressed, the OR could match rows the translated side excludes, so the
	// whole thing has to be given up.
	if left.kind == convUnconvertible || right.kind == convUnconvertible {
		return resUnconvertible
	}
	if left.kind == convAlwaysFalse && right.kind == convAlwaysFalse {
		return resAlwaysFalse
	}

	l, lUsable := left.usable()
	r, rUsable := right.usable()
	switch {
	case lUsable && rUsable:
		e, err := c.arena.Or([]*vortexffi.Expr{l, r})
		if err != nil {
			return resUnconvertible
		}

		return converted(e)
	case lUsable:
		return converted(l)
	case rUsable:
		return converted(r)
	default:
		return resUnconvertible
	}
}

// usable reports whether a result carries an expression that can be composed.
func (r convResult) usable() (*vortexffi.Expr, bool) {
	return r.expr, r.kind == convOK && r.expr != nil
}

func (c *vortexFilterConverter) VisitUnbound(iceberg.UnboundPredicate) convResult {
	// Callers bind the filter to the file schema before pushdown, so an unbound
	// predicate here means the caller is misusing the converter; skip it rather
	// than binding with a guessed case sensitivity.
	return resUnconvertible
}

func (c *vortexFilterConverter) VisitBound(pred iceberg.BoundPredicate) convResult {
	return iceberg.VisitBoundPredicate(pred, c)
}

// column resolves a bound term to a Vortex column expression, using the field's
// dotted path in the file schema so nested fields are reachable.
func (c *vortexFilterConverter) column(term iceberg.BoundTerm) (*vortexffi.Expr, bool) {
	if _, ok := term.(iceberg.BoundReference); !ok {
		return nil, false
	}
	ref := term.Ref()
	if ref == nil {
		return nil, false
	}

	path, ok := fieldPath(c.sc, ref.Field().ID)
	if !ok {
		return nil, false
	}

	e, err := c.arena.Column(path)
	if err != nil {
		return nil, false
	}

	return e, true
}

// fieldPath finds the dotted name path of a field ID within a schema, walking
// only struct nesting. List and map element IDs are not reachable by name path.
func fieldPath(sc *iceberg.Schema, fieldID int) ([]string, bool) {
	var walk func(fields []iceberg.NestedField, prefix []string) ([]string, bool)
	walk = func(fields []iceberg.NestedField, prefix []string) ([]string, bool) {
		for _, f := range fields {
			path := append(prefix, f.Name)
			if f.ID == fieldID {
				return path, true
			}

			if st, isStruct := f.Type.(*iceberg.StructType); isStruct {
				if found, ok := walk(st.FieldList, path); ok {
					return found, true
				}
			}
		}

		return nil, false
	}

	return walk(sc.Fields(), nil)
}

// literal converts an Iceberg literal to a Vortex literal expression.
//
// Date, time, timestamp and UUID values are deliberately absent: they are Vortex
// extension dtypes, and the C FFI has no constructor for an extension-typed
// scalar, so a comparison against one is rejected at scan time with a dtype
// mismatch. Tracked upstream as vortex-data/vortex#8702. Predicates over those
// types therefore stay unconvertible and are simply not pushed down.
func (c *vortexFilterConverter) literal(lit iceberg.Literal, typ iceberg.Type) (*vortexffi.Expr, bool) {
	var (
		e   *vortexffi.Expr
		err error
	)

	switch v := lit.Any().(type) {
	case bool:
		e, err = c.arena.LiteralBool(v)
	case int32:
		e, err = c.arena.LiteralInt32(v)
	case int64:
		e, err = c.arena.LiteralInt64(v)
	case float32:
		e, err = c.arena.LiteralFloat32(v)
	case float64:
		e, err = c.arena.LiteralFloat64(v)
	case string:
		e, err = c.arena.LiteralString(v)
	case []byte:
		e, err = c.arena.LiteralBinary(v)
	case iceberg.Decimal:
		dec, isDec := typ.(iceberg.DecimalType)
		if !isDec {
			return nil, false
		}
		// BigInt is the exact unscaled value, so 128-bit decimals survive
		// intact rather than being truncated to 64 bits.
		e, err = c.arena.LiteralDecimal(v.Val.BigInt(), dec.Precision(), dec.Scale())
	default:
		return nil, false
	}

	if err != nil {
		return nil, false
	}

	return e, true
}

func (c *vortexFilterConverter) binary(op vortexffi.BinaryOp, term iceberg.BoundTerm,
	lit iceberg.Literal,
) convResult {
	// Vortex comparisons distinguish signed zeros and compare NaN bit patterns;
	// Arrow's numeric comparisons do not. A residual cannot recover matching
	// rows removed here, so leave floating comparisons entirely to Arrow.
	// Set membership is handled separately and uses Arrow's bitwise semantics.
	switch term.Type().(type) {
	case iceberg.Float32Type, iceberg.Float64Type:
		return resUnconvertible
	}

	col, ok := c.column(term)
	if !ok {
		return resUnconvertible
	}

	val, ok := c.literal(lit, term.Type())
	if !ok {
		return resUnconvertible
	}

	e, err := c.arena.Binary(op, col, val)
	if err != nil {
		return resUnconvertible
	}

	return converted(e)
}

func (c *vortexFilterConverter) VisitEqual(t iceberg.BoundTerm, l iceberg.Literal) convResult {
	return c.binary(vortexffi.OpEqual, t, l)
}

func (c *vortexFilterConverter) VisitNotEqual(t iceberg.BoundTerm, l iceberg.Literal) convResult {
	return c.binary(vortexffi.OpNotEqual, t, l)
}

func (c *vortexFilterConverter) VisitGreater(t iceberg.BoundTerm, l iceberg.Literal) convResult {
	return c.binary(vortexffi.OpGreater, t, l)
}

func (c *vortexFilterConverter) VisitGreaterEqual(t iceberg.BoundTerm, l iceberg.Literal) convResult {
	return c.binary(vortexffi.OpGreaterEqual, t, l)
}

func (c *vortexFilterConverter) VisitLess(t iceberg.BoundTerm, l iceberg.Literal) convResult {
	return c.binary(vortexffi.OpLess, t, l)
}

func (c *vortexFilterConverter) VisitLessEqual(t iceberg.BoundTerm, l iceberg.Literal) convResult {
	return c.binary(vortexffi.OpLessEqual, t, l)
}

func (c *vortexFilterConverter) VisitIsNull(t iceberg.BoundTerm) convResult {
	col, found := c.column(t)
	if !found {
		return resUnconvertible
	}

	e, err := c.arena.IsNull(col)
	if err != nil {
		return resUnconvertible
	}

	return converted(e)
}

func (c *vortexFilterConverter) VisitNotNull(t iceberg.BoundTerm) convResult {
	isNull := c.VisitIsNull(t)
	if isNull.kind != convOK {
		return resUnconvertible
	}

	e, err := c.arena.Not(isNull.expr)
	if err != nil {
		return resUnconvertible
	}

	return converted(e)
}

func (c *vortexFilterConverter) VisitIn(t iceberg.BoundTerm, lits iceberg.Set[iceberg.Literal]) convResult {
	if lits.Len() > setPredicateLimit {
		return resUnconvertible
	}

	col, found := c.column(t)
	if !found {
		return resUnconvertible
	}

	eqs := make([]*vortexffi.Expr, 0, lits.Len())
	for _, lit := range lits.Members() {
		val, converted := c.literal(lit, t.Type())
		if !converted {
			return resUnconvertible
		}

		eq, err := c.arena.Binary(vortexffi.OpEqual, col, val)
		if err != nil {
			return resUnconvertible
		}
		eqs = append(eqs, eq)
	}

	if len(eqs) == 0 {
		return resAlwaysFalse
	}

	e, err := c.arena.Or(eqs)
	if err != nil {
		return resUnconvertible
	}

	return converted(e)
}

func (c *vortexFilterConverter) VisitNotIn(t iceberg.BoundTerm, lits iceberg.Set[iceberg.Literal]) convResult {
	in := c.VisitIn(t, lits)
	if in.kind != convOK {
		return resUnconvertible
	}
	negated, err := c.arena.Not(in.expr)
	if err != nil {
		return resUnconvertible
	}
	// Arrow's is_in returns false for a null input, so Iceberg's NOT IN
	// retains that row. Rust's NOT(OR(equal)) instead yields null. Preserve
	// null inputs explicitly so pushdown cannot remove residual matches.
	isNull := c.VisitIsNull(t)
	if isNull.kind != convOK {
		return resUnconvertible
	}
	result, err := c.arena.Or([]*vortexffi.Expr{isNull.expr, negated})
	if err != nil {
		return resUnconvertible
	}

	return converted(result)
}

// NaN and prefix predicates have no Vortex equivalent reachable through the C
// FFI: there is no is_nan primitive, and no LIKE or starts_with expression
// (the JNI bindings expose Expression.like, the C API does not).

func (c *vortexFilterConverter) VisitIsNan(iceberg.BoundTerm) convResult { return resUnconvertible }

func (c *vortexFilterConverter) VisitNotNan(iceberg.BoundTerm) convResult { return resUnconvertible }

func (c *vortexFilterConverter) VisitStartsWith(iceberg.BoundTerm, iceberg.Literal) convResult {
	return resUnconvertible
}

func (c *vortexFilterConverter) VisitNotStartsWith(iceberg.BoundTerm, iceberg.Literal) convResult {
	return resUnconvertible
}
