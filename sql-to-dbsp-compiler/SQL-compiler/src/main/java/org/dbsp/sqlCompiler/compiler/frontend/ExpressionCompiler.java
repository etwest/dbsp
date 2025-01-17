/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustSqlRuntimeLibrary;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public class ExpressionCompiler extends RexVisitorImpl<DBSPExpression> implements IModule, ICompilerComponent {
    private final TypeCompiler typeCompiler;
    @Nullable
    public final DBSPVariablePath inputRow;
    private final RexBuilder rexBuilder;
    private final List<RexLiteral> constants;
    private final DBSPCompiler compiler;

    public ExpressionCompiler(@Nullable DBSPVariablePath inputRow, DBSPCompiler compiler) {
        this(inputRow, Linq.list(), compiler);
    }

    /**
     * Create a compiler that will translate expressions pertaining to a row.
     * @param inputRow         Variable representing the row being compiled.
     * @param constants        Additional constants.  Expressions compiled
     *                         may use RexInputRef, which are field references
     *                         within the row.  Calcite seems to number constants
     *                         as additional fields within the row, after the end of
     *                         the input row.
     * @param compiler         Handle to the compiler.
     */
    public ExpressionCompiler(@Nullable DBSPVariablePath inputRow,
                              List<RexLiteral> constants,
                              DBSPCompiler compiler) {
        super(true);
        this.inputRow = inputRow;
        this.constants = constants;
        this.rexBuilder = compiler.frontend.getRexBuilder();
        this.compiler = compiler;
        this.typeCompiler = compiler.getTypeCompiler();
        if (inputRow != null &&
                !inputRow.getNonVoidType().is(DBSPTypeRef.class))
            throw new TranslationException("Expected a reference type for row", inputRow.getNode());
    }

    /**
     * Convert an expression that refers to a field in the input row.
     * @param inputRef   index in the input row.
     * @return           the corresponding DBSP expression.
     */
    @Override
    public DBSPExpression visitInputRef(RexInputRef inputRef) {
        if (this.inputRow == null)
            throw new RuntimeException("Row referenced without a row context");
        // Unfortunately it looks like we can't trust the type coming from Calcite.
        DBSPTypeTuple type = this.inputRow.getNonVoidType().deref().to(DBSPTypeTuple.class);
        int index = inputRef.getIndex();
        if (index < type.size()) {
            return new DBSPFieldExpression(
                    inputRef, this.inputRow,
                    inputRef.getIndex()).applyCloneIfNeeded();
        }
        if (index - type.size() < this.constants.size())
            return this.visitLiteral(this.constants.get(index - type.size()));
        throw new TranslationException("Index in row out of bounds ", inputRef);
    }

    @Override
    public DBSPExpression visitLiteral(RexLiteral literal) {
        try {
            DBSPType type = this.typeCompiler.convertType(literal.getType());
            if (literal.isNull())
                return DBSPLiteral.none(type);
            if (type.is(DBSPTypeInteger.class)) {
                DBSPTypeInteger intType = type.to(DBSPTypeInteger.class);
                switch (intType.getWidth()) {
                    case 16:
                        return new DBSPI32Literal(Objects.requireNonNull(literal.getValueAs(Short.class)));
                    case 32:
                        return new DBSPI32Literal(Objects.requireNonNull(literal.getValueAs(Integer.class)));
                    case 64:
                        return new DBSPI64Literal(Objects.requireNonNull(literal.getValueAs(Long.class)));
                    default:
                        throw new UnsupportedOperationException("Unsupported integer width type " + intType.getWidth());
                }
            } else if (type.is(DBSPTypeDouble.class))
                return new DBSPDoubleLiteral(Objects.requireNonNull(literal.getValueAs(Double.class)));
            else if (type.is(DBSPTypeFloat.class))
                return new DBSPFloatLiteral(Objects.requireNonNull(literal.getValueAs(Float.class)));
            else if (type.is(DBSPTypeString.class))
                return new DBSPStringLiteral(Objects.requireNonNull(literal.getValueAs(String.class)));
            else if (type.is(DBSPTypeBool.class))
                return new DBSPBoolLiteral(Objects.requireNonNull(literal.getValueAs(Boolean.class)));
            else if (type.is(DBSPTypeDecimal.class))
                return new DBSPDecimalLiteral(
                        literal, type, Objects.requireNonNull(literal.getValueAs(BigDecimal.class)));
            else if (type.is(DBSPTypeKeyword.class))
                return new DBSPKeywordLiteral(literal, Objects.requireNonNull(literal.getValue()).toString());
            else if (type.is(DBSPTypeMillisInterval.class))
                return new DBSPIntervalMillisLiteral(literal, type, Objects.requireNonNull(
                        literal.getValueAs(BigDecimal.class)).longValue());
            else if (type.is(DBSPTypeTimestamp.class)) {
                return new DBSPTimestampLiteral(literal, type,
                        Objects.requireNonNull(literal.getValueAs(TimestampString.class)));
            } else if (type.is(DBSPTypeDate.class)) {
                return new DBSPDateLiteral(literal, type, Objects.requireNonNull(literal.getValueAs(DateString.class)));
            } else if (type.is(DBSPTypeGeoPoint.class)) {
                Point point = literal.getValueAs(Point.class);
                Coordinate c = Objects.requireNonNull(point).getCoordinate();
                return new DBSPGeoPointLiteral(literal,
                        new DBSPDoubleLiteral(c.getOrdinate(0)),
                        new DBSPDoubleLiteral(c.getOrdinate(1)));
            }
        } catch (Throwable ex) {
            throw new Unimplemented(literal, ex);
        }
        throw new Unimplemented(literal);
    }

    /**
     * Given operands for "operation" with left and right types,
     * compute the type that both operands must be cast to.
     * Note: this ignores nullability of types.
     * @param left       Left operand type.
     * @param right      Right operand type.
     * @return           Common type operands must be cast to.
     */
    public static DBSPType reduceType(DBSPType left, DBSPType right) {
        if (left.is(DBSPTypeNull.class))
            return right.setMayBeNull(true);
        if (right.is(DBSPTypeNull.class))
            return left.setMayBeNull(true);
        left = left.setMayBeNull(false);
        right = right.setMayBeNull(false);
        if (left.sameType(right))
            return left;

        DBSPTypeInteger li = left.as(DBSPTypeInteger.class);
        DBSPTypeInteger ri = right.as(DBSPTypeInteger.class);
        DBSPTypeDecimal ld = left.as(DBSPTypeDecimal.class);
        DBSPTypeDecimal rd = right.as(DBSPTypeDecimal.class);
        DBSPTypeFP lf = left.as(DBSPTypeFP.class);
        DBSPTypeFP rf = right.as(DBSPTypeFP.class);
        if (li != null) {
            if (ri != null)
                return new DBSPTypeInteger(null, Math.max(li.getWidth(), ri.getWidth()), true,false);
            if (rf != null || rd != null)
                return right.setMayBeNull(false);
        }
        if (lf != null) {
            if (ri != null || rd != null)
                return left.setMayBeNull(false);
            if (rf != null) {
                if (lf.getWidth() < rf.getWidth())
                    return right.setMayBeNull(false);
                else
                    return left.setMayBeNull(false);
            }
        }
        if (ld != null) {
            if (ri != null)
                return left.setMayBeNull(false);
            if (rf != null)
                return right.setMayBeNull(false);
        }
        throw new Unimplemented("Cast from " + right + " to " + left);
    }

    public static DBSPExpression aggregateOperation(
            SqlOperator node, DBSPOpcode op,
            DBSPType type, DBSPExpression left, DBSPExpression right) {
        DBSPType leftType = left.getNonVoidType();
        DBSPType rightType = right.getNonVoidType();
        DBSPType commonBase = reduceType(leftType, rightType);
        if (commonBase.is(DBSPTypeNull.class)) {
            return DBSPLiteral.none(type);
        }
        DBSPType resultType = commonBase.setMayBeNull(leftType.mayBeNull || rightType.mayBeNull);
        DBSPExpression binOp = new DBSPBinaryExpression(node, resultType, op, left, right);
        return binOp.cast(type);
    }

    // Like makeBinaryExpression, but accepts multiple operands.
    private static DBSPExpression makeBinaryExpressions(
            Object node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        if (operands.size() < 2)
            throw new Unimplemented(node);
        DBSPExpression accumulator = operands.get(0);
        for (int i = 1; i < operands.size(); i++)
            accumulator = makeBinaryExpression(node, type, opcode, Linq.list(accumulator, operands.get(i)));
        return accumulator.cast(type);
    }

    public static boolean needCommonType(DBSPType result, DBSPType left, DBSPType right) {
        return !left.is(IsDateType.class) && !right.is(IsDateType.class);
    }

    public static DBSPExpression makeBinaryExpression(
            Object node, DBSPType type, DBSPOpcode opcode, List<DBSPExpression> operands) {
        // Why doesn't Calcite do this?
        if (operands.size() != 2)
            throw new TranslationException("Expected 2 operands, got " + operands.size(), node);
        DBSPExpression left = operands.get(0);
        DBSPExpression right = operands.get(1);
        if (left == null || right == null)
            throw new Unimplemented(node);
        DBSPType leftType = left.getNonVoidType();
        DBSPType rightType = right.getNonVoidType();

        if (needCommonType(type, leftType, rightType)) {
            DBSPType commonBase = reduceType(leftType, rightType);
            if (commonBase.is(DBSPTypeNull.class)) {
                // Result is always NULL.  Perhaps we should give a warning?
                return DBSPLiteral.none(type);
            }
            if (!leftType.setMayBeNull(false).sameType(commonBase))
                left = left.cast(commonBase.setMayBeNull(leftType.mayBeNull));
            if (!rightType.setMayBeNull(false).sameType(commonBase))
                right = right.cast(commonBase.setMayBeNull(rightType.mayBeNull));
        }
        // TODO: we don't need the whole function here, just the result type.
        RustSqlRuntimeLibrary.FunctionDescription function = RustSqlRuntimeLibrary.INSTANCE.getImplementation(
                opcode, type, left.getNonVoidType(), right.getNonVoidType());
        DBSPExpression call = new DBSPBinaryExpression(node, function.returnType, opcode, left, right);
        return call.cast(type);
    }

    public static DBSPExpression makeUnaryExpression(
            Object node, DBSPType type, DBSPOpcode op, List<DBSPExpression> operands) {
        if (operands.size() != 1)
            throw new TranslationException("Expected 1 operands, got " + operands.size(), node);
        DBSPExpression operand = operands.get(0);
        if (operand == null)
            throw new Unimplemented("Found unimplemented expression in " + node);
        DBSPType resultType = operand.getNonVoidType();
        if (op.toString().startsWith("is_"))
            // these do not produce nullable results
            resultType = resultType.setMayBeNull(false);
        DBSPExpression expr = new DBSPUnaryExpression(node, resultType, op, operand);
        return expr.cast(type);
    }

    public static DBSPExpression wrapBoolIfNeeded(DBSPExpression expression) {
        DBSPType type = expression.getNonVoidType();
        if (type.mayBeNull) {
            return new DBSPUnaryExpression(
                    expression.getNode(), type.setMayBeNull(false),
                    DBSPOpcode.WRAP_BOOL, expression);
        }
        return expression;
    }

    @Override
    public DBSPExpression visitCall(RexCall call) {
        Logger.INSTANCE.from(this, 2)
                .append(call.toString())
                .append(" ")
                .append(call.getType().toString());
        if (call.op.kind == SqlKind.SEARCH) {
            // TODO: ideally the optimizer should do this before handing the expression to us.
            // Then we can get rid of the rexBuilder field too.
            call = (RexCall)RexUtil.expandSearch(this.rexBuilder, null, call);
        }
        List<DBSPExpression> ops = Linq.map(call.operands, e -> e.accept(this));
        boolean anyNull = Linq.any(ops, o -> o.getNonVoidType().mayBeNull);
        DBSPType type = this.typeCompiler.convertType(call.getType());
        switch (call.op.kind) {
            case TIMES:
                return makeBinaryExpression(call, type, DBSPOpcode.MUL, ops);
            case DIVIDE:
                // We enforce that the type of the result of division is always nullable
                type = type.setMayBeNull(true);
                return makeBinaryExpression(call, type, DBSPOpcode.DIV, ops);
            case MOD:
                return makeBinaryExpression(call, type, DBSPOpcode.MOD, ops);
            case PLUS:
                return makeBinaryExpressions(call, type, DBSPOpcode.ADD, ops);
            case MINUS:
                return makeBinaryExpression(call, type, DBSPOpcode.SUB, ops);
            case LESS_THAN:
                return makeBinaryExpression(call, type, DBSPOpcode.LT, ops);
            case GREATER_THAN:
                return makeBinaryExpression(call, type, DBSPOpcode.GT, ops);
            case LESS_THAN_OR_EQUAL:
                return makeBinaryExpression(call, type, DBSPOpcode.LTE, ops);
            case GREATER_THAN_OR_EQUAL:
                return makeBinaryExpression(call, type, DBSPOpcode.GTE, ops);
            case EQUALS:
                return makeBinaryExpression(call, type, DBSPOpcode.EQ, ops);
            case IS_DISTINCT_FROM:
                return makeBinaryExpression(call, type, DBSPOpcode.IS_DISTINCT, ops);
            case IS_NOT_DISTINCT_FROM: {
                DBSPExpression op = makeBinaryExpression(call, type, DBSPOpcode.IS_DISTINCT, ops);
                return makeUnaryExpression(call, DBSPTypeBool.INSTANCE, DBSPOpcode.NOT, Linq.list(op));
            }
            case NOT_EQUALS:
                return makeBinaryExpression(call, type, DBSPOpcode.NEQ, ops);
            case OR:
                return makeBinaryExpressions(call, type, DBSPOpcode.OR, ops);
            case AND:
                return makeBinaryExpressions(call, type, DBSPOpcode.AND, ops);
            case NOT:
                return makeUnaryExpression(call, type, DBSPOpcode.NOT, ops);
            case IS_FALSE:
                return makeUnaryExpression(call, type, DBSPOpcode.IS_FALSE, ops);
            case IS_NOT_TRUE:
                return makeUnaryExpression(call, type, DBSPOpcode.IS_NOT_TRUE, ops);
            case IS_TRUE:
                return makeUnaryExpression(call, type, DBSPOpcode.IS_TRUE, ops);
            case IS_NOT_FALSE:
                return makeUnaryExpression(call, type, DBSPOpcode.IS_NOT_FALSE, ops);
            case PLUS_PREFIX:
                return makeUnaryExpression(call, type, DBSPOpcode.UNARY_PLUS, ops);
            case MINUS_PREFIX:
                return makeUnaryExpression(call, type, DBSPOpcode.NEG, ops);
            case BIT_AND:
                return makeBinaryExpressions(call, type, DBSPOpcode.BW_AND, ops);
            case BIT_OR:
                return makeBinaryExpressions(call, type, DBSPOpcode.BW_OR, ops);
            case BIT_XOR:
                return makeBinaryExpressions(call, type, DBSPOpcode.XOR, ops);
            case CAST:
            case REINTERPRET:
                return ops.get(0).cast(type);
            case IS_NULL:
            case IS_NOT_NULL: {
                if (!type.sameType(DBSPTypeBool.INSTANCE))
                    throw new TranslationException("Expected expression to produce a boolean result", call);
                DBSPExpression arg = ops.get(0);
                DBSPType argType = arg.getNonVoidType();
                if (argType.mayBeNull) {
                    if (call.op.kind == SqlKind.IS_NULL)
                        return ops.get(0).is_null();
                    else
                        return new DBSPUnaryExpression(call, type, DBSPOpcode.NOT, ops.get(0).is_null());
                } else {
                    // Constant-fold
                    if (call.op.kind == SqlKind.IS_NULL)
                        return new DBSPBoolLiteral(false);
                    else
                        return new DBSPBoolLiteral(true);
                }
            }
            case CASE: {
                /*
                A switched case (CASE x WHEN x1 THEN v1 ... ELSE e END)
                has an even number of arguments and odd-numbered arguments are predicates.
                A condition case (CASE WHEN p1 THEN v1 ... ELSE e END) has an odd number of
                arguments and even-numbered arguments are predicates, except for the last argument.
                */
                DBSPExpression result = ops.get(ops.size() - 1);
                if (ops.size() % 2 == 0) {
                    DBSPExpression value = ops.get(0);
                    // Compute casts if needed.
                    DBSPType finalType = result.getNonVoidType();
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        if (ops.get(i + 1).getNonVoidType().mayBeNull)
                            finalType = finalType.setMayBeNull(true);
                    }
                    if (!result.getNonVoidType().sameType(finalType))
                        result = result.cast(finalType);
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        DBSPExpression alt = ops.get(i + 1);
                        if (!alt.getNonVoidType().sameType(finalType))
                            alt = alt.cast(finalType);
                        DBSPExpression comp = makeBinaryExpression(
                                call, DBSPTypeBool.INSTANCE, DBSPOpcode.EQ,
                                Linq.list(value, ops.get(i)));
                        comp = wrapBoolIfNeeded(comp);
                        result = new DBSPIfExpression(call, comp, alt, result);
                    }
                } else {
                    // Compute casts if needed.
                    // Build this backwards
                    DBSPType finalType = result.getNonVoidType();
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        if (ops.get(index).getNonVoidType().mayBeNull)
                            finalType = finalType.setMayBeNull(true);
                    }

                    if (!result.getNonVoidType().sameType(finalType))
                        result = result.cast(finalType);
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        DBSPExpression alt = ops.get(index);
                        if (!alt.getNonVoidType().sameType(finalType))
                            alt = alt.cast(finalType);
                        DBSPExpression condition = wrapBoolIfNeeded(ops.get(index - 1));
                        result = new DBSPIfExpression(call, condition, alt, result);
                    }
                }
                return result;
            }
            case ST_POINT: {
                if (ops.size() != 2)
                    throw new Unimplemented("Expected only 2 operands", call);
                DBSPExpression left = ops.get(0);
                DBSPExpression right = ops.get(1);
                String functionName = "make_geopoint" + type.nullableSuffix() +
                        "_d" + left.getNonVoidType().nullableSuffix() +
                        "_d" + right.getNonVoidType().nullableSuffix();
                return new DBSPApplyExpression(functionName, type, left, right);
            }
            case OTHER_FUNCTION: {
                String opName = call.op.getName().toLowerCase();
                switch (opName) {
                    case "truncate":
                    case "round": {
                        DBSPExpression right;
                        if (call.operands.size() < 1)
                            throw new Unimplemented(call);
                        DBSPExpression left = ops.get(0);
                        if (call.operands.size() == 1)
                            right = new DBSPI32Literal(0);
                        else
                            right = ops.get(1);
                        DBSPType leftType = left.getNonVoidType();
                        DBSPType rightType = right.getNonVoidType();
                        if (!rightType.is(DBSPTypeInteger.class))
                            throw new Unimplemented("ROUND expects a constant second argument", call);
                        String function = opName + "_" +
                                leftType.baseTypeWithSuffix();
                        return new DBSPApplyExpression(function, type, left, right);
                    }
                    case "numeric_inc":
                    case "sign":
                    case "log10":
                    case "ln":
                    case "abs": {
                        if (call.operands.size() != 1)
                            throw new Unimplemented(call);
                        DBSPExpression arg = ops.get(0);
                        DBSPType argType = arg.getNonVoidType();
                        String function = opName + "_" + argType.baseTypeWithSuffix();
                        return new DBSPApplyExpression(function, type, arg);
                    }
                    case "st_distance": {
                        if (call.operands.size() != 2)
                            throw new Unimplemented(call);
                        DBSPExpression left = ops.get(0);
                        DBSPExpression right = ops.get(1);
                        String function = "st_distance_" + left.getNonVoidType().nullableSuffix() +
                                "_" + right.getNonVoidType().nullableSuffix();
                        return new DBSPApplyExpression(function, DBSPTypeDouble.INSTANCE.setMayBeNull(anyNull), left, right);
                    }
                    case "division":
                        return makeBinaryExpression(call, type, DBSPOpcode.DIV, ops);
                    case "cardinality": {
                        if (call.operands.size() != 1)
                            throw new Unimplemented(call);
                        DBSPExpression arg = ops.get(0);
                        DBSPExpression len = new DBSPApplyMethodExpression("len", DBSPTypeUSize.INSTANCE, arg);
                        return len.cast(type);
                    }
                    case "element": {
                        type = type.setMayBeNull(true);  // Why isn't this always nullable?
                        DBSPExpression arg = ops.get(0);
                        DBSPTypeVec arrayType = arg.getNonVoidType().to(DBSPTypeVec.class);
                        String method = "element";
                        if (arrayType.getElementType().mayBeNull)
                            method += "N";
                        return new DBSPApplyExpression(method, type, arg);
                    }
                    case "power": {
                        if (call.operands.size() != 2)
                            throw new Unimplemented(call);
                        DBSPType leftType = ops.get(0).getNonVoidType();
                        DBSPType rightType = ops.get(1).getNonVoidType();
                        String functionName = "power_" + leftType.baseTypeWithSuffix() +
                                "_" + rightType.baseTypeWithSuffix();
                        return new DBSPApplyExpression(functionName, type, ops.get(0), ops.get(1));
                    }
                }
                throw new Unimplemented(call);
            }
            case OTHER:
                String opName = call.op.getName().toLowerCase();
                //noinspection SwitchStatementWithTooFewBranches
                switch (opName) {
                    case "||":
                        return makeBinaryExpression(call, type, DBSPOpcode.CONCAT, ops);
                    default:
                        break;
                }
                throw new Unimplemented(call);
            case EXTRACT: {
                if (call.operands.size() != 2)
                    throw new Unimplemented(call);
                DBSPKeywordLiteral keyword = ops.get(0).to(DBSPKeywordLiteral.class);
                DBSPType type1 = ops.get(1).getNonVoidType();
                String functionName = "extract_" + type1.to(IsNumericType.class).getRustString() +
                        "_" + keyword + type1.nullableSuffix();
                return new DBSPApplyExpression(functionName, type, ops.get(1));
            }
            case FLOOR:
            case CEIL: {
                if (call.operands.size() == 2) {
                    DBSPKeywordLiteral keyword = ops.get(1).to(DBSPKeywordLiteral.class);
                    String functionName = call.getKind().toString().toLowerCase() + "_" +
                            type.to(DBSPTypeBaseType.class).shortName() + "_" + keyword + type.nullableSuffix();
                    return new DBSPApplyExpression(functionName, type, ops.get(0));
                } else if (call.operands.size() == 1) {
                    String functionName = call.getKind().toString().toLowerCase() + "_" +
                            type.to(DBSPTypeBaseType.class).shortName() + type.nullableSuffix();
                    return new DBSPApplyExpression(functionName, type, ops.get(0));
                } else {
                    throw new Unimplemented(call);
                }
            }
            case ARRAY_VALUE_CONSTRUCTOR: {
                DBSPTypeVec vec = type.to(DBSPTypeVec.class);
                DBSPType elemType = vec.getElementType();
                List<DBSPExpression> args = Linq.map(ops, o -> o.cast(elemType));
                return new DBSPApplyExpression("vec!", type, args.toArray(new DBSPExpression[0]));
            }
            case ITEM: {
                if (call.operands.size() != 2)
                    throw new Unimplemented(call);
                return new DBSPIndexExpression(call, ops.get(0), ops.get(1).cast(DBSPTypeUSize.INSTANCE), true);
            }
            case DOT:
            default:
                throw new Unimplemented(call);
        }
    }

    DBSPExpression compile(RexNode expression) {
        Logger.INSTANCE.from(this, 3)
                .append("Compiling ")
                .append(expression.toString())
                .newline();
        DBSPExpression result = expression.accept(this);
        if (result == null)
            throw new Unimplemented(expression);
        return result;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
