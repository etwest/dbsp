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

import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.statements.*;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The compiler is stateful: it compiles a sequence of SQL statements
 * defining tables and views.  The views must be defined in terms of
 * the previously defined tables and views.  Multiple views can be
 * compiled.  The result is a circuit which has an input for each table
 * and an output for each view.
 * The function generateOutputForNextView can be used to prevent
 * some views from generating outputs.
 */
public class CalciteToDBSPCompiler extends RelVisitor
        implements IModule, ICompilerComponent {
    /**
     * Number of first day of the week.
     * This should be selected by the SQL dialect, but it seems
     * to be hardwired in the Calcite optimizer.
     */
    public static final int firstDOW = 1;

    // Result is deposited here
    private DBSPPartialCircuit circuit;
    // Map each compiled RelNode operator to its DBSP implementation.
    final Map<RelNode, DBSPOperator> nodeOperator;
    final TableContents tableContents;
    final CompilerOptions options;
    final DBSPCompiler compiler;

    /**
     * Create a compiler that translated from calcite to DBSP circuits.
     * @param trackTableContents  If true this compiler will track INSERT and DELETE statements.
     * @param options             Options for compilation.
     * @param compiler            Parent compiler; used to report errors.
     */
    public CalciteToDBSPCompiler(boolean trackTableContents,
                                 CompilerOptions options, DBSPCompiler compiler) {
        this.circuit = new DBSPPartialCircuit(compiler);
        this.compiler = compiler;
        this.nodeOperator = new HashMap<>();
        this.tableContents = new TableContents(compiler, trackTableContents);
        this.options = options;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }

    private DBSPType convertType(RelDataType dt) {
        return this.compiler.getTypeCompiler().convertType(dt);
    }

    private DBSPType makeZSet(DBSPType type) {
        return TypeCompiler.makeZSet(type, DBSPTypeWeight.INSTANCE);
    }

    /**
     * Gets the circuit produced so far and starts a new one.
     */
    public DBSPPartialCircuit getFinalCircuit() {
        DBSPPartialCircuit result = this.circuit;
        this.circuit = new DBSPPartialCircuit(this.compiler);
        return result;
    }

    /**
     * This retrieves the operator that is an input.  If the operator may
     * produce multiset results and this is not desired (asMultiset = false),
     * a distinct operator is introduced in the circuit.
     */
    private DBSPOperator getInputAs(RelNode input, boolean asMultiset) {
        DBSPOperator op = this.getOperator(input);
        if (op.isMultiset && !asMultiset) {
            op = new DBSPDistinctOperator(input, op);
            this.circuit.addOperator(op);
        }
        return op;
    }

    <T> boolean visitIfMatches(RelNode node, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(node, clazz);
        if (value != null) {
            Logger.INSTANCE.from(this, 4)
                    .append("Processing ")
                    .append(node.toString())
                    .newline();
            method.accept(value);
            return true;
        }
        return false;
    }

    private boolean generateOutputForNextView = true;

    /**
     * @param generate
     * If 'false' the next "create view" statements will not generate
     * an output for the circuit.  This is sticky, it has to be
     * explicitly reset.
     */
    public void generateOutputForNextView(boolean generate) {
         this.generateOutputForNextView = generate;
    }

    /**
     * Helper function for creating aggregates.
     * @param node         RelNode that generates this aggregate.
     * @param aggregates   Aggregates to implement.
     * @param groupCount   Number of groupBy variables.
     * @param inputRowType Type of input row.
     * @param resultType   Type of result produced.
     */
    public DBSPAggregate createAggregate(RelNode node,
            List<AggregateCall> aggregates, DBSPTypeTuple resultType,
            DBSPType inputRowType, int groupCount) {
        DBSPVariablePath rowVar = inputRowType.ref().var("v");
        DBSPAggregate result = new DBSPAggregate(node, rowVar, aggregates.size());
        int aggIndex = 0;

        for (AggregateCall call: aggregates) {
            DBSPType resultFieldType = resultType.getFieldType(aggIndex + groupCount);
            AggregateCompiler compiler = new AggregateCompiler(
                    this.getCompiler(), call, resultFieldType, rowVar);
            DBSPAggregate.Implementation implementation = compiler.compile();
            result.set(aggIndex, implementation);
            aggIndex++;
        }
        return result;
    }

    /**
     * Given a struct type find the index of the specified field.
     * Throws if no such field exists.
     */
    static int getFieldIndex(String field, RelDataType type) {
        int index = 0;
        for (RelDataTypeField rowField: type.getFieldList()) {
            if (rowField.getName().equals(field))
                return index;
            index++;
        }
        throw new RuntimeException("Type " + type + " has no field named " + field);
    }

    public void visitCorrelate(LogicalCorrelate correlate) {
        // We decorrelate queries using Calcite's optimizer.
        // So we assume that the only correlated queries we receive
        // are unnest-type queries.  We assume that unnest queries
        // have a restricted plan of this form:
        // LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
        //    LeftSubquery
        //    Uncollect
        //      LogicalProject(COL=[$cor0.ARRAY])
        //        LogicalValues(tuples=[[{ 0 }]])
        // Instead of projecting and joining again we directly apply flatmap.
        // The translation for this is:
        // stream.flat_map({
        //   move |x: &Tuple2<Vec<i32>, Option<i32>>, | -> _ {
        //     let xA: Vec<i32> = x.0.clone();
        //     let xB: x.1.clone();
        //     x.0.clone().into_iter().map({
        //        move |e: i32, | -> Tuple3<Vec<i32>, Option<i32>, i32> {
        //            Tuple3::new(xA.clone(), xB.clone(), e)
        //        }
        //     })
        //  });
        DBSPTypeTuple type = this.convertType(correlate.getRowType()).to(DBSPTypeTuple.class);
        if (correlate.getJoinType().isOuterJoin())
            throw new Unimplemented(correlate);
        this.visit(correlate.getLeft(), 0, correlate);
        DBSPOperator left = this.getInputAs(correlate.getLeft(), true);
        DBSPTypeTuple leftElementType = left.getOutputZSetElementType();

        RelNode correlateRight = correlate.getRight();
        if (!(correlateRight instanceof Uncollect))
            throw new Unimplemented(correlate);
        Uncollect uncollect = (Uncollect) correlateRight;
        RelNode uncollectInput = uncollect.getInput();
        if (!(uncollectInput instanceof LogicalProject))
            throw new Unimplemented(correlate);
        LogicalProject project = (LogicalProject) uncollectInput;
        if (project.getProjects().size() != 1)
            throw new Unimplemented(correlate);
        RexNode projection = project.getProjects().get(0);
        if (!(projection instanceof RexFieldAccess))
            throw new Unimplemented(correlate);
        RexFieldAccess field = (RexFieldAccess) projection;
        RelDataType leftRowType = correlate.getLeft().getRowType();
        // The index of the field that is the array
        int arrayFieldIndex = getFieldIndex(field.getField().getName(), leftRowType);
        List<Integer> allFields = IntStream.range(0, leftElementType.size())
                .boxed()
                .collect(Collectors.toList());
        DBSPType indexType = null;
        if (uncollect.withOrdinality) {
            // Index field is always last
            indexType = type.getFieldType(type.size() - 1);
        }
        DBSPFlatmap flatmap = new DBSPFlatmap(correlate, leftElementType, arrayFieldIndex,
                allFields, indexType);
        DBSPFlatMapOperator flatMap = new DBSPFlatMapOperator(uncollect,
                flatmap, TypeCompiler.makeZSet(type, DBSPTypeWeight.INSTANCE), left);
        this.assignOperator(correlate, flatMap);
    }

    public void visitUncollect(Uncollect uncollect) {
        // This represents an unnest.
        // flat_map(move |x| { x.0.into_iter().map(move |e| Tuple1::new(e)) })
        DBSPType type = this.convertType(uncollect.getRowType());
        RelNode input = uncollect.getInput();
        DBSPTypeTuple inputRowType = this.convertType(input.getRowType()).to(DBSPTypeTuple.class);
        // We expect this to be a single-element tuple whose type is a vector.
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType indexType = null;
        if (uncollect.withOrdinality) {
            DBSPTypeTuple pair = type.to(DBSPTypeTuple.class);
            indexType = pair.getFieldType(1);
        }
        DBSPExpression function = new DBSPFlatmap(uncollect, inputRowType, 0,
                Linq.list(), indexType);
        DBSPFlatMapOperator flatMap = new DBSPFlatMapOperator(uncollect, function,
                TypeCompiler.makeZSet(type, DBSPTypeWeight.INSTANCE), opInput);
        this.assignOperator(uncollect, flatMap);
    }

    public void visitAggregate(LogicalAggregate aggregate) {
        DBSPType type = this.convertType(aggregate.getRowType());
        DBSPTypeTuple tuple = type.to(DBSPTypeTuple.class);
        RelNode input = aggregate.getInput();
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType inputRowType = this.convertType(input.getRowType());
        List<AggregateCall> aggregates = aggregate.getAggCallList();
        DBSPVariablePath t = inputRowType.ref().var("t");

        if (!aggregates.isEmpty()) {
            if (aggregate.getGroupType() != org.apache.calcite.rel.core.Aggregate.Group.SIMPLE)
                throw new Unimplemented(aggregate);
            DBSPExpression[] groups = new DBSPExpression[aggregate.getGroupCount()];
            int next = 0;
            for (int index: aggregate.getGroupSet()) {
                groups[next] = t.field(index);
                next++;
            }
            DBSPExpression keyExpression = new DBSPRawTupleExpression(groups);
            DBSPType[] aggTypes = Utilities.arraySlice(tuple.tupFields, aggregate.getGroupCount());
            DBSPTypeTuple aggType = new DBSPTypeTuple(aggTypes);

            DBSPExpression groupKeys =
                    new DBSPRawTupleExpression(
                            keyExpression,
                            DBSPTupleExpression.flatten(t)).closure(
                    t.asParameter());
            DBSPIndexOperator index = new DBSPIndexOperator(
                    aggregate, this.declare("index", groupKeys),
                    keyExpression.getNonVoidType(), inputRowType, DBSPTypeWeight.INSTANCE, false, opInput);
            this.circuit.addOperator(index);
            DBSPType groupType = keyExpression.getNonVoidType();
            DBSPAggregate fold = this.createAggregate(aggregate, aggregates, tuple, inputRowType, aggregate.getGroupCount());
            // The aggregate operator will not return a stream of type aggType, but a stream
            // with a type given by fd.defaultZero.
            DBSPTypeTuple typeFromAggregate = fold.defaultZeroType();
            DBSPAggregateOperator agg = new DBSPAggregateOperator(aggregate, groupType,
                    typeFromAggregate, DBSPTypeWeight.INSTANCE, null, fold, index);

            // Flatten the resulting set
            DBSPTypeRawTuple kvType = new DBSPTypeRawTuple(groupType.ref(), typeFromAggregate.ref());
            DBSPVariablePath kv = kvType.var("kv");
            DBSPExpression[] flattenFields = new DBSPExpression[aggregate.getGroupCount() + aggType.size()];
            for (int i = 0; i < aggregate.getGroupCount(); i++)
                flattenFields[i] = kv.field(0).field(i);
            for (int i = 0; i < aggType.size(); i++) {
                DBSPExpression flattenField = kv.field(1).field(i);
                // Here we correct from the type produced by the Folder (typeFromAggregate) to the
                // actual expected type aggType (which is the tuple of aggTypes).
                flattenFields[aggregate.getGroupCount() + i] = flattenField.cast(aggTypes[i]);
            }
            DBSPExpression mapper = new DBSPTupleExpression(flattenFields).closure(kv.asParameter());
            this.circuit.addOperator(agg);
            DBSPMapOperator map = new DBSPMapOperator(aggregate,
                    this.declare("flatten", mapper), tuple, DBSPTypeWeight.INSTANCE, agg);
            if (aggregate.getGroupCount() == 0) {
                // This almost works, but we have a problem with empty input collections
                // for aggregates without grouping.
                // aggregate_stream returns empty collections for empty input collections -- the fold
                // method is never invoked.
                // So we need to do some postprocessing step for this case.
                // The current result is a zset like {}/{c->1}: either the empty set (for an empty input)
                // or the correct count with a weight of 1.
                // We need to produce {z->1}/{c->1}, where z is the actual zero of the fold above.
                // For this we synthesize the following graph:
                // {}/{c->1}------------------------
                //    | map (|x| x -> z}           |
                // {}/{z->1}                       |
                //    | -                          |
                // {} {z->-1}   {z->1} (constant)  |
                //          \  /                  /
                //           +                   /
                //         {z->1}/{}  -----------
                //                 \ /
                //                  +
                //              {z->1}/{c->1}
                this.circuit.addOperator(map);
                DBSPVariablePath _t = tuple.var("_t");
                DBSPExpression toZero = fold.defaultZero().closure(_t.asRefParameter());
                DBSPOperator map1 = new DBSPMapOperator(aggregate, toZero, type, DBSPTypeWeight.INSTANCE, map);
                this.circuit.addOperator(map1);
                DBSPOperator neg = new DBSPNegateOperator(aggregate, map1);
                this.circuit.addOperator(neg);
                DBSPOperator constant = new DBSPConstantOperator(
                        aggregate, new DBSPZSetLiteral(DBSPTypeWeight.INSTANCE, fold.defaultZero()), false);
                this.circuit.addOperator(constant);
                DBSPOperator sum = new DBSPSumOperator(aggregate, Linq.list(constant, neg, map));
                this.assignOperator(aggregate, sum);
            } else {
                this.assignOperator(aggregate, map);
            }
        } else {
            DBSPOperator dist = new DBSPDistinctOperator(aggregate, opInput);
            this.assignOperator(aggregate, dist);
        }
    }

    public void visitScan(LogicalTableScan scan) {
        List<String> name = scan.getTable().getQualifiedName();
        String tableName = name.get(name.size() - 1);
        @Nullable
        DBSPOperator source = this.circuit.getOperator(tableName);
        if (source != null) {
            if (source.is(DBSPSinkOperator.class))
                // We do this because sink operators do not have outputs.
                // A table scan for a sink operator can appear because of
                // a VIEW that is an input to a query.
                Utilities.putNew(this.nodeOperator, scan, source.to(DBSPSinkOperator.class).input());
            else
                // Multiple queries can share an input.
                // Or the input may have been created by a CREATE TABLE statement.
                Utilities.putNew(this.nodeOperator, scan, source);
            return;
        }

        if (this.options.optimizerOptions.generateInputForEveryTable)
            throw new RuntimeException("Could not find input for table " + tableName);
        @Nullable String comment = null;
        if (scan.getTable() instanceof RelOptTableImpl) {
            RelOptTableImpl impl = (RelOptTableImpl) scan.getTable();
            CreateRelationStatement.EmulatedTable et = impl.unwrap(CreateRelationStatement.EmulatedTable.class);
            if (et != null)
                comment = et.getStatement();
        }
        DBSPType rowType = this.convertType(scan.getRowType());
        DBSPSourceOperator result = new DBSPSourceOperator(scan, this.makeZSet(rowType), comment, tableName);
        this.assignOperator(scan, result);
    }

    void assignOperator(RelNode rel, DBSPOperator op) {
        Utilities.putNew(this.nodeOperator, rel, op);
        this.circuit.addOperator(op);
    }

    DBSPOperator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    public void visitProject(LogicalProject project) {
        // LogicalProject is not really SQL project, it is rather map.
        RelNode input = project.getInput();
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType outputType = this.convertType(project.getRowType());
        DBSPTypeTuple tuple = outputType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType());
        DBSPVariablePath row = inputType.ref().var("t");
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(row, this.compiler);

        List<DBSPExpression> resultColumns = new ArrayList<>();
        int index = 0;
        for (RexNode column : project.getProjects()) {
            if (column instanceof RexOver) {
                throw new UnsupportedException("Optimizer should have removed OVER expressions", column);
            } else {
                DBSPExpression exp = expressionCompiler.compile(column);
                DBSPType expectedType = tuple.getFieldType(index);
                if (!exp.getNonVoidType().sameType(expectedType)) {
                    // Calcite's optimizations do not preserve types!
                    exp = exp.cast(expectedType);
                }
                resultColumns.add(exp);
                index++;
            }
        }
        DBSPExpression exp = new DBSPTupleExpression(project, resultColumns);
        DBSPExpression closure = new DBSPClosureExpression(project, exp, row.asParameter());
        DBSPExpression mapFunc = this.declare("map", closure);
        DBSPMapOperator op = new DBSPMapOperator(
                project, mapFunc, outputType, DBSPTypeWeight.INSTANCE, opInput);
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    DBSPOperator castOutput(RelNode node, DBSPOperator operator, DBSPType outputElementType) {
        DBSPType inputElementType = operator.getOutputZSetElementType();
        if (inputElementType.sameType(outputElementType))
            return operator;
        DBSPExpression function = inputElementType.caster(outputElementType);
        DBSPOperator map = new DBSPMapOperator(
                node, function, outputElementType, DBSPTypeWeight.INSTANCE, operator);
        this.circuit.addOperator(map);
        return map;
    }

    private void visitUnion(LogicalUnion union) {
        RelDataType rowType = union.getRowType();
        DBSPType outputType = this.convertType(rowType);
        List<DBSPOperator> inputs = Linq.map(union.getInputs(), this::getOperator);
        // input type nullability may not match
        inputs = Linq.map(inputs, o -> this.castOutput(union, o, outputType));
        DBSPSumOperator sum = new DBSPSumOperator(union, inputs);
        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(union, sum);
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        boolean first = true;
        RelDataType rowType = minus.getRowType();
        DBSPType outputType = this.convertType(rowType);
        List<DBSPOperator> inputs = new ArrayList<>();
        for (RelNode input : minus.getInputs()) {
            DBSPOperator opInput = this.getInputAs(input, false);
            if (!first) {
                DBSPOperator neg = new DBSPNegateOperator(minus, opInput);
                neg = this.castOutput(minus, neg, outputType);
                this.circuit.addOperator(neg);
                inputs.add(neg);
            } else {
                opInput = this.castOutput(minus, opInput, outputType);
                inputs.add(opInput);
            }
            first = false;
        }

        DBSPSumOperator sum = new DBSPSumOperator(minus, inputs);
        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(minus, sum);
            this.assignOperator(minus, d);
        }
    }

    public DBSPExpression declare(String prefix, DBSPExpression closure) {
        // return this.circuit.declareLocal(prefix, closure).getVarReference();
        return closure;
    }

    public void visitFilter(LogicalFilter filter) {
        DBSPType type = this.convertType(filter.getRowType());
        DBSPVariablePath t = type.ref().var("t");
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.compiler);
        DBSPExpression condition = expressionCompiler.compile(filter.getCondition());
        condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
        condition = new DBSPClosureExpression(filter.getCondition(), condition, t.asParameter());
        DBSPOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(
                filter, this.declare("cond", condition), input);
        this.assignOperator(filter, fop);
    }

    private DBSPOperator filterNonNullKeys(LogicalJoin join,
            List<Integer> keyFields, DBSPOperator input) {
        DBSPTypeTuple rowType = input.getNonVoidType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        boolean shouldFilter = Linq.any(keyFields, i -> rowType.tupFields[i].mayBeNull);
        if (!shouldFilter) return input;

        DBSPVariablePath var = rowType.ref().var("r");
        // Build a condition that checks whether any of the key fields is null.
        @Nullable
        DBSPExpression condition = null;
        for (int i = 0; i < rowType.size(); i++) {
            if (keyFields.contains(i)) {
                DBSPFieldExpression field = new DBSPFieldExpression(join, var, i);
                DBSPExpression expr = field.is_null();
                if (condition == null)
                    condition = expr;
                else
                    condition = new DBSPBinaryExpression(
                            join, DBSPTypeBool.INSTANCE, DBSPOpcode.OR, condition, expr);
            }
        }

        Objects.requireNonNull(condition);
        condition = new DBSPUnaryExpression(join, condition.getNonVoidType(), DBSPOpcode.NOT, condition);
        DBSPClosureExpression filterFunc = condition.closure(var.asParameter());
        DBSPExpression ff = this.declare("filter", filterFunc);
        DBSPFilterOperator filter = new DBSPFilterOperator(join, ff, input);
        this.circuit.addOperator(filter);
        return filter;
    }

    private void visitJoin(LogicalJoin join) {
        JoinRelType joinType = join.getJoinType();
        if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI)
            throw new Unimplemented(join);

        DBSPTypeTuple resultType = this.convertType(join.getRowType()).to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new TranslationException("Unexpected join with " + join.getInputs().size() + " inputs", join);
        DBSPOperator left = this.getInputAs(join.getInput(0), true);
        DBSPOperator right = this.getInputAs(join.getInput(1), true);
        DBSPTypeTuple leftElementType = left.getNonVoidType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
                leftElementType.to(DBSPTypeTuple.class).size(), this.compiler.getTypeCompiler());
        JoinConditionAnalyzer.ConditionDecomposition decomposition = analyzer.analyze(join.getCondition());
        // If any key field is nullable we need to filter the inputs; this will make key columns non-nullable
        DBSPOperator filteredLeft = this.filterNonNullKeys(join, Linq.map(decomposition.comparisons, c -> c.leftColumn), left);
        DBSPOperator filteredRight = this.filterNonNullKeys(join, Linq.map(decomposition.comparisons, c -> c.rightColumn), right);

        leftElementType = filteredLeft.getNonVoidType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        DBSPTypeTuple rightElementType = filteredRight.getNonVoidType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        int leftColumns = leftElementType.size();
        int rightColumns = rightElementType.size();
        int totalColumns = leftColumns + rightColumns;
        DBSPTypeTuple leftResultType = resultType.slice(0, leftColumns);
        DBSPTypeTuple rightResultType = resultType.slice(leftColumns, leftColumns + rightColumns);

        DBSPVariablePath l = leftElementType.ref().var("l");
        DBSPVariablePath r = rightElementType.ref().var("r");
        DBSPTupleExpression lr = DBSPTupleExpression.flatten(l, r);
        List<DBSPExpression> leftKeyFields = Linq.map(
                decomposition.comparisons,
                c -> l.field(c.leftColumn).applyCloneIfNeeded().cast(c.resultType));
        List<DBSPExpression> rightKeyFields = Linq.map(
                decomposition.comparisons,
                c -> r.field(c.rightColumn).applyCloneIfNeeded().cast(c.resultType));
        DBSPExpression leftKey = new DBSPRawTupleExpression(leftKeyFields);
        DBSPExpression rightKey = new DBSPRawTupleExpression(rightKeyFields);

        @Nullable
        RexNode leftOver = decomposition.getLeftOver();
        DBSPExpression condition = null;
        DBSPExpression originalCondition = null;
        if (leftOver != null) {
            DBSPVariablePath t = resultType.ref().var("t");
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.compiler);
            condition = expressionCompiler.compile(leftOver);
            if (condition.getNonVoidType().mayBeNull)
                condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
            originalCondition = condition;
            condition = new DBSPClosureExpression(join.getCondition(), condition, t.asParameter());
            condition = this.declare("cond", condition);
        }
        DBSPVariablePath k = leftKey.getNonVoidType().var("k");

        DBSPClosureExpression toLeftKey = new DBSPRawTupleExpression(leftKey, DBSPTupleExpression.flatten(l))
                .closure(l.asParameter());
        DBSPIndexOperator leftIndex = new DBSPIndexOperator(
                join, this.declare("index", toLeftKey),
                leftKey.getNonVoidType(), leftElementType, DBSPTypeWeight.INSTANCE, false, filteredLeft);
        this.circuit.addOperator(leftIndex);

        DBSPClosureExpression toRightKey = new DBSPRawTupleExpression(rightKey, DBSPTupleExpression.flatten(r))
                .closure(r.asParameter());
        DBSPIndexOperator rIndex = new DBSPIndexOperator(
                join, this.declare("index", toRightKey),
                rightKey.getNonVoidType(), rightElementType, DBSPTypeWeight.INSTANCE, false, filteredRight);
        this.circuit.addOperator(rIndex);

        // For outer joins additional columns may become nullable.
        DBSPTupleExpression allFields = lr.pointwiseCast(resultType);
        DBSPClosureExpression makeTuple = allFields.closure(k.asRefParameter(), l.asParameter(), r.asParameter());
        DBSPJoinOperator joinResult = new DBSPJoinOperator(join, resultType, DBSPTypeWeight.INSTANCE,
                this.declare("pair", makeTuple),
                left.isMultiset || right.isMultiset, leftIndex, rIndex);

        DBSPOperator inner = joinResult;
        if (originalCondition != null) {
            DBSPBoolLiteral blit = originalCondition.as(DBSPBoolLiteral.class);
            if (blit == null || blit.value == null || !blit.value) {
                // Technically if blit.value == null or !blit.value then
                // the filter is false, and the result is empty.  But hopefully
                // the calcite optimizer won't allow that.
                DBSPFilterOperator fop = new DBSPFilterOperator(join, condition, joinResult);
                this.circuit.addOperator(joinResult);
                inner = fop;
            }
            // if blit it true we don't need to filter.
        }

        // Handle outer joins
        DBSPOperator result = inner;
        DBSPVariablePath joinVar = resultType.var("j");
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            DBSPVariablePath lCasted = leftResultType.var("l");
            this.circuit.addOperator(result);
            // project the join on the left columns
            DBSPClosureExpression toLeftColumns =
                    DBSPTupleExpression.flatten(joinVar)
                            .slice(0, leftColumns)
                            .pointwiseCast(leftResultType).closure(joinVar.asRefParameter());
            DBSPOperator joinLeftColumns = new DBSPMapOperator(
                    join, this.declare("proj", toLeftColumns),
                    leftResultType, DBSPTypeWeight.INSTANCE, inner);
            this.circuit.addOperator(joinLeftColumns);
            DBSPOperator distJoin = new DBSPDistinctOperator(join, joinLeftColumns);
            this.circuit.addOperator(distJoin);

            // subtract from left relation
            DBSPOperator leftCast = left;
            if (!leftResultType.sameType(leftElementType)) {
                DBSPClosureExpression castLeft =
                    DBSPTupleExpression.flatten(l).pointwiseCast(leftResultType).closure(l.asParameter()
                );
                leftCast = new DBSPMapOperator(join, castLeft, leftResultType, DBSPTypeWeight.INSTANCE, left);
                this.circuit.addOperator(leftCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(join, leftCast, distJoin);
            this.circuit.addOperator(sub);
            DBSPDistinctOperator dist = new DBSPDistinctOperator(join, sub);
            this.circuit.addOperator(dist);

            // fill nulls in the right relation fields
            DBSPTupleExpression rEmpty = new DBSPTupleExpression(
                    Linq.map(rightElementType.tupFields,
                             et -> DBSPLiteral.none(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPClosureExpression leftRow = DBSPTupleExpression.flatten(lCasted, rEmpty).closure(
                    lCasted.asRefParameter());
            DBSPOperator expand = new DBSPMapOperator(join,
                    this.declare("expand", leftRow), resultType, DBSPTypeWeight.INSTANCE, dist);
            this.circuit.addOperator(expand);
            result = new DBSPSumOperator(join, result, expand);
        }
        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            DBSPVariablePath rCasted = rightResultType.var("r");
            this.circuit.addOperator(result);

            // project the join on the right columns
            DBSPClosureExpression toRightColumns =
                    DBSPTupleExpression.flatten(joinVar)
                            .slice(leftColumns, totalColumns)
                            .pointwiseCast(rightResultType).closure(
                    joinVar.asRefParameter());
            DBSPOperator joinRightColumns = new DBSPMapOperator(
                    join, this.declare("proj", toRightColumns),
                    rightResultType, DBSPTypeWeight.INSTANCE, inner);
            this.circuit.addOperator(joinRightColumns);
            DBSPOperator distJoin = new DBSPDistinctOperator(join, joinRightColumns);
            this.circuit.addOperator(distJoin);

            // subtract from right relation
            DBSPOperator rightCast = right;
            if (!rightResultType.sameType(rightElementType)) {
                DBSPClosureExpression castRight =
                        DBSPTupleExpression.flatten(r).pointwiseCast(rightResultType).closure(
                        r.asParameter());
                rightCast = new DBSPMapOperator(join, castRight, rightResultType, DBSPTypeWeight.INSTANCE, right);
                this.circuit.addOperator(rightCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(join, rightCast, distJoin);
            this.circuit.addOperator(sub);
            DBSPDistinctOperator dist = new DBSPDistinctOperator(join, sub);
            this.circuit.addOperator(dist);

            // fill nulls in the left relation fields
            DBSPTupleExpression lEmpty = new DBSPTupleExpression(
                    Linq.map(leftElementType.tupFields,
                            et -> DBSPLiteral.none(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPClosureExpression rightRow =
                    DBSPTupleExpression.flatten(lEmpty, rCasted).closure(
                    rCasted.asRefParameter());
            DBSPOperator expand = new DBSPMapOperator(join,
                    this.declare("expand", rightRow), resultType, DBSPTypeWeight.INSTANCE, dist);
            this.circuit.addOperator(expand);
            result = new DBSPSumOperator(join, result, expand);
        }

        this.assignOperator(join, Objects.requireNonNull(result));
    }

    @Nullable
    ModifyTableTranslation modifyTableTranslation;

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression.
     * This can be invoked by a DDM statement, or by a SQL query that computes a constant result.
     */
    public void visitLogicalValues(LogicalValues values) {
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, this.compiler);
        DBSPTypeTuple sourceType = this.convertType(values.getRowType()).to(DBSPTypeTuple.class);
        DBSPTypeTuple resultType;
        if (this.modifyTableTranslation != null) {
            resultType = this.modifyTableTranslation.getResultType();
            if (sourceType.size() != resultType.size())
                throw new TranslationException("Expected a tuple with " + resultType.size() +
                        " values but got " + values, values);
        } else {
            resultType = sourceType;
        }

        DBSPZSetLiteral result = new DBSPZSetLiteral(resultType, DBSPTypeWeight.INSTANCE);
        for (List<RexLiteral> t : values.getTuples()) {
            List<DBSPExpression> expressions = new ArrayList<>();
            if (t.size() != sourceType.size())
                throw new TranslationException("Expected a tuple with " + sourceType.size() +
                        " values but got " + t, values);
            int i = 0;
            for (RexLiteral rl : t) {
                DBSPType resultFieldType = resultType.tupFields[i];
                DBSPExpression expr = expressionCompiler.compile(rl);
                if (expr.is(DBSPLiteral.class)) {
                    // The expression compiler does not actually have type information
                    // so the nulls produced will have the wrong type.
                    DBSPLiteral lit = expr.to(DBSPLiteral.class);
                    if (lit.isNull)
                        expr = DBSPLiteral.none(resultFieldType);
                }
                if (!expr.getNonVoidType().sameType(resultFieldType)) {
                    DBSPExpression cast = expr.cast(resultFieldType);
                    expressions.add(cast);
                } else {
                    expressions.add(expr);
                }
                i++;
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(t, expressions);
            result.add(expression);
        }

        if (this.modifyTableTranslation != null) {
            this.modifyTableTranslation.setResult(result);
        } else {
            DBSPOperator constant = new DBSPConstantOperator(values, result, false);
            this.assignOperator(values, constant);
        }
    }

    public void visitIntersect(LogicalIntersect intersect) {
        // Intersect is a special case of join.
        List<RelNode> inputs = intersect.getInputs();
        RelNode input = intersect.getInput(0);
        DBSPOperator previous = this.getInputAs(input, false);

        if (inputs.size() == 0)
            throw new UnsupportedException(intersect);
        if (inputs.size() == 1) {
            Utilities.putNew(this.nodeOperator, intersect, previous);
            return;
        }

        DBSPType inputRowType = this.convertType(input.getRowType());
        DBSPTypeTuple resultType = this.convertType(intersect.getRowType()).to(DBSPTypeTuple.class);
        DBSPVariablePath t = inputRowType.ref().var("t");
        DBSPExpression entireKey =
                new DBSPRawTupleExpression(
                        t.applyClone(),
                        new DBSPRawTupleExpression()).closure(
                t.asParameter());
        DBSPVariablePath l = DBSPTypeRawTuple.EMPTY_TUPLE_TYPE.ref().var("l");
        DBSPVariablePath r = DBSPTypeRawTuple.EMPTY_TUPLE_TYPE.ref().var("r");
        DBSPVariablePath k = inputRowType.ref().var("k");

        DBSPClosureExpression closure = k.applyClone().closure(
                k.asParameter(), l.asParameter(), r.asParameter());
        for (int i = 1; i < inputs.size(); i++) {
            DBSPOperator previousIndex = new DBSPIndexOperator(
                    intersect,
                    this.declare("index", entireKey),
                    inputRowType, new DBSPTypeRawTuple(), DBSPTypeWeight.INSTANCE,
                    previous.isMultiset, previous);
            this.circuit.addOperator(previousIndex);
            DBSPOperator inputI = this.getInputAs(intersect.getInput(i), false);
            DBSPOperator index = new DBSPIndexOperator(
                    intersect,
                    this.declare("index", entireKey),
                    inputRowType, new DBSPTypeRawTuple(), DBSPTypeWeight.INSTANCE,
                    inputI.isMultiset, inputI);
            this.circuit.addOperator(index);
            previous = new DBSPJoinOperator(intersect, resultType, DBSPTypeWeight.INSTANCE,
                    closure, false, previousIndex, index);
            this.circuit.addOperator(previous);
        }
        Utilities.putNew(this.nodeOperator, intersect, previous);
    }

    DBSPExpression compileWindowBound(RexWindowBound bound, DBSPType boundType, ExpressionCompiler eComp) {
        IsNumericType numType = boundType.to(IsNumericType.class);
        DBSPExpression numericBound;
        if (bound.isUnbounded())
            numericBound = numType.getMaxValue();
        else if (bound.isCurrentRow())
            numericBound = numType.getZero();
        else {
            DBSPExpression value = eComp.compile(Objects.requireNonNull(bound.getOffset()));
            numericBound = value.cast(boundType);
        }
        String beforeAfter = bound.isPreceding() ? "Before" : "After";
        return new DBSPStructExpression(DBSPTypeAny.INSTANCE.path(
                new DBSPPath("RelOffset", beforeAfter)),
                DBSPTypeAny.INSTANCE, numericBound);
    }

    public void visitWindow(LogicalWindow window) {
        DBSPTypeTuple windowResultType = this.convertType(window.getRowType()).to(DBSPTypeTuple.class);
        RelNode inputNode = window.getInput();
        DBSPOperator input = this.getInputAs(window.getInput(0), true);
        DBSPTypeTuple inputRowType = this.convertType(inputNode.getRowType()).to(DBSPTypeTuple.class);
        DBSPVariablePath inputRowRefVar = inputRowType.ref().var("t");
        ExpressionCompiler eComp = new ExpressionCompiler(inputRowRefVar, window.constants, this.compiler);
        int windowFieldIndex = inputRowType.size();
        DBSPVariablePath previousRowRefVar = inputRowRefVar;

        DBSPTypeTuple currentTupleType = inputRowType;
        DBSPOperator lastOperator = input;
        for (Window.Group group: window.groups) //noinspection GrazieInspection
        {
            if (lastOperator != input)
                this.circuit.addOperator(lastOperator);
            List<RelFieldCollation> orderKeys = group.orderKeys.getFieldCollations();
            // Sanity checks
            if (orderKeys.size() > 1)
                throw new Unimplemented("ORDER BY not yet supported with multiple columns", window);
            RelFieldCollation collation = orderKeys.get(0);
            if (collation.getDirection() != RelFieldCollation.Direction.ASCENDING)
                throw new Unimplemented("OVER only supports ascending sorting", window);
            int orderColumnIndex = collation.getFieldIndex();
            DBSPExpression orderField = inputRowRefVar.field(orderColumnIndex);
            DBSPType sortType = inputRowType.tupFields[orderColumnIndex];
            if (!sortType.is(DBSPTypeInteger.class) &&
                    !sortType.is(DBSPTypeTimestamp.class))
                throw new Unimplemented("OVER currently requires an integer type for ordering ", window);
            if (sortType.mayBeNull)
                throw new Unimplemented("OVER currently does not support sorting on nullable column ", window);

            // Create window description
            DBSPExpression lb = this.compileWindowBound(group.lowerBound, sortType, eComp);
            DBSPExpression ub = this.compileWindowBound(group.upperBound, sortType, eComp);
            DBSPExpression windowExpr = new DBSPStructExpression(
                    DBSPTypeAny.INSTANCE.path(
                            new DBSPPath("RelRange", "new")),
                    DBSPTypeAny.INSTANCE, lb, ub);
            DBSPExpression windowExprVar = this.declare("window", windowExpr);

            // Map each row to an expression of the form: |t| (partition, (order, t.clone()))
            List<Integer> partitionKeys = group.keys.toList();
            List<DBSPExpression> expressions = Linq.map(partitionKeys, inputRowRefVar::field);
            DBSPTupleExpression partition = new DBSPTupleExpression(window, expressions);
            DBSPExpression orderAndRow = new DBSPRawTupleExpression(orderField, inputRowRefVar.applyClone());
            DBSPExpression mapExpr = new DBSPRawTupleExpression(partition, orderAndRow);
            DBSPClosureExpression mapClo = mapExpr.closure(inputRowRefVar.asParameter());
            DBSPExpression mapCloVar = this.declare("map", mapClo);
            DBSPOperator mapIndex = new DBSPMapIndexOperator(window, mapCloVar,
                    partition.getNonVoidType(), orderAndRow.getNonVoidType(), DBSPTypeWeight.INSTANCE, input);
            this.circuit.addOperator(mapIndex);

            List<AggregateCall> aggregateCalls = group.getAggregateCalls(window);
            List<DBSPType> types = Linq.map(aggregateCalls, c -> this.convertType(c.type));
            DBSPTypeTuple tuple = new DBSPTypeTuple(types);
            DBSPAggregate fd = this.createAggregate(window, aggregateCalls, tuple, inputRowType, 0);

            // Compute aggregates for the window
            DBSPTypeTuple aggResultType = fd.defaultZeroType().to(DBSPTypeTuple.class);
            // This operator is always incremental, so create the non-incremental version
            // of it by adding a D and an I around it.
            DBSPDifferentialOperator diff = new DBSPDifferentialOperator(window, mapIndex);
            this.circuit.addOperator(diff);
            DBSPWindowAggregateOperator windowAgg = new DBSPWindowAggregateOperator(
                    group, null, fd,
                    windowExprVar, partition.getNonVoidType(), sortType,
                    aggResultType, DBSPTypeWeight.INSTANCE, diff);
            this.circuit.addOperator(windowAgg);
            DBSPIntegralOperator integral = new DBSPIntegralOperator(window, windowAgg);
            this.circuit.addOperator(integral);

            // Join the previous result with the aggregate
            // First index the aggregate.
            DBSPExpression partAndOrder = new DBSPRawTupleExpression(partition, orderField);
            DBSPExpression indexedInput = new DBSPRawTupleExpression(partAndOrder, previousRowRefVar.applyClone());
            DBSPExpression partAndOrderClo = indexedInput.closure(previousRowRefVar.asParameter());
            DBSPOperator indexInput = new DBSPIndexOperator(window,
                    this.declare("index", partAndOrderClo),
                    partAndOrder.getNonVoidType(), previousRowRefVar.getNonVoidType().deref(),
                    DBSPTypeWeight.INSTANCE, lastOperator.isMultiset, lastOperator);
            this.circuit.addOperator(indexInput);

            DBSPVariablePath key = partAndOrder.getNonVoidType().var("k");
            DBSPVariablePath left = currentTupleType.var("l");
            DBSPVariablePath right = aggResultType.ref().var("r");
            DBSPExpression[] allFields = new DBSPExpression[
                    currentTupleType.size() + aggResultType.size()];
            for (int i = 0; i < currentTupleType.size(); i++)
                allFields[i] = left.field(i).applyCloneIfNeeded();
            for (int i = 0; i < aggResultType.size(); i++) {
                // Calcite is very smart and sometimes infers non-nullable result types
                // for these aggregates.  So we have to cast the results to whatever
                // Calcite says they will be.
                allFields[i + currentTupleType.size()] = right.field(i).applyCloneIfNeeded().cast(
                        windowResultType.getFieldType(windowFieldIndex));
                windowFieldIndex++;
            }
            DBSPTupleExpression addExtraFieldBody = new DBSPTupleExpression(allFields);
            DBSPClosureExpression addExtraField =
                    addExtraFieldBody.closure(key.asRefParameter(), left.asRefParameter(), right.asParameter());
            lastOperator = new DBSPJoinOperator(window, addExtraFieldBody.getNonVoidType(),
                    DBSPTypeWeight.INSTANCE, this.declare("join", addExtraField),
                    indexInput.isMultiset || windowAgg.isMultiset, indexInput, integral);
            currentTupleType = addExtraFieldBody.getNonVoidType().to(DBSPTypeTuple.class);
            previousRowRefVar = currentTupleType.ref().var("t");
        }
        this.assignOperator(window, lastOperator);
    }

    public void visitSort(LogicalSort sort) {
        // Aggregate in a single group.
        // TODO: make this more efficient?
        RelNode input = sort.getInput();
        DBSPType inputRowType = this.convertType(input.getRowType());
        DBSPOperator opInput = this.getOperator(input);

        DBSPVariablePath t = inputRowType.var("t");
        DBSPExpression emptyGroupKeys =
                new DBSPRawTupleExpression(
                        new DBSPRawTupleExpression(),
                        DBSPTupleExpression.flatten(t)).closure(t.asRefParameter());
        DBSPIndexOperator index = new DBSPIndexOperator(
                sort, this.declare("index", emptyGroupKeys),
                new DBSPTypeRawTuple(), inputRowType, DBSPTypeWeight.INSTANCE,
                opInput.isMultiset, opInput);
        this.circuit.addOperator(index);
        // apply an aggregation function that just creates a vector.
        DBSPTypeVec vecType = new DBSPTypeVec(inputRowType);
        DBSPExpression zero = DBSPTypeAny.INSTANCE.path(
                new DBSPPath(vecType.name, "new")).call();
        DBSPVariablePath accum = vecType.var("a");
        DBSPVariablePath row = inputRowType.var("v");
        // An element with weight 'w' is pushed 'w' times into the vector
        DBSPExpression wPush = new DBSPApplyExpression(
                "weighted_push", null, accum, row, this.compiler.weightVar);
        DBSPExpression push = wPush.closure(
                accum.asRefParameter(true), row.asRefParameter(),
                this.compiler.weightVar.asParameter());
        DBSPExpression constructor = DBSPTypeAny.INSTANCE.path(
            new DBSPPath(
                    new DBSPSimplePathSegment("Fold",
                            DBSPTypeAny.INSTANCE,
                        new DBSPTypeUser(null, "UnimplementedSemigroup",
                                false, DBSPTypeAny.INSTANCE),
                        DBSPTypeAny.INSTANCE,
                        DBSPTypeAny.INSTANCE),
                    new DBSPSimplePathSegment("new")));

        DBSPExpression folder = constructor.call(zero, push);
        DBSPAggregateOperator agg = new DBSPAggregateOperator(sort,
                new DBSPTypeRawTuple(), new DBSPTypeVec(inputRowType), DBSPTypeWeight.INSTANCE,
                this.declare("toVec", folder), null,
                index);
        this.circuit.addOperator(agg);

        // Generate comparison function for sorting the vector
        DBSPComparatorExpression comparator = new DBSPNoComparatorExpression(sort, inputRowType);
        for (RelFieldCollation collation: sort.getCollation().getFieldCollations()) {
            int field = collation.getFieldIndex();
            RelFieldCollation.Direction direction = collation.getDirection();
            boolean ascending;
            switch (direction) {
                case ASCENDING:
                    ascending = true;
                    break;
                case DESCENDING:
                    ascending = false;
                    break;
                default:
                case STRICTLY_ASCENDING:
                case STRICTLY_DESCENDING:
                case CLUSTERED:
                    throw new Unimplemented(sort);
            }
            comparator = new DBSPFieldComparatorExpression(sort, comparator, field, ascending);
        }
        DBSPSortExpression sorter = new DBSPSortExpression(sort, inputRowType, comparator);
        DBSPOperator sortElement = new DBSPMapOperator(sort,
                this.declare("sort", sorter), vecType, DBSPTypeWeight.INSTANCE, agg);
        this.assignOperator(sort, sortElement);
    }

    @Override
    public void visit(
            RelNode node, int ordinal,
            @Nullable RelNode parent) {
        Logger.INSTANCE.from(this, 3)
                .append("Visiting ")
                .append(node.toString())
                .newline();
        if (this.nodeOperator.containsKey(node))
            // We have already done this one.  This can happen because the
            // plan can be a DAG, not just a tree.
            return;

        // logical correlates are not done in postorder.
        if (this.visitIfMatches(node, LogicalCorrelate.class, this::visitCorrelate))
            return;

        // First process children
        super.visit(node, ordinal, parent);
        // Synthesize current node
        boolean success =
                this.visitIfMatches(node, LogicalTableScan.class, this::visitScan) ||
                this.visitIfMatches(node, LogicalProject.class, this::visitProject) ||
                this.visitIfMatches(node, LogicalUnion.class, this::visitUnion) ||
                this.visitIfMatches(node, LogicalMinus.class, this::visitMinus) ||
                this.visitIfMatches(node, LogicalFilter.class, this::visitFilter) ||
                this.visitIfMatches(node, LogicalValues.class, this::visitLogicalValues) ||
                this.visitIfMatches(node, LogicalAggregate.class, this::visitAggregate) ||
                this.visitIfMatches(node, LogicalJoin.class, this::visitJoin) ||
                this.visitIfMatches(node, LogicalIntersect.class, this::visitIntersect) ||
                this.visitIfMatches(node, LogicalWindow.class, this::visitWindow) ||
                this.visitIfMatches(node, LogicalSort.class, this::visitSort) ||
                this.visitIfMatches(node, Uncollect.class, this::visitUncollect);
        if (!success)
            throw new Unimplemented(node);
    }

    @SuppressWarnings("UnusedReturnValue")
    @Nullable
    public DBSPNode compile(FrontEndStatement statement) {
        if (statement.is(CreateViewStatement.class)) {
            CreateViewStatement view = statement.to(CreateViewStatement.class);
            RelNode rel = view.getRelNode();
            Logger.INSTANCE.from(this, 2)
                    .append(CalciteCompiler.getPlan(rel))
                    .newline();
            this.go(rel);
            // TODO: connect the result of the query compilation with
            // the fields of rel; for now we assume that these are 1/1
            DBSPOperator op = this.getOperator(rel);
            DBSPOperator o;
            if (this.generateOutputForNextView) {
                o = new DBSPSinkOperator(
                        view, view.tableName, view.statement, statement.comment, op);
            } else {
                // We may already have a node for this output
                DBSPOperator previous = this.circuit.getOperator(view.tableName);
                if (previous != null)
                    return previous;
                o = new DBSPNoopOperator(view, op, statement.comment, view.tableName);
            }
            this.circuit.addOperator(o);
            return o;
        } else if (statement.is(CreateTableStatement.class) ||
                statement.is(DropTableStatement.class)) {
            this.tableContents.execute(statement);
            CreateTableStatement create = statement.as(CreateTableStatement.class);
            if (create != null && this.options.optimizerOptions.generateInputForEveryTable) {
                // We create an input for the circuit.  The inputs
                // could be created by visiting LogicalTableScan, but if a table
                // is *not* used in a view, it won't have a corresponding input
                // in the circuit.
                String tableName = create.tableName;
                CreateTableStatement def = this.tableContents.getTableDefinition(tableName);
                DBSPType rowType = def.getRowType(this.compiler.getTypeCompiler());
                DBSPSourceOperator result = new DBSPSourceOperator(
                        create, this.makeZSet(rowType), def.statement, tableName);
                this.circuit.addOperator(result);
            }
            return null;
        } else if (statement.is(TableModifyStatement.class)) {
            TableModifyStatement modify = statement.to(TableModifyStatement.class);
            // The type of the data must be extracted from the modified table
            if (!(modify.node instanceof SqlInsert))
                throw new Unimplemented(statement);
            SqlInsert insert = (SqlInsert) statement.node;
            assert insert != null;
            CreateTableStatement def = this.tableContents.getTableDefinition(modify.tableName);
            this.modifyTableTranslation = new ModifyTableTranslation(
                    modify, def, insert.getTargetColumnList(), this.compiler);
            if (modify.rel instanceof LogicalTableScan) {
                // Support for INSERT INTO table (SELECT * FROM otherTable)
                LogicalTableScan scan = (LogicalTableScan) modify.rel;
                List<String> name = scan.getTable().getQualifiedName();
                String sourceTable = name.get(name.size() - 1);
                DBSPZSetLiteral.Contents data = this.tableContents.getTableContents(sourceTable);
                this.tableContents.addToTable(modify.tableName, data);
                this.modifyTableTranslation = null;
                return new DBSPZSetLiteral(DBSPTypeWeight.INSTANCE, data);
            } else if (modify.rel instanceof LogicalValues) {
                this.go(modify.rel);
                DBSPZSetLiteral result = this.modifyTableTranslation.getTranslation();
                this.tableContents.addToTable(modify.tableName, result.getContents());
                this.modifyTableTranslation = null;
                return result;
            }
        }
        assert statement.node != null;
        throw new Unimplemented(statement.node);
    }

    public TableContents getTableContents() {
        return this.tableContents;
    }
}
