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

package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;

/**
 * A partial circuit is a circuit under construction.
 * A complete circuit can be obtained by calling the "seal" method.
 */
public class DBSPPartialCircuit extends DBSPNode implements IDBSPOuterNode, IModule {
    public final List<DBSPSourceOperator> inputOperators = new ArrayList<>();
    public final List<DBSPSinkOperator> outputOperators = new ArrayList<>();
    public final List<DBSPOperator> allOperators = new ArrayList<>();
    public final Map<String, DBSPOperator> operatorDeclarations = new HashMap<>();
    public final IErrorReporter errorReporter;

    public DBSPPartialCircuit(IErrorReporter errorReporter) {
        super(null);
        this.errorReporter = errorReporter;
    }

    /**
     * @return the names of the input tables.
     * The order of the tables corresponds to the inputs of the generated circuit.
     */
    public List<String> getInputTables() {
        return Linq.map(this.inputOperators, DBSPOperator::getName);
    }

    public int getOutputCount() {
        return this.outputOperators.size();
    }

    public DBSPType getOutputType(int outputNo) {
        return this.outputOperators.get(outputNo).getNonVoidType();
    }

    public DBSPTypeRawTuple getOutputType() {
        return new DBSPTypeRawTuple(null, Linq.map(this.outputOperators, IHasType::getNonVoidType));
    }

    public void addOperator(DBSPOperator operator) {
        Logger.INSTANCE.from(this, 1)
                .append("Adding ")
                .append(operator.toString())
                .newline();
        if (this.operatorDeclarations.containsKey(operator.outputName)) {
            this.errorReporter.reportError(operator.getSourcePosition(), false, "Duplicate definition",
                    "View " + operator.outputName + " already defined");
            DBSPOperator previous = this.operatorDeclarations.get(operator.outputName);
            this.errorReporter.reportError(previous.getSourcePosition(), false, "Duplicate definition",
                    "This is the previous definition");
            return;
        }
        Utilities.putNew(this.operatorDeclarations, operator.outputName, operator);
        if (operator.is(DBSPSourceOperator.class))
            this.inputOperators.add(operator.to(DBSPSourceOperator.class));
        else if (operator.is(DBSPSinkOperator.class))
            this.outputOperators.add(operator.to(DBSPSinkOperator.class));
        this.allOperators.add(operator);
    }

    public Iterable<DBSPOperator> getAllOperators() { return this.allOperators; }

    @Nullable
    public DBSPOperator getOperator(String tableName) {
        return this.operatorDeclarations.get(tableName);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (IDBSPNode op: this.allOperators) {
            IDBSPOuterNode outer = op.as(IDBSPOuterNode.class);
            if (outer != null)
                outer.accept(visitor);
        }
        visitor.postorder(this);
    }

    public boolean sameCircuit(DBSPPartialCircuit other) {
        if (this == other)
            return true;
        return Linq.same(this.allOperators, other.allOperators);
    }

    /**
     * No more changes are expected to the circuit.
     */
    public DBSPCircuit seal(String name) {
        return new DBSPCircuit(this, name);
    }

    public int getInputCount() {
        return this.inputOperators.size();
    }

    @Override
    public String toString() {
        return "PartialCircuit" + this.id;
    }
}
