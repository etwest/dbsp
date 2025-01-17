package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.visitors.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;

/**
 * Replaces MUL_WEIGHT with a cast followed by a multiplication.
 */
public class EliminateMulWeight extends InnerRewriteVisitor {
    public EliminateMulWeight(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        if (expression.operation.equals(DBSPOpcode.MUL_WEIGHT)) {
            DBSPExpression left = this.transform(expression.left);
            DBSPExpression right = this.transform(expression.right);
            DBSPExpression result = new DBSPBinaryExpression(
                    expression.getNode(), expression.getNonVoidType(), DBSPOpcode.MUL,
                    left, right.cast(left.getNonVoidType()));
            this.map(expression, result);
            return false;
        }
        return super.preorder(expression);
    }
}
