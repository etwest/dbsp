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

package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPI32Literal extends DBSPLiteral {
    @Nullable
    public final Integer value;

    public DBSPI32Literal() {
        this(null, true);
    }

    public DBSPI32Literal(int value) {
        this(value, false);
    }

    public DBSPI32Literal(@Nullable Object node, DBSPType type , @Nullable Integer value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPI32Literal(@Nullable Integer value, boolean nullable) {
        this(null, DBSPTypeInteger.SIGNED_32.setMayBeNull(nullable), value);
        if (value == null && !nullable)
            throw new RuntimeException("Null value with non-nullable type");
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }

    public DBSPTypeInteger getIntegerType() {
        assert this.type != null;
        return this.type.to(DBSPTypeInteger.class);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPI32Literal that = (DBSPI32Literal) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return new DBSPI32Literal(Objects.requireNonNull(this.value));
    }
}
