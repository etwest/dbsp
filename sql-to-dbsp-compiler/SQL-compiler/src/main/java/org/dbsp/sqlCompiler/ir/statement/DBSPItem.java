package org.dbsp.sqlCompiler.ir.statement;

/**
 * A base class for Rust <a href="https://doc.rust-lang.org/reference/items.html">items</a>.
 */
public abstract class DBSPItem extends DBSPStatement {
    protected DBSPItem() {
        super(null);
    }
}
