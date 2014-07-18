package halo.client;

import halo.core.HaloTable;
import halo.core.RowSet;

import java.io.IOException;

/**
 * WhereClause is a tree structure representing the where clause part in
 * a query sentence.
 */
public class WhereClause {
    ColumnFilterTree filterTree;

    /**
     * Constructs a trivial where-clause.
     */
    public WhereClause() {
        filterTree = new ColumnFilterTree();
    }

    /**
     * Constructs a where-clause from a filter tree.
     */
    public WhereClause(ColumnFilterTree filterTree) {
        this.filterTree = filterTree;
    }

    public ColumnFilterTree getFilterTree() {
        return filterTree;
    }

    /* Parses a where-clause statement. */
    public static WhereClause parse(String statement) throws BadSqlStatement {
        return new WhereClauseParser(statement, 0, statement.length()).getWhereClause();
    }

    /* Parses a where-clause statement. */
    public static WhereClause parse(String statement, int start) throws BadSqlStatement {
        return new WhereClauseParser(statement, start, statement.length()).getWhereClause();
    }

    /* Parses a where-clause statement. */
    public static WhereClause parse(String statement, int start, int stop) throws BadSqlStatement {
        if (stop > statement.length()){
            stop = statement.length();
        }
        return new WhereClauseParser(statement, start, stop).getWhereClause();
    }

    public RowSet applyToTable(HaloTable table) throws IOException {
        return filterTree.applyToTable(table);
    }
}
