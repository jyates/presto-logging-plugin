package com.shopify.presto.eventlisteners;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.google.common.collect.Sets;

import java.util.Set;

public class QueryDetails {
    private String queryText;

    // The table or view to be created/dropped/modified/inserted into if there is one
    private String targetTable;

    // Tables or views in the from part of the query
    private Set<String> fromTables;

    // Tables defined in CTEs
    private Set<String> cteTables;

    // Table aliases
    private Set<String> aliasTables;

    private String operation;

    private QueryDetails(String queryText, String targetTable, Set<String> fromTables, Set<String> cteTables, Set<String> aliasTables,  String operation) {
        this.queryText = queryText;
        this.targetTable = targetTable;
        this.fromTables = fromTables;
        this.cteTables = cteTables;
        this.aliasTables = aliasTables;
        this.operation = operation;
    }

    public String getQueryText() {
        return queryText;
    }

    public String getOperation() {
        return operation;
    }

    public Set<String> getFromTables() {
        return fromTables;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public Set<String> getCTETables() {
        return cteTables;
    }

    public Set<String> getAliasTables() {
        return aliasTables;
    }

    public Set<String> getFromTablesWithoutCTEs() {
        return Sets.difference(fromTables, cteTables);
    }

    public static QueryDetails parseQueryDetails(String queryText) {
        SqlParser parser = new SqlParser();
        Statement stmt = parser.createStatement(queryText, new ParsingOptions());
        String operation = stmt.getClass().getSimpleName();
        AstTableVisitor v = new AstTableVisitor();
        AstTableVisitor.TableVisitorContext ctx = new AstTableVisitor.TableVisitorContext();
        stmt.accept(v, ctx);
        return new QueryDetails(queryText, ctx.targetTable, ctx.fromTables, ctx.cteTables, ctx.aliasTables, operation);
    }
}
