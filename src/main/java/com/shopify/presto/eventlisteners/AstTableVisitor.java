package com.shopify.presto.eventlisteners;

import com.facebook.presto.sql.tree.*;

import java.util.HashSet;
import java.util.Set;

class AstTableVisitor extends DefaultTraversalVisitor<Object, AstTableVisitor.TableVisitorContext> {

    /**
     * Visit every table reference and add them to a set. Find any CTE definitions so we can remove them
     * from the list of actual tables that are referenced. It's not possible to distinguish views from
     * tables just by parsing the query, we just treat everything as a "table".
     */

    @Override
    protected Object visitTable(Table node, TableVisitorContext context) {
        context.addFromTable(node.getName().getSuffix());
        return super.visitTable(node, context);
    }

    @Override
    protected Object visitAliasedRelation(AliasedRelation node, TableVisitorContext context)
    {
        context.addAliasTable(node.getAlias().getValue());
        return process(node.getRelation(), context);
    }

    @Override
    protected Object visitInsert(Insert node, TableVisitorContext context)
    {
        context.setTargetTable(node.getTarget().getSuffix());
        super.visitInsert(node, context);
        return null;
    }

    @Override
    protected Object visitQuerySpecification(QuerySpecification node, TableVisitorContext context) {
        super.visitQuerySpecification(node, context);
        return null;
    }

    @Override
    protected Object visitWithQuery(WithQuery node, TableVisitorContext context)
    {
        context.addCTETable(node.getName().getValue());
        super.visitWithQuery(node, context);
        return null;
    }

    @Override
    protected Object visitCreateTable(CreateTable node, TableVisitorContext context)
    {
        context.setTargetTable(node.getName().getSuffix());
        super.visitCreateTable(node, context);
        return null;
    }

    @Override
    protected Object visitCreateView(CreateView node, TableVisitorContext context)
    {
        context.setTargetTable(node.getName().getSuffix());
        super.visitCreateView(node, context);
        return null;
    }

    @Override
    protected Object visitDropTable(DropTable node, TableVisitorContext context)
    {
        context.setTargetTable(node.getTableName().getSuffix());
        super.visitDropTable(node, context);
        return null;
    }

    @Override
    protected Object visitDropView(DropView node, TableVisitorContext context)
    {
        context.setTargetTable(node.getName().getSuffix());
        super.visitDropView(node, context);
        return null;
    }

    @Override
    protected Object visitCreateTableAsSelect(CreateTableAsSelect node, TableVisitorContext context)
    {
        context.setTargetTable(node.getName().getSuffix());
        super.visitCreateTableAsSelect(node, context);
        return null;
    }

    static class TableVisitorContext {
        Set<String> fromTables;
        Set<String> cteTables;
        Set<String> aliasTables;
        String targetTable;

        TableVisitorContext() {
            fromTables = new HashSet<>();
            aliasTables = new HashSet<>();
            cteTables = new HashSet<>();
            targetTable = "";
        }

        public void addFromTable(String table) {
            fromTables.add(table);
        }

        public void addAliasTable(String table) {
            aliasTables.add(table);
        }

        public void setTargetTable(String table) {
            targetTable = table;
        }

        public void addCTETable(String table) {
            cteTables.add(table);
        }
    }
}
