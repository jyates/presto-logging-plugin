package com.shopify.presto.eventlistener;

import com.shopify.presto.eventlisteners.QueryDetails;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQueryDetails {

    @Test
    public void testParseSelectSimple() {
        QueryDetails d = QueryDetails.parseQueryDetails("SELECT * FROM t1");
        assertEquals(newHashSet("t1"), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("Query", d.getOperation());
    }

    @Test
    public void testParseSelectSubquery() {
        QueryDetails d = QueryDetails.parseQueryDetails("SELECT * FROM t1, (SELECT a, b FROM t2)");
        assertEquals(newHashSet("t1", "t2"), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("Query", d.getOperation());
    }

    @Test
    public void testParseSelectSubqueryWithAlias() {
        QueryDetails d = QueryDetails.parseQueryDetails("SELECT * FROM t1, (SELECT a, b FROM t2) x");
        assertEquals(newHashSet("t1", "t2"), d.getFromTables());
        assertEquals(newHashSet("x"), d.getAliasTables());
        assertEquals("", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("Query", d.getOperation());
    }

    @Test
    public void testParseJoin() {
        QueryDetails d = QueryDetails.parseQueryDetails("SELECT * FROM t1 as y JOIN (SELECT a, b FROM t2) x ON (x.a = y.a)");
        assertEquals(newHashSet("t1", "t2"), d.getFromTables());
        assertEquals(newHashSet("x", "y"), d.getAliasTables());
        assertEquals("", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("Query", d.getOperation());
    }

    @Test
    public void testParseSelectCTE() {
        QueryDetails d = QueryDetails.parseQueryDetails("WITH x as (SELECT a, b FROM t2) SELECT * FROM t1 JOIN x ON (e.a = t1.a)");
        assertEquals(newHashSet("t1", "t2", "x"), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("", d.getTargetTable());
        assertEquals(newHashSet("x"), d.getCTETables());
        assertEquals("Query", d.getOperation());
        assertEquals(newHashSet("t1", "t2"), d.getFromTablesWithoutCTEs());
    }

    @Test
    public void testParseInsertSimple() {
        QueryDetails d = QueryDetails.parseQueryDetails("INSERT INTO t2 SELECT * FROM t1");
        assertEquals(newHashSet("t1"), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("t2", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("Insert", d.getOperation());
    }

    @Test
    public void testParseCreateTable() {
        QueryDetails d = QueryDetails.parseQueryDetails("CREATE TABLE x (c1 VARCHAR)");
        assertEquals(Collections.emptySet(), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("x", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("CreateTable", d.getOperation());
    }

    @Test
    public void testParseCreateTableAsSelect() {
        QueryDetails d = QueryDetails.parseQueryDetails("CREATE TABLE x AS SELECT * FROM y");
        assertEquals(newHashSet("y"), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("x", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("CreateTableAsSelect", d.getOperation());
    }

    @Test
    public void testParseCreateView() {
        QueryDetails d = QueryDetails.parseQueryDetails("CREATE VIEW x AS SELECT * FROM y");
        assertEquals(newHashSet("y"), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("x", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("CreateView", d.getOperation());
    }

    @Test
    public void testParseDropTable() {
        QueryDetails d = QueryDetails.parseQueryDetails("DROP TABLE x");
        assertEquals(Collections.emptySet(), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("x", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("DropTable", d.getOperation());
    }

    @Test
    public void testParseDropView() {
        QueryDetails d = QueryDetails.parseQueryDetails("DROP VIEW x");
        assertEquals(Collections.emptySet(), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("x", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("DropView", d.getOperation());
    }

    @Test
    public void testDecimalLiteral() {
        QueryDetails d = QueryDetails.parseQueryDetails("SELECT 100.0");
        assertEquals(Collections.emptySet(), d.getFromTables());
        assertEquals(Collections.emptySet(), d.getAliasTables());
        assertEquals("", d.getTargetTable());
        assertEquals(Collections.emptySet(), d.getCTETables());
        assertEquals("Query", d.getOperation());
    }
}
