package com.distocraft.dc5000.dwhm.test;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.dwhm.TableAlterDetails;

import java.util.Iterator;
import java.util.Properties;

/**
 * This class is used by DWHManager to generate the batch statements needed to alter existing table.
 * Is fairly simple in that it doesnt do any checking in the alter statements being used i.e. it wont
 * check for duplicate columns or bad sql.
 */
public class TableAlterDetailsTest extends TestCase {
    private static final String tableName = "SOME_TABLE";
    private TableAlterDetails testInstance = null;
    public TableAlterDetailsTest(){
        super("TableAlterDetailsTest");
    }

    @Override @Before
    protected void setUp() throws Exception {
		// initialise StaticProperties
		final Properties prop = new Properties();
		prop.setProperty("property1", "value1");
		StaticProperties.giveProperties(prop);
        testInstance = new TableAlterDetails(tableName, 100);
    }

    @Test
    public void testGetTableName(){
        assertEquals("Table name not set correctly", tableName, testInstance.getTableName());
    }

    @Test
    public void testMaxBatchSize(){
        testInstance = new TableAlterDetails(tableName, 2);
        final int expectedSqlBatchCount = 3;
        final int counters = 5;
        for(int i=1;i<=counters;i++){
            testInstance.addColumn("counter-" + i + " numeric(18) null");
        }
        assertEquals("Number of counters altered isnt correct", counters, testInstance.getTotalAlterCount());
        final Iterator<String> it = testInstance.getIterator();
        int batchCount = 0;
        while(it.hasNext()){
            it.next();
            batchCount++;
        }
        assertEquals("Number of batchs generated wasnt correct ", expectedSqlBatchCount, batchCount);
    }


    @Test
    public void testAlterColumn(){
        final String column = "col2";
        final String datatype = "numeric";
        final String value = "(18)";
        final String nullable = "null";
        final String alterSql = " add tmp_" + column + " " + datatype + value + " " + nullable + "; "
              + "update " + tableName + " as a set tmp_" + column + " = cast(a." + column + " as " + datatype
              + "); " + "alter table " + tableName + " delete " + column + "; " + "alter table " + tableName
              + " RENAME  tmp_" + column + " TO " + column;
        testInstance.alterExistingColumn(alterSql);
        final String alterStatement = testInstance.getIterator().next();
        assertEquals("", "alter table " + tableName + " " + alterSql+";", alterStatement);
    }
    
    @Test
    public void testAlterOnlyIndex(){
        final String alterSql = "DROP INDEX DC_E_REDB_SUBSCRIBER_RAW_01_agent_circuit_id_LF;";
        testInstance.alterOnlyIndex(alterSql);
        final String alterStatement = testInstance.getIterator().next();
        assertEquals(alterSql, alterStatement);
    }
    
    @Test
    public void testAlterMultipleOnlyIndex(){
        final String alterSql1 = "DROP INDEX DC_E_REDB_SUBSCRIBER_RAW_01_agent_circuit_id_LF;";
        final String alterSql2 = "DROP INDEX DC_E_REDB_SUBSCRIBER_RAW_02_agent_circuit_id_LF;";
        final String alterSql3 = "DROP INDEX DC_E_REDB_SUBSCRIBER_RAW_03_agent_circuit_id_LF;";
        testInstance.alterOnlyIndex(alterSql1);
        testInstance.alterOnlyIndex(alterSql2);
        testInstance.alterOnlyIndex(alterSql3);
        final Iterator<String> it = testInstance.getIterator();
        int batchCount = 0;
        while(it.hasNext()){
            it.next();
            batchCount++;
        }
        assertEquals("Number of batchs generated wasnt correct ", 3, batchCount);
    }
    
    @Test
    public void testErrorIfIteratorStarted(){
        testInstance.deleteColumn("col");
        testInstance.getIterator();
        try{
            testInstance.deleteColumn("col");
            fail("Exception should have been thrown");
        } catch (RuntimeException e){
            // expected
        }
    }

    @Test
    public void testAddColumn(){
        final int counters = 3;
        final String expectedSQL = "alter table " + tableName+ " ADD counter-1 numeric(18) null," +
                "ADD counter-2 numeric(18) null,ADD counter-3 numeric(18) null;" ;
        for(int i=1;i<=counters;i++){
            testInstance.addColumn("counter-" + i + " numeric(18) null");
        }
        assertEquals("Number of counters altered isnt correct", counters, testInstance.getTotalAlterCount());
        doSimpleGeneration(expectedSQL);
    }
    @Test
    public void testDeleteColumn(){
        final String expectedSQL = "alter table " + tableName+ " DROP counter-1," +
                "DROP counter-2;" ;
        final int counters = 2;
        for(int i=1;i<=counters;i++){
            testInstance.deleteColumn("counter-" + i);
        }
        assertEquals("Number of counters altered isnt correct", counters, testInstance.getTotalAlterCount());
        doSimpleGeneration(expectedSQL);
    }
    private void doSimpleGeneration(final String expectedSQL){

        final Iterator<String> iter = testInstance.getIterator();
        if(!iter.hasNext()){
            fail("NO SQL Batch statements generated");
        }
        final String actualSql = iter.next();
        assertEquals("Wrong batch statement generated", expectedSQL, actualSql);
        if(iter.hasNext()){
            fail("Too many SQL Batch statements generated");
        }
    }
    public static TestSuite suite() {
        return new TestSuite(TableAlterDetailsTest.class);
    }
}
