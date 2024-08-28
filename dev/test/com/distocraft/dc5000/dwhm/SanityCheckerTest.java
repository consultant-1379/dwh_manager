package com.distocraft.dc5000.dwhm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;
import java.util.Set;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;

public class SanityCheckerTest {

	private static RockFactory rock = null;
    private static final String TESTDB_DRIVER = "org.hsqldb.jdbcDriver";
    private static final String DWHREP_URL = "jdbc:hsqldb:mem:testdb";    
    private static final String USER_NAME = "sa";   
    private static final String PASSWORD = "";    
    private static final String DATABASE_NAME= "test";

    private static Method columnCheckerMethod;
    private	static Method getPartitionTypeMethod;
    private static Method sortColumnsForEventsMethod;
    
	private final Mockery context = new JUnit4Mockery() {
	    {
	      setImposteriser(ClassImposteriser.INSTANCE);
	    }
	};
	
	private SanityChecker instance;
	private RockFactory mockReprock;
	private RockFactory mockDwhrock;
	private Logger mockLog;
	private Dwhtype mockDwhType;
	private Dwhpartition mockDwhPartition;
	

	@Before
	public void setUp() throws Exception {
    	rock = new RockFactory(DWHREP_URL, USER_NAME, PASSWORD, TESTDB_DRIVER, DATABASE_NAME, true);        
    	DatabaseTestUtils.loadSetup(rock, "dwhManagerRemoveDWH");
    	final Properties p = new Properties();        
    	p.put("dwhm.debug", "true");        
    	final String currentLocation = System.getProperty("user.home");        
    	if(!currentLocation.endsWith("ant_common")){        	
    		p.put("dwhm.templatePath", ".\\jar\\"); // Gets tests running on laptop        
    	}        
    	
    	//setup the Reflected Methods...
    	columnCheckerMethod = SanityChecker.class.getDeclaredMethod("columnCheck", List.class, Dwhpartition.class, Connection.class);
    	columnCheckerMethod.setAccessible(true);
    	getPartitionTypeMethod = SanityChecker.class.getDeclaredMethod("getPartitionType", Dwhtype.class);
    	getPartitionTypeMethod.setAccessible(true);
    	sortColumnsForEventsMethod = SanityChecker.class.getDeclaredMethod("sortColumnsForEvents", List.class);
    	sortColumnsForEventsMethod.setAccessible(true);
    	
    	StaticProperties.giveProperties(p);          
    	// Setting up database tables to be used in testing        

		mockDwhType = context.mock(Dwhtype.class, "mockDwhType");
		mockDwhPartition = context.mock(Dwhpartition.class, "mockDwhPartition");
		mockReprock = context.mock(RockFactory.class, "mockReprock");
		mockDwhrock = context.mock(RockFactory.class, "mockDwhrock");
		mockLog = context.mock(Logger.class, "mockLog");
		context.checking(new Expectations() {
			{
				allowing(mockLog).getName();
				will(returnValue("testLogger"));
			}
		});
		// Test instance
		instance = new SanityChecker(mockReprock, mockDwhrock, mockLog);
	}
	
	@After
	public void tearDown() throws Exception {
		DatabaseTestUtils.shutdown(rock);
		instance = null;
	}
	
	/**
	 * Test to check fix for HN77651:ENIQ: 11.2.12 Some 'select' tables have partitions marked as INSANE
	 * Unhandled new SQL Anywhere Error -141 exception meant a DWHPartition delete did not happen, leading to a FK constraint when delete done on DWHType.
	 * If "SQL Anywhere Error -141" SQLException occurs want Dwhpartition.deleteDB() to be called.
	 */
	@Test
	public void testExistenceCheckWithSQLAnywhereError141() {
		final String reason = "SQL Anywhere Error -141";
		final String sqlState = "";	
		final int errorCode = 2706;
	
		try {
			final SQLException sqlEx = new SQLException(reason, sqlState, errorCode);
			final Connection mockCon = context.mock(Connection.class); //NOPMD
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(throwException(sqlEx));
					//partition.getTablename()
					allowing(mockDwhPartition).getTablename();
					will(returnValue("SELECT_CUSTOM_E_CPP_AGGLEVEL"));
					//Testing that partition.deleteDB(); is called
					allowing(mockDwhPartition).deleteDB();
					will(returnValue(0));
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		} catch (Exception e) {
			fail("testExistenceCheckWithSQLAnywhereError141:Expected partition.deleteDB() to be called: Exception "+e);
		}
	} //testExistenceCheckWithSQLAnywhereError141
	
	@Test
	public void testExistenceCheckWithASAError141() {
		final String reason = "ASA Error -141";
		final String sqlState = "";	
		final int errorCode = 2706;
		
		try {
			final SQLException sqlEx = new SQLException(reason, sqlState, errorCode);
			final Connection mockCon = context.mock(Connection.class); //NOPMD
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(throwException(sqlEx));
					//partition.getTablename()
					allowing(mockDwhPartition).getTablename();
					will(returnValue("SELECT_CUSTOM_E_CPP_AGGLEVEL"));
					//Testing that partition.deleteDB(); is called
					allowing(mockDwhPartition).deleteDB();
					will(returnValue(0));
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		} catch (Exception e) {
			fail("testExistenceCheckWithASAError141: Expected partition.deleteDB() to be called: Exception "+e);
		}
	}

	@Test
	public void testExistenceCheckWithSQLAnywhereError141_EVENTS() {
		final String reason = "SQL Anywhere Error -141";
		final String sqlState = "S0002";	
		final int errorCode = 2706;
		
		try {
			final SQLException sqlEx = new SQLException(reason, sqlState, errorCode);
			
			final Connection mockCon = context.mock(Connection.class); //NOPMD
			context.checking(new Expectations() {
				{
					allowing(mockCon).createStatement();
					will(throwException(sqlEx));
					allowing(mockDwhPartition).getTablename();
					will(returnValue("SELECT_CUSTOM_E_CPP_AGGLEVEL"));
					//Testing that partition.deleteDB(); is called
					allowing(mockDwhPartition).deleteDB();
					will(returnValue(0));
				}
			});
			final Method existenceCheckForVolumeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForVolumeBasedPartitions", Dwhpartition.class, Connection.class);
			existenceCheckForVolumeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForVolumeBasedPartitionsMethod.invoke(instance, mockDwhPartition, mockCon);
		} catch (Exception e) {
			fail("testExistenceCheckWithSQLAnywhereError141:Expected partition.deleteDB() to be called: Exception "+e);
		}
	}

	@Test
	public void testExistenceCheckForTimeBasedPartitions_StarttimeIsNull(){
		try {

			final Connection mockCon = context.mock(Connection.class); //NOPMD
			final Statement statementMock = context.mock(Statement.class);
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(returnValue(statementMock));

					one(mockDwhType).getType();
					will(returnValue("UNPARTITIONED"));
					one(mockDwhPartition).getStatus();
					will(returnValue("Status of partition"));
					one(mockDwhPartition).getStarttime();
					will(returnValue(null));
					//Exception should be thrown
					
					//the finally block...
					one(statementMock).getMoreResults();
					will(returnValue(false));
					one(statementMock).close();
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		}catch(Exception e){
			String expectedMessage = "Partition of UNPARTITIONED type should have only starttime defined (and null endtime).";
			assertEquals("Should have got CheckForTimeBasedPartitionsException!", expectedMessage, e.getCause().getMessage());
		}
	}

	@Test
	public void testExistenceCheckForTimeBasedPartitions_dateColumnIsNull(){
		try {

			final Connection mockCon = context.mock(Connection.class); //NOPMD
			final Statement statementMock = context.mock(Statement.class);
			final Timestamp timestampMock = context.mock(Timestamp.class);
			
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(returnValue(statementMock));

					one(mockDwhType).getType();
					will(returnValue("UNPARTITIONED"));
					one(mockDwhPartition).getStatus();
					will(returnValue("Status of partition"));
					one(mockDwhPartition).getStarttime();
					will(returnValue(timestampMock));
					one(mockDwhPartition).getEndtime();
					will(returnValue(null));
					
					//want to make the Method throw an Exception...
					one(mockDwhType).getDatadatecolumn();
					will(returnValue(null));
					allowing(mockDwhPartition).getStatus();
					will(returnValue("INSANE"));
					//Exception should be thrown
					
					//the finally block...
					one(statementMock).getMoreResults();
					will(returnValue(false));
					one(statementMock).close();
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		}catch(Exception e){
			String expectedMessage = "Illegal partition status INSANE for UNPARTITIONED type";
			assertEquals("Should have got CheckForTimeBasedPartitionsException!", expectedMessage, e.getCause().getMessage());
		}
	}

	@Test
	public void testExistenceCheckForTimeBasedPartitions_dateColumnIsNull_PartitionACTIVE(){
		try {

			final Connection mockCon = context.mock(Connection.class); //NOPMD
			final Statement statementMock = context.mock(Statement.class);
			final Timestamp timestampMock = context.mock(Timestamp.class);
			final ResultSet resultsetMock = context.mock(ResultSet.class);
			
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(returnValue(statementMock));

					one(mockDwhType).getType();
					will(returnValue("UNPARTITIONED"));
					one(mockDwhPartition).getStatus();
					will(returnValue("Status of partition"));
					one(mockDwhPartition).getStarttime();
					will(returnValue(timestampMock));
					one(mockDwhPartition).getEndtime();
					will(returnValue(null));
					
					one(mockDwhType).getDatadatecolumn();
					will(returnValue(null));
					allowing(mockDwhPartition).getStatus();
					will(returnValue("ACTIVE"));
					one(mockDwhPartition).getTablename();
					will(returnValue("TABLE1"));
					one(statementMock).executeQuery("SELECT count(*) FROM TABLE1");
					will(returnValue(resultsetMock));
					one(resultsetMock).next();
					will(returnValue(true));
					one(resultsetMock).getInt(1);
					will(returnValue(1));
					
					//the finally block...
					one(resultsetMock).close();
					one(statementMock).getMoreResults();
					will(returnValue(false));
					one(statementMock).close();
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		}catch(Exception e){
			fail("Test Failed: "+e.getCause().toString());
		}
	}

	@Test
	public void testExistenceCheckForTimeBasedPartitions_DwhtypeIsUNPARTITIONED(){
		try {

			final Connection mockCon = context.mock(Connection.class); //NOPMD
			final Statement statementMock = context.mock(Statement.class);
			final ResultSet resultsetMock = context.mock(ResultSet.class);
			final String dateColumn = "20.04.2011";
			
			final Timestamp partitionTS = new Timestamp(1234567);
			final Timestamp earliestTS  = new Timestamp(12345678);	
			final Timestamp latestTS    = new Timestamp(123456789);

			
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(returnValue(statementMock));

					one(mockDwhType).getType();
					will(returnValue("UNPARTITIONED"));
					one(mockDwhPartition).getStatus();
					will(returnValue("Status of partition"));
					one(mockDwhPartition).getStarttime();
					will(returnValue(partitionTS));
					one(mockDwhPartition).getEndtime();
					will(returnValue(null));
					
					one(mockDwhType).getDatadatecolumn();
					will(returnValue(dateColumn));
					allowing(mockDwhPartition).getStatus();
					will(returnValue("ACTIVE"));
					
					one(mockDwhPartition).getTablename();
					will(returnValue("TABLE1"));
					one(statementMock).executeQuery("SELECT min(20.04.2011),max(20.04.2011) FROM TABLE1");
					will(returnValue(resultsetMock));
					one(resultsetMock).next();
					will(returnValue(true));
					
					one(resultsetMock).getTimestamp(1);
					will(returnValue(earliestTS));
					
					one(resultsetMock).wasNull();
					will(returnValue(false));
					one(mockDwhPartition).getStarttime();
					will(returnValue(partitionTS));
					
					one(resultsetMock).getTimestamp(2);
					will(returnValue(latestTS));
					one(resultsetMock).wasNull();
					will(returnValue(true));
					//the finally block...
					one(resultsetMock).close();
					one(statementMock).getMoreResults();
					will(returnValue(false));
					one(statementMock).close();
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		}catch(Exception e){
			fail("Test Failed: "+e.getCause().toString());
		}
	}

	//TODO: Here
	@Test
	public void testExistenceCheckForTimeBasedPartitions_DwhtypeIsPARTITIONED(){
		try {

			final Connection mockCon = context.mock(Connection.class); //NOPMD
			final Statement statementMock = context.mock(Statement.class);
			final ResultSet resultsetMock = context.mock(ResultSet.class);
			
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(returnValue(statementMock));

					allowing(mockDwhType).getType();
					will(returnValue("PARTITIONED"));
					allowing(mockDwhPartition).getStatus();
					will(returnValue("NEW"));
					one(mockDwhPartition).getTablename();
					will(returnValue("TABLE1"));
					one(statementMock).executeQuery("SELECT count(*) FROM TABLE1");
					will(returnValue(resultsetMock));

					one(resultsetMock).next();
					will(returnValue(true));
					one(resultsetMock).getInt(1);
					will(returnValue(0));

					//the finally block...
					one(resultsetMock).close();
					one(statementMock).getMoreResults();
					will(returnValue(false));
					one(statementMock).close();
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		}catch(Exception e){
			fail("Test Failed: "+e.getCause().toString());
		}
	}

	//TODO: Here
	@Test
	public void testExistenceCheckForTimeBasedPartitions_DwhtypeIsSIMPLE(){
		try {

			final Connection mockCon = context.mock(Connection.class); //NOPMD
			final Statement statementMock = context.mock(Statement.class);
			final ResultSet resultsetMock = context.mock(ResultSet.class);
			
			context.checking(new Expectations() {
				{
					//stmt = con.createStatement();
					allowing(mockCon).createStatement();
					will(returnValue(statementMock));

					allowing(mockDwhType).getType();
					will(returnValue("SIMPLE"));
					allowing(mockDwhPartition).getStatus();
					will(returnValue("INSANE_AC"));
					
					one(mockDwhPartition).getStarttime();
					will(returnValue(new Timestamp(123456)));
					one(mockDwhPartition).getEndtime();
					will(returnValue(null));
					one(mockDwhPartition).getTablename();
					will(returnValue("TABLE1"));

					one(statementMock).executeQuery("SELECT count(*) FROM TABLE1");
					will(returnValue(resultsetMock));

					one(resultsetMock).next();
					will(returnValue(true));
					one(resultsetMock).getInt(1);
					will(returnValue(0));

					//the finally block...
					one(resultsetMock).close();
					one(statementMock).getMoreResults();
					will(returnValue(false));
					one(statementMock).close();
				}
			});
			final Method existenceCheckForTimeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForTimeBasedPartitions", Dwhtype.class, Dwhpartition.class, Connection.class);
			existenceCheckForTimeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForTimeBasedPartitionsMethod.invoke(instance, mockDwhType, mockDwhPartition, mockCon);
		}catch(Exception e){
			fail("Test Failed: "+e.getCause().toString());
		}
	}

	@Test
	public void testExistenceCheckForVolumeBasedPartitions(){
		final Connection mockCon = context.mock(Connection.class); //NOPMD
		final Statement statementMock = context.mock(Statement.class);
		final ResultSet resultSetMock = context.mock(ResultSet.class);

		try {
			context.checking(new Expectations() {
				{
					allowing(mockCon).createStatement();
					will(returnValue(statementMock));
					allowing(mockDwhPartition).getTablename();
					will(returnValue("SELECT_CUSTOM_E_CPP_AGGLEVEL"));
					
					allowing(statementMock).executeQuery("SELECT count(*) FROM SELECT_CUSTOM_E_CPP_AGGLEVEL");
					will(returnValue(resultSetMock));
					allowing(resultSetMock).next();
					will(returnValue(true));
					allowing(resultSetMock).getInt(1);
					will(returnValue(1));
					
					allowing(resultSetMock).close();
					allowing(statementMock).close();
				}
			});

			final Method existenceCheckForVolumeBasedPartitionsMethod = SanityChecker.class.getDeclaredMethod("existenceCheckForVolumeBasedPartitions", Dwhpartition.class, Connection.class);
			existenceCheckForVolumeBasedPartitionsMethod.setAccessible(true);
			existenceCheckForVolumeBasedPartitionsMethod.invoke(instance, mockDwhPartition, mockCon);		
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testSetStatusOfINSANEPartition_INSANE_AC(){
		testSetStatusOfINSANEPartition("INSANE_AC", "ACTIVE");
	}

	@Test
	public void testSetStatusOfINSANEPartition_INSANE_MA(){
		testSetStatusOfINSANEPartition("INSANE_MA", "MANUAL");
	}
	
	@Test
	public void testSetStatusOfINSANEPartition_INSANE_MG(){
		testSetStatusOfINSANEPartition("INSANE_MG", "MIGRATED");
	}
	
	@Test
	public void testSetStatusOfINSANEPartition_INSANE_NE(){
		testSetStatusOfINSANEPartition("INSANE_NE", "NEW");
	}

	@Test
	public void testSetStatusOfINSANEPartition_INSANE_RO(){
		testSetStatusOfINSANEPartition("INSANE_RO", "READONLY");
	}

	@Test
	public void testSetStatusOfINSANEPartition_UndefinedStatus(){
		testSetStatusOfINSANEPartition("INSANE_XX", "READONLY");
	}

	@Test
	public void testSetStatusOfPartition_ACTIVE(){
		testSetStatusOfPartition("ACTIVE", "INSANE_AC", "setValue");
	}

	@Test
	public void testSetStatusOfPartition_MANUAL(){
		testSetStatusOfPartition("MANUAL", "INSANE_MA", "setValue");
	}
	
	@Test
	public void testSetStatusOfPartition_MIGRATED(){
		testSetStatusOfPartition("MIGRATED", "INSANE_MG", "setValue");
	}

	@Test
	public void testSetStatusOfPartition_READONLY(){
		testSetStatusOfPartition("READONLY", "INSANE_RO", "setValue");
	}

	@Test
	public void testSetStatusOfPartition_NEW(){
		testSetStatusOfPartition("NEW", "INSANE_NE", "setValue");
	}

	@Test
	public void testSetStatusOfPartition_INSANE(){
		testSetStatusOfPartition("INSANE", "", "setValue");
	}

	@Test
	public void testSetStatusOfPartition_UNDEFINED(){
		testSetStatusOfPartition("UNDEFINED", "INSANE", null);
	}


	@Test
	public void testSanityCheck_NoPartitionsFound(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);

		context.checking(new Expectations (){
			{
				allowing(mockDwhType).getStorageid();
				will(returnValue("DC_E_XXX_ATMPORTBH:RANKBH"));
			}
		});
		assertTrue("The sanity check should have found no partitions for DC_E_XXX_ATMPORTBH:RANKBH", sanityChecker.sanityCheck(mockDwhType));
	}

	@Test
	public void testGetPartitionType(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		try {
			final Dwhtype dwhtype = new Dwhtype(rock);
			dwhtype.setPartitionplan("medium_day");
			final Short result = (Short)getPartitionTypeMethod.invoke(sanityChecker, dwhtype);
			assertTrue("The partition type should be 0, (Stats)", result.intValue() == 0);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSortColumnsForEvents(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		final Dwhtype dwhtype = new Dwhtype(rock);
		
		dwhtype.setStorageid("DC_E_MGW_ATMPORT:DAY");
		dwhtype.setBasetablename("DC_E_MGW_ATMPORT_DAY");
		dwhtype.setType("PARTITIONED");
		
		try {
			Dwhcolumn col1 = new Dwhcolumn(rock);
			col1.setDataname("A1_1");
			Dwhcolumn col2 = new Dwhcolumn(rock);
			col2.setDataname("A1_3");
			Dwhcolumn col3 = new Dwhcolumn(rock);
			col3.setDataname("A1_2");
			Dwhcolumn col4 = new Dwhcolumn(rock);
			col4.setDataname("A1_4");
			
			List<Dwhcolumn> unsorted = new ArrayList<Dwhcolumn>();
			unsorted.add(col1);
			unsorted.add(col2);
			unsorted.add(col3);
			unsorted.add(col4);
			//sanityChecker.sortColumnsForEvents(unsorted);
			sortColumnsForEventsMethod.invoke(sanityChecker, unsorted);
			
			//Check if the list is sorted properly...
			assertTrue("Expected A1_1 to be in position 0 of the sorted list.", unsorted.get(0).getDataname().equalsIgnoreCase("A1_1"));
			assertTrue("Expected A1_2 to be in position 1 of the sorted list.", unsorted.get(1).getDataname().equalsIgnoreCase("A1_2"));
			assertTrue("Expected A1_3 to be in position 2 of the sorted list.", unsorted.get(2).getDataname().equalsIgnoreCase("A1_3"));
			assertTrue("Expected A1_4 to be in position 3 of the sorted list.", unsorted.get(3).getDataname().equalsIgnoreCase("A1_4"));
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSanityCheck_PartitionsFound(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		final long partitionCount = 6;

		final Dwhtype dwhtype = new Dwhtype(rock);
		
		dwhtype.setStorageid("DC_E_MGW_ATMPORT:DAY");
		dwhtype.setBasetablename("DC_E_MGW_ATMPORT_DAY");
		dwhtype.setPartitioncount(partitionCount);
		dwhtype.setType("PARTITIONED");

		assertTrue("The Partition Sanity Check should true", sanityChecker.sanityCheck(dwhtype));
		
		//Check that none of the Partitions have been set INSANE...
		try {
			final Dwhpartition dp_cond = new Dwhpartition(rock);
			dp_cond.setStorageid("DC_E_MGW_ATMPORT:DAY");

			final DwhpartitionFactory dp_fact = new DwhpartitionFactory(rock, dp_cond);
			
			final Vector<Dwhpartition> partitions = dp_fact.get();
			final Iterator<Dwhpartition> itr = partitions.iterator();
			
			while(itr.hasNext()){
				assertFalse("The Partition should not be set INSANE_*", itr.next().getStatus().startsWith("INSANE"));
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}    
	}
	
	/*This test checks to see if the sanity check will set one Partition INSANE if 
	 *its column check fails. 
	 */
	@Test
	public void testSanityCheck_INSANEPartitions(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		final long partitionCount = 6;

		final Dwhtype dwhtype = new Dwhtype(rock);

		dwhtype.setStorageid("DC_E_MGW_ATMPORT:DAY");
		dwhtype.setBasetablename("DC_E_MGW_ATMPORT_DAY");
		dwhtype.setPartitioncount(partitionCount);
		dwhtype.setType("PARTITIONED");

		assertTrue("Should be able to purposly make the Partition go INSANE (just for test purposes)", makePartitionGoINSANE("DC_E_MGW_ATMPORT_DAY_01"));

		assertFalse("The Partition Sanity Check should false, because we have removed a column from SYS.SYSCOLUMNS", sanityChecker.sanityCheck(dwhtype));

		//Check that none of the Partitions have been set INSANE...
		try {
			final Dwhpartition dp_cond = new Dwhpartition(rock);
			dp_cond.setStorageid("DC_E_MGW_ATMPORT:DAY");

			final DwhpartitionFactory dp_fact = new DwhpartitionFactory(rock, dp_cond);

			final Vector<Dwhpartition> partitions = dp_fact.get();
			final Iterator<Dwhpartition> itr = partitions.iterator();
			Dwhpartition partition = null;
			while(itr.hasNext()){
				partition = itr.next();
				if(partition.getTablename().equalsIgnoreCase("DC_E_MGW_ATMPORT_DAY_01")){
					assertTrue("The Partition should be set INSANE_AC", partition.getStatus().startsWith("INSANE"));					
				}else{
					assertFalse("The Partition should not be set INSANE_*", partition.getStatus().startsWith("INSANE"));
				}
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}    
	}

	@Test
	public void testSanityCheckOfTechpack(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		
		final Dwhtechpacks tp = new Dwhtechpacks(rock);
		tp.setTechpack_name("DC_E_MGW_TEST");
		tp.setVersionid("DC_E_MGW:((666))");
		assertTrue("The TP: DC_E_MGW_TEST, should not have any insane partitions", sanityChecker.sanityCheck(tp));
	}

	
	@Test
	public void testSanityCheckOfTechpack_WithOneTableINSANE(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);

		assertTrue("Should be able to purposly make the Partition go INSANE (just for test purposes)", makePartitionGoINSANE("DC_E_MGW_ATMPORT_DAY_03"));
		
		final Dwhtechpacks tp = new Dwhtechpacks(rock);
		tp.setTechpack_name("DC_E_MGW_TEST");
		tp.setVersionid("DC_E_MGW:((666))");
		assertFalse("The TP: DC_E_MGW_TEST, should not have any insane partitions", sanityChecker.sanityCheck(tp));
	}

	@Test
	public void testColumnCheckPASS(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		try {
			final Dwhpartition dp_cond = new Dwhpartition(rock);
			dp_cond.setStorageid("DC_E_MGW_ATMPORT:DAY");
			dp_cond.setTablename("DC_E_MGW_ATMPORT_DAY_01");
			
			Connection con = rock.getConnection();//NOPMD

			final Dwhcolumn dc_cond = new Dwhcolumn(rock);
			dc_cond.setStorageid("DC_E_MGW_ATMPORT:DAY");
			final DwhcolumnFactory dc_fact = new DwhcolumnFactory(rock, dc_cond);

			final Vector<Dwhcolumn> columns = dc_fact.get();

			columnCheckerMethod.invoke(sanityChecker, columns, dp_cond, con);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSanityCheck_NO_TP(){
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		Dwhtechpacks tp = null;
		assertTrue("sanityCheck should return null if the supplied TP == null", sanityChecker.sanityCheck(tp));	
	}
	
	
	@Test  (expected = Exception.class)
	public void testColumnCheckFAIL() throws Exception{
		final SanityChecker sanityChecker = new SanityChecker(rock, rock, mockLog);
		final String tableName = "DC_E_MGW_ATMPORT_DAY_01";

		final Dwhpartition dp_cond = new Dwhpartition(rock);
		dp_cond.setStorageid("DC_E_MGW_ATMPORT:DAY");
		dp_cond.setTablename(tableName);

		Connection con = rock.getConnection(); //NOPMD

		final Dwhcolumn dc_cond = new Dwhcolumn(rock);
		dc_cond.setStorageid("DC_E_MGW_ATMPORT:DAY");
		final DwhcolumnFactory dc_fact = new DwhcolumnFactory(rock, dc_cond);

		final Vector<Dwhcolumn> columns = dc_fact.get();

		makePartitionGoINSANE(tableName);

		columnCheckerMethod.invoke(sanityChecker, columns, dp_cond, con);

	}

	private void testSetStatusOfINSANEPartition(final String oldStatus, final String newStatus){
		Method setStatusOfINSANEPartitionMethod;
		try {
			context.checking(new Expectations() {
				{
					allowing(mockDwhPartition).getStatus();
					will(returnValue(oldStatus));
					allowing(mockDwhPartition).setStatus(newStatus);
					allowing(mockDwhPartition).updateDB();
					
					//All the calls during the logging...
					allowing(mockDwhPartition).getStorageid();
					will(returnValue("storageId"));
					allowing(mockDwhPartition).getTablename();
					will(returnValue("tableName"));
					allowing(mockDwhPartition).getStatus();
					will(returnValue(newStatus));
				}
			});
			setStatusOfINSANEPartitionMethod = SanityChecker.class.getDeclaredMethod("setStatusOfINSANEPartition", Dwhpartition.class);
			setStatusOfINSANEPartitionMethod.setAccessible(true);
			setStatusOfINSANEPartitionMethod.invoke(instance, mockDwhPartition);		
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	private void testSetStatusOfPartition(final String oldStatus, final String newStatus, final String valueForSet){
		
		final Set<String> setOfModifiedColumns = new HashSet<String>();
		if(valueForSet != null){
			setOfModifiedColumns.add(valueForSet);
		}
		Method setStatusOfPartitionMethod;
		try {
			context.checking(new Expectations() {
				{
					allowing(mockDwhPartition).getStatus();
					will(returnValue(oldStatus));
					allowing(mockDwhPartition).setStatus(newStatus);
					
					if(oldStatus.equalsIgnoreCase("UNDEFINED")){
						allowing(mockDwhPartition).getTablename();
						will(returnValue("someTableName"));
					}
					//check to see if the partition should be saved...
					one(mockDwhPartition).gimmeModifiedColumns();
					will(returnValue(setOfModifiedColumns));
					
					if(valueForSet != null){
						one(mockDwhPartition).updateDB();
					}
				}
			});
			setStatusOfPartitionMethod = SanityChecker.class.getDeclaredMethod("setStatusOfPartition", Dwhpartition.class);
			setStatusOfPartitionMethod.setAccessible(true);
			setStatusOfPartitionMethod.invoke(instance, mockDwhPartition);		
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	private boolean makePartitionGoINSANE(final String tableName) {
		boolean success = true;
		Statement stmt = null;
		try {
			final String query = "DELETE FROM sys.SYSCOLUMNS WHERE tname like '"+tableName+"%' and cname like 'pmSecondsWithUnexp%';";
			Connection con = rock.getConnection(); //NOPMD
			stmt = con.createStatement();
			stmt.execute(query.toString());
		} catch (SQLException e1) {
			success = false;
			e1.printStackTrace();
		}finally{
			if (stmt != null){
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}			
		}
		return success;
	}
} //class SanityCheckerTest
