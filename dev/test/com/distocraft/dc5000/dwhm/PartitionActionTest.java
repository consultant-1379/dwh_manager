package com.distocraft.dc5000.dwhm;


import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import org.jmock.Expectations;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.ericsson.eniq.common.testutilities.BaseUnitTestX;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;

public class PartitionActionTest extends BaseUnitTestX {

  private static final int MS_IN_DAY = 1000 * 60 * 24 * 60;

  private static final int MS_IN_FOUR_DAYS = MS_IN_DAY * 4;

  private static final int MS_IN_SIX_DAYS = MS_IN_DAY * 6;

  private static final int MS_IN_EIGHT_DAYS = MS_IN_DAY * 8;

  private static final int MS_IN_SEVEN_DAYS = MS_IN_DAY * 7;

  private static final String EVENT_E_SGEH_ERR_RAW_06 = "EVENT_E_SGEH_ERR_RAW_06";

  private static final String EVENT_E_SGEH_ERR_RAW_05 = "EVENT_E_SGEH_ERR_RAW_05";

  private static final String EVENT_E_SGEH_ERR_RAW_04 = "EVENT_E_SGEH_ERR_RAW_04";

  private static final String EVENT_E_SGEH_ERR_RAW_03 = "EVENT_E_SGEH_ERR_RAW_03";

  private static final String EVENT_E_SGEH_ERR_RAW_02 = "EVENT_E_SGEH_ERR_RAW_02";

  private static final String EVENT_E_SGEH_ERR_RAW_01 = "EVENT_E_SGEH_ERR_RAW_01";

  private final String latestTableNameForRawEvents = EVENT_E_SGEH_ERR_RAW_01;
  private final long partitionEndTime = 0L;

  private final String mockPhyTabCacheName = "mockPhysicalTableCacheName";

  Logger loggerForClass = Logger.getLogger("PartitionActionTest");
	private ActivationCache mockActivationCache = null;
	private static String rawStorageId = "DC_E_BSS_MOTS_RAW";

  private static String rawEventsStorageId = "EVENT_E_SGEH_ERR:RAW";
	private static String rawStorageIdUnpart = "DC_E_TEST_UNPART_RAW";
	private static String rawStorageIdFirstPart = "DC_E_FIRST_UNPART_RAW";
	private static String rawViewName = "DC_E_BSS_MOTS_RAW";

  private static String rawEventsViewName = "EVENT_E_SGEH_ERR_RAW";
	private static String rawViewNameUnpart = "DC_E_TEST_UNPART_RAW";
	private static String rawViewNameFirstPart ="DC_E_FIRST_UNPART_RAW"; 
	private static RockFactory dwhrep = null;
	static Connection dwhrepConnection = null;

	private final static String TESTDB_DRIVER = "org.hsqldb.jdbcDriver";
	private final static String DWHREP_URL = "jdbc:hsqldb:mem:dwhrep";

	private static Dwhtype dwhType;
	private static RockFactory etlrep = null;
	private static RockFactory dwhdb = null;
	private static RockFactory dwhdb_dba = null;

	private String techPackName = null;
	short partitionType;
	long defaultPartitionSize;
	long maxStorage = 90;

	private static final String LOG_DIR = System.getProperty("user.home");
	private static final String REJECTED_DIR = System.getProperty("user.home");

	@Before
	public void before() throws Exception {
		dwhdb_dba = DatabaseTestUtils.getTestDbConnection("SA", "");
		etlrep = dwhdb = dwhdb_dba;

		DatabaseTestUtils.loadSetup(dwhdb_dba, "dwhManagerRemoveDWH");
		final Properties p = new Properties();
		p.put("dwhm.debug", "true");
		final String currentLocation = System.getProperty("user.home");
		if (!currentLocation.endsWith("ant_common")) {
			p.put("dwhm.templatePath", ".\\jar\\"); // Gets tests running on
													// laptop
		}
		StaticProperties.giveProperties(p);
		System.setProperty("LOG_DIR", LOG_DIR);
		System.setProperty("REJECTED_DIR", REJECTED_DIR);

	}

	@After
	public void after() {
		DatabaseTestUtils.close(dwhdb_dba);
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		setupProperties();
		setupDwhRep();
		dwhType = new Dwhtype(dwhrep, rawStorageId);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		/* Cleaning up after test */
		Statement stmt = dwhrepConnection.createStatement();
		stmt.execute("DROP TABLE DWHPartition");
		stmt.execute("DROP TABLE DWHType");
		stmt.execute("DROP TABLE Dwhtechpacks");
		stmt.execute("DROP TABLE Versioning");
		stmt.execute("DROP TABLE PartitionPlan");
		stmt.execute("DROP TABLE DC_E_BSS_MOTS_RAW_01");
		stmt.execute("DROP SCHEMA dwhrep");
		dwhrepConnection = null;
		dwhrep = null;
		StaticProperties.giveProperties(new Properties());
	}

	private static void setupProperties() throws Exception {
		Properties props = new Properties();
		props.setProperty("DWHManager.viewCreateRetries", "2");
		props.setProperty("DWHManager.viewCreateRetryPeriod", "1");
		props.setProperty("DWHManager.viewCreateRetryRandom", "1");

		props.put("dwhm.debug", "false");

		StaticProperties.giveProperties(props);
	}

	private static void setupDwhRep() throws SQLException, RockException {
		dwhrep = new RockFactory(DWHREP_URL, "SA", "", TESTDB_DRIVER,
				"dwhrepConnection", true);

		dwhrepConnection = dwhrep.getConnection();

		Statement stmt1 = dwhrepConnection.createStatement();
		stmt1.execute("CREATE SCHEMA dwhrep AUTHORIZATION DBA");
		stmt1.close();

		createDwhTypeTable(dwhrepConnection);
		createDwhPartitionTable(dwhrepConnection, rawViewName, rawStorageId);
		createDwhTechPacks(dwhrepConnection);
		createVersioning(dwhrepConnection);
		createPartitionPlan(dwhrepConnection);
		createDWHTables(dwhrepConnection);
	}

	private static void createDwhTypeTable(Connection connection)
			throws SQLException {
		Statement stmt = connection.createStatement();
		stmt.execute("CREATE TABLE DWHType (TECHPACK_NAME varchar(30), TYPENAME  varchar(255), TABLELEVEL  varchar(50), STORAGEID varchar(255),"
				+ "PARTITIONSIZE numeric, PARTITIONCOUNT numeric, STATUS  varchar(50), TYPE  varchar(50), OWNER varchar(50), VIEWTEMPLATE varchar(255),"
				+ "CREATETEMPLATE  varchar(255), NEXTPARTITIONTIME timestamp, BASETABLENAME varchar(125), DATADATECOLUMN  varchar(128), PUBLICVIEWTEMPLATE varchar(255),"
				+ "PARTITIONPLAN varchar(128))");
		stmt.executeUpdate("INSERT INTO DWHType VALUES('DC_E_BSS','DC_E_BSS_MOTS','RAW', '"
				+ rawStorageId
				+ "',-1,15,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2011-03-17 00:00:00.0',"
				+ "'DC_E_BSS_MOTS_RAW','DATE_ID',"
				+ "'createpublicview.vm','large_raw')");
		stmt.executeUpdate("INSERT INTO DWHType VALUES('DC_E_TEST','DC_E_TEST_UNPART','RAW', '"
				+ rawStorageIdUnpart
				+ "',-1,15,'ENABLED','UNPARTITIONED','dc','createview.vm','createpartition.vm','2011-03-17 00:00:00.0',"
				+ "'DC_E_TEST_UNPART_RAW','DATE_ID',"
				+ "'createpublicview.vm','small_raw')");
		stmt.executeUpdate("INSERT INTO DWHType VALUES('DC_E_FIRST','DC_E_FIRST_UNPART','RAW', '"
				+ rawStorageIdFirstPart
				+ "',-1,15,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2011-03-17 00:00:00.0',"
				+ "'DC_E_FIRST_UNPART_RAW','DATE_ID',"
				+ "'createpublicview.vm','small_raw')");
    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_ERR','RAW', '" + rawEventsStorageId
        + "',-1,15,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2011-03-17 00:00:00.0',"
        + "'DC_E_BSS_MOTS_RAW','DATE_ID'," + "'createpublicview.vm','sgeh_raw')");

		stmt.close();
	}

	private static void createDWHTables(Connection connection)
			throws SQLException {
		Statement stmt = connection.createStatement();
    createTable(stmt, "DC_E_BSS_MOTS_RAW_01");
    createTable(stmt, "DC_E_BSS_MOTS_RAW_02");
    createTable(stmt, "DC_E_BSS_MOTS_RAW_03");
    createTable(stmt, "DC_E_BSS_MOTS_RAW_04");
    createTable(stmt, "DC_E_BSS_MOTS_RAW_05");
    createTable(stmt, "DC_E_BSS_MOTS_RAW_06");

    createTable(stmt, "DC_E_TEST_UNPART_RAW_01");
    createTable(stmt, "DC_E_TEST_UNPART_RAW_02");
    createTable(stmt, "DC_E_TEST_UNPART_RAW_03");
    createTable(stmt, "DC_E_TEST_UNPART_RAW_04");
    createTable(stmt, "DC_E_TEST_UNPART_RAW_05");
    createTable(stmt, "DC_E_TEST_UNPART_RAW_06");

    createTable(stmt, "DC_E_FIRST_UNPART_RAW_01");
    createTable(stmt, "DC_E_FIRST_UNPART_RAW_02");

    createTable(stmt, EVENT_E_SGEH_ERR_RAW_01);
    createTable(stmt, EVENT_E_SGEH_ERR_RAW_02);
    createTable(stmt, EVENT_E_SGEH_ERR_RAW_03);
    createTable(stmt, EVENT_E_SGEH_ERR_RAW_04);
    createTable(stmt, EVENT_E_SGEH_ERR_RAW_05);
    createTable(stmt, EVENT_E_SGEH_ERR_RAW_06);

    stmt.execute("CREATE TABLE EVENT_E_SGEH_ERR_DUBCHECK (TABLENAME varchar(50), FILENAME  varchar(255))");

		stmt.close();
	}

  protected static void createTable(final Statement stmt, final String table_name) throws SQLException {
    stmt.execute("CREATE TABLE "
        + table_name
        + " (OSS_ID varchar(50), SN  varchar(255), MOID  varchar(50), BSC varchar(255),"
        + "TS_NAME varchar(12), DATE_ID date, YEAR_ID smallint, MONTH_ID  tinyint, DAY_ID tinyint, HOUR_ID tinyint,"
        + "DATETIME_ID  timestamp, MIN_ID timestamp, TIMELEVEL varchar(125), SESSION_ID  int, BATCH_ID smallint,"
        + "PERIOD_DURATION int, ROWSTATUS varchar(12), DC_RELEASE varchar(12), DC_SOURCE varchar(12), DC_TIMEZONE varchar(12),"
        + "DC_SUSPECTFLAG int,UTC_DATETIME_ID timestamp, ID1 numeric, ID2 numeric, CONERRCNT numeric, CONCNT numeric)");
  }

  private static void createDwhPartitionTable(final Connection connection, final String viewName, final String storageId)
      throws SQLException {
    final Statement stmt = connection.createStatement();
		stmt.execute("CREATE TABLE DWHPartition (STORAGEID varchar(255),TABLENAME varchar(255),STARTTIME timestamp,ENDTIME timestamp,STATUS varchar(10),LOADORDER integer)");
		insertIntoDwhPartition(viewName, storageId, stmt);
		insertIntoDwhPartition(rawViewNameUnpart, rawStorageIdUnpart, stmt);
    insertIntoDwhPartition(rawEventsViewName, rawEventsStorageId, stmt);

		insertIntoDwhPartitionForFirstPart(rawViewNameFirstPart, rawStorageIdFirstPart, stmt);
		stmt.close();
	}

	private static void createPartitionPlan(Connection connection)
			throws SQLException {
		Statement stmt = connection.createStatement();
		stmt.execute("CREATE TABLE Partitionplan ( PARTITIONPLAN VARCHAR(31)  ,DEFAULTSTORAGETIME BIGINT  ,DEFAULTPARTITIONSIZE BIGINT  ,MAXSTORAGETIME BIGINT  ,PARTITIONTYPE SMALLINT)");
    stmt.executeUpdate("INSERT INTO Partitionplan VALUES( 'large_raw'  ,90  ,168  ,90  ,0 )");
    stmt.executeUpdate("INSERT INTO Partitionplan VALUES( 'small_raw'  ,90  ,384  ,90  ,0 )");
    stmt.executeUpdate("INSERT INTO Partitionplan VALUES( 'sgeh_raw'  ,200  ,400  ,200  ,1 )");
		stmt.close();
	}

	private static void createVersioning(Connection connection)
			throws SQLException {

		Statement stmt = connection.createStatement();
		stmt.execute("CREATE TABLE Versioning ( VERSIONID VARCHAR(31)  ,DESCRIPTION VARCHAR(31) ,STATUS BIGINT  ,TECHPACK_NAME VARCHAR(31) ,TECHPACK_VERSION VARCHAR(31) ,TECHPACK_TYPE VARCHAR(31) ,PRODUCT_NUMBER VARCHAR(31) ,LOCKEDBY VARCHAR(31) ,LOCKDATE TIMESTAMP  ,BASEDEFINITION VARCHAR(31) ,BASEVERSION VARCHAR(31) ,INSTALLDESCRIPTION VARCHAR(31) ,UNIVERSENAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,ENIQ_LEVEL VARCHAR(31) ,LICENSENAME VARCHAR(31))");
		stmt.executeUpdate("INSERT INTO Versioning VALUES( 'DC_E_BSS:((40))'  ,'Ericsson BSS'  ,1  ,'DC_E_BSS'  ,'testTECHPACK_VERSION'  ,'PM'  ,'COA 252 139/1'  ,'test'  ,'2000-01-01 00:00:00.0'  ,'testBASEDEFINITION'  ,'testBASEVERSION'  ,'testINSTALLDESCRIPTION'  ,'testUNIVERSENAME'  ,'testUNIVERSEEXTENSION'  ,'testENIQ_LEVEL'  ,'testLICENSENAME' )");
		stmt.executeUpdate("INSERT INTO Versioning VALUES( 'DC_E_TEST:((00))'  ,'TEST'  ,1  ,'DC_E_TEST'  ,'testTECHPACK_VERSION'  ,'PM'  ,'COA 252 139/1'  ,'test'  ,'2000-01-01 00:00:00.0'  ,'testBASEDEFINITION'  ,'testBASEVERSION'  ,'testINSTALLDESCRIPTION'  ,'testUNIVERSENAME'  ,'testUNIVERSEEXTENSION'  ,'testENIQ_LEVEL'  ,'testLICENSENAME' )");
		stmt.executeUpdate("INSERT INTO Versioning VALUES( 'DC_E_FIRST:((00))'  ,'FIRST'  ,1  ,'DC_E_FIRST'  ,'testTECHPACK_VERSION'  ,'PM'  ,'COA 252 139/1'  ,'test'  ,'2000-01-01 00:00:00.0'  ,'testBASEDEFINITION'  ,'testBASEVERSION'  ,'testINSTALLDESCRIPTION'  ,'testUNIVERSENAME'  ,'testUNIVERSEEXTENSION'  ,'testENIQ_LEVEL'  ,'testLICENSENAME' )");
    stmt.executeUpdate("INSERT INTO Versioning VALUES( 'EVENT_E_SGEH:((00))'  ,'FIRST'  ,1  ,'EVENT_E_SGEH'  ,'testTECHPACK_VERSION'  ,'ENIQ_EVENT'  ,'COA 252 139/1'  ,'test'  ,'2000-01-01 00:00:00.0'  ,'testBASEDEFINITION'  ,'testBASEVERSION'  ,'testINSTALLDESCRIPTION'  ,'testUNIVERSENAME'  ,'testUNIVERSEEXTENSION'  ,'testENIQ_LEVEL'  ,'testLICENSENAME' )");
	}

	private static void insertIntoDwhPartition(final String viewName,
			final String storageId, final Statement stmt) throws SQLException {
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_01',"
				+ "'2011-03-01 00:00:00.0','2011-03-07 00:00:00.0','ACTIVE', '0')");
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_02',"
				+ "'2011-03-07 00:00:00.0','2011-03-14 00:00:00.0','ACTIVE', '0')");
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_03',"
				+ "'2011-03-14 00:00:00.0','2011-03-21 00:00:00.0','ACTIVE', '0')");
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_04',"
				+ "'2011-03-21 00:00:00.0','2011-03-28 00:00:00.0','ACTIVE', '0')");
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_05',"
				+ "'2011-03-28 00:00:00.0','2011-04-03 00:00:00.0','ACTIVE', '0')");
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_06',"
				+ "'2011-04-03 00:00:00.0','2011-04-10 00:00:00.0','ACTIVE', '0')");
		

	}
	
	private static void insertIntoDwhPartitionForFirstPart(String viewName,
			String storageId, Statement stmt) throws SQLException {
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_01',"
				+ "NULL,'2011-03-07 00:00:00.0','ACTIVE', '0')");
		stmt.executeUpdate("insert into DWHPartition values ('"
				+ storageId
				+ "','"
				+ viewName
				+ "_02',"
				+ "NULL,'2011-03-14 00:00:00.0','ACTIVE', '0')");

	}

	private static void createDwhTechPacks(Connection connection)
			throws SQLException {

		Statement stmt = connection.createStatement();
		stmt.execute("CREATE TABLE Dwhtechpacks ( TECHPACK_NAME VARCHAR(31)  ,VERSIONID VARCHAR(31) ,CREATIONDATE TIMESTAMP )");
		stmt.executeUpdate("INSERT INTO Dwhtechpacks VALUES( 'DC_E_BSS' ,'DC_E_BSS:((40))'  ,'2000-01-01 00:00:00.0' )");
		stmt.executeUpdate("INSERT INTO Dwhtechpacks VALUES( 'DC_E_TEST' ,'DC_E_TEST:((00))'  ,'2000-01-01 00:00:00.0' )");
		stmt.executeUpdate("INSERT INTO Dwhtechpacks VALUES( 'DC_E_FIRST' ,'DC_E_FIRST:((00))'  ,'2000-01-01 00:00:00.0' )");
    stmt.executeUpdate("INSERT INTO Dwhtechpacks VALUES( 'EVENT_E_SGEH' ,'EVENT_E_SGEH:((00))'  ,'2000-01-01 00:00:00.0' )");
	}


	@Test
	public void checkPartitionAction() throws Exception {
		try {
			techPackName = "DC_E_BSS";
      partitionType = 1;
			defaultPartitionSize = 123L;
			createMockedActivationCache(techPackName);
			System.setProperty("dwhm.test", "yes");
			new StubbedPartitionAction(dwhrep, dwhrep, techPackName,
					loggerForClass);
		} catch (Exception e) {
			// fail the test case
			fail("Fail due to exception: " + e.getMessage());
		}finally {
		      System.clearProperty("dwhm.test");
	    }
	}

	@Test
	public void checkPartitionActionForUnpartitionedType() throws Exception {
		try {
			techPackName = "DC_E_TEST";
			partitionType = 1;
			defaultPartitionSize = 123L;
			createMockedActivationCache(techPackName);
			
			System.setProperty("dwhm.test", "yes");
			new StubbedPartitionAction(dwhrep, dwhrep, techPackName,
					loggerForClass);
		} catch (Exception e) {
			// fail the test case
			fail("Fail due to exception: " + e.getMessage());
		}finally {
		      System.clearProperty("dwhm.test");
	    }

	}
	
	@Test
	public void checkForFirstPartitioning() throws Exception {
		try {
			techPackName = "DC_E_FIRST";
			partitionType = 1;
			defaultPartitionSize = 123L;
			createMockedActivationCache(techPackName);
			System.setProperty("dwhm.test", "yes");
			new StubbedPartitionAction(dwhrep, dwhrep, techPackName,
					loggerForClass);
		} catch (Exception e) {
			// fail the test case
			fail("Fail due to exception: " + e.getMessage());
		}finally {
		      System.clearProperty("dwhm.test");
	    }

	}

	/** create mock context for Activation **/
	private void createMockedActivationCache(final String tpName)
			throws Exception {
		String activationCacheName = tpName + "_ActivationCache";
		mockActivationCache = context.mock(ActivationCache.class,
				activationCacheName);

		context.checking(new Expectations() {

			{
        allowing(mockActivationCache).isActive(with(any(String.class)));
				will(returnValue(true));
				allowing(mockActivationCache).isActive(with(any(String.class)),
						with(any(String.class)), with(any(String.class)));
				will(returnValue(true));

				allowing(mockActivationCache).getStorageTime(
						with(any(String.class)), with(any(String.class)),
						with(any(String.class)));
				will(returnValue(maxStorage));

			}
		});
	}
	

	/** Private class to override some functions **/

	private class StubbedPartitionAction extends PartitionAction {

    public StubbedPartitionAction(RockFactory reprock, RockFactory dwhrock, String techPack, Logger clog)
        throws Exception {
			super(reprock, dwhrock, techPack, clog);
		}

		@Override
		protected void doRevalidation() {

			System.out.println("Sent for revalidation");
		}


		@Override
		protected ActivationCache getActivationCache() {
			return mockActivationCache;
		}
	}
}
