package com.distocraft.dc5000.dwhm;

//import static org.easymock.EasyMock.expect;
//import static org.easymock.EasyMock.expectLastCall;
//import static org.easymock.classextension.EasyMock.createMock;
//import static org.easymock.classextension.EasyMock.createNiceMock;
//import static org.easymock.classextension.EasyMock.replay;
//import static org.easymock.classextension.EasyMock.verify;
import static org.junit.Assert.*;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;import org.junit.Ignore;
import org.junit.Before;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.ericsson.eniq.common.TechPackType;
import com.ericsson.eniq.common.Utils;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;

import org.jmock.Expectations;

public class CreateViewsActionTest extends CreateViewsActionBaseUnitTestX{
	
	private RockFactory rock = null;
	private Dwhtype dwhtypeInstance ;
	private String storageIde = "DC_E_MGW_ATMPORT:COUNT" ;
    
	private RockFactory mockReprock;
	private RockFactory mockDwhrock;
	private RockFactory mockDbadwhrock ;
	private Dwhtype mockDwhType;
	private Logger mockLog;
	private Dwhcolumn mockDwhcolumn1;
	private Dwhcolumn mockDwhcolumn2;
	private Dwhcolumn mockDwhcolumn3;
	private Dwhcolumn mockDwhcolumn4;
	private Dwhcolumn mockDwhcolumn5;
	private Dwhcolumn mockDwhcolumn6;
	private Utils mockUtils;
	private Dwhtechpacks mockDwhTPType;
	private DwhtechpacksFactory mockDwhTpFactory;
	
	private static boolean started = false ;
	
	/*****************************************EVENTS*********************/

	  private static RockFactory dwhrep = null;

	  static Connection dwhrepConnection = null;

	  private final Logger clog = Logger.getLogger("CreateViewsActionTest");

	  private final static String TESTDB_DRIVER = "org.hsqldb.jdbcDriver";

	  private final static String DWHREP_URL = "jdbc:hsqldb:mem:dwhrep";

	  private static String rawStorageId = "EVENT_E_SGEH_SUCCESS:RAW";

	  private static String rawLev2StorageId = "EVENT_E_SGEH_SUCCESS:RAW_LEV2";

	  private static String testStorageId = "EVENT_E_SGEH_SUCCESS:TEST";

	  private static String rawViewName = "EVENT_E_SGEH_SUCCESS_RAW";

	  private static String rawLev2ViewName = "EVENT_E_SGEH_SUCCESS_RAW_LEV2";

	  private static String testViewName = "EVENT_E_SGEH_SUCCESS_TEST";

	  private final String sqlStatementForCreatingDcView =  "\n\nIF (SELECT count(*) FROM SYSVIEWS WHERE viewname='"	      + rawViewName	      + "' AND vcreator='dc') > 0\n\nBEGIN\n\n  DROP VIEW dc."	      + rawViewName	      + "\n\nEND\n\n\n\nCOMMIT\n\n\n\n\n\n\n\nCREATE VIEW dc."	      + rawViewName	      + " AS\n\n\nSELECT * FROM dc."	      + rawViewName	      + "_01\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."	      + rawViewName	      + "_02\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."	      + rawViewName	      + "_03\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."	      + rawViewName	      + "_04\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."	      + rawViewName	      + "_05\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."	      + rawViewName	      + "_06\n\n\n\n\n\n\nGRANT SELECT ON dc." + rawViewName + " TO dc\n\nGRANT SELECT ON dc." + rawViewName
	      + " TO dcbo\n\n\n\nCOMMIT\n\n\n\n\n\n\n\n\n\n\n \n\n\n \n\n\n \n";
	  
	  private final String sqlStatementForCreatingTimeRangeDcView = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE' AND vcreator='dc') > 0\nBEGIN\n  DROP VIEW dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE\nEND\n\ncommit\n\n\nCREATE VIEW dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE AS\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_01' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_01\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_02' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_02\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_03' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_03\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_04' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_04\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_05' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_05\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_06' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_06\n\nGRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE TO dc\nGRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE TO dcbo\n\ncommit\n\n" ;

	  private final String sqlStatementForCreatingDcViewWithRawLev2Tables = "\n\nIF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_SUCCESS_RAW' AND vcreator='dc') > 0\n\nBEGIN\n\n  DROP VIEW dc.EVENT_E_SGEH_SUCCESS_RAW\n\nEND\n\n\n\nCOMMIT\n\n\n\n\n\n\n\nCREATE VIEW dc.EVENT_E_SGEH_SUCCESS_RAW AS\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_01\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_02\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_03\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_04\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_05\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_06\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_01\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_02\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_03\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_04\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_05\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_06\n\n\n\n\n\n\nGRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_RAW TO dc\n\nGRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_RAW TO dcbo\n\n\n\nCOMMIT\n\n\n\n\n\n\n\n\n\n\n \n\n\n \n\n\n \n" ;
	  private final String sqlStatementForCreatingTimeRangeDcViewWithRawLev2Tables = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE' AND vcreator='dc') > 0\nBEGIN\n  DROP VIEW dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE\nEND\n\ncommit\n\n\nCREATE VIEW dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE AS\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_01' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_01\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_02' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_02\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_03' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_03\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_04' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_04\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_05' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_05\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_06' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_06\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_LEV2_01' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_01\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_LEV2_02' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_02\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_LEV2_03' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_03\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_LEV2_04' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_04\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_LEV2_05' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_05\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUCCESS_RAW_LEV2_06' AS TABLENAME FROM dc.EVENT_E_SGEH_SUCCESS_RAW_LEV2_06\n\nGRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE TO dc\nGRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_RAW_TIMERANGE TO dcbo\n\ncommit\n\n" ;

	 private final String sqlStatementForCreatingDcViewWithRawLev2TablesFirst = "\n\nIF (SELECT count(*) FROM SYSVIEWS WHERE viewname='"			 + rawViewName 			 + "' AND vcreator='dc') > 0\n\nBEGIN\n\n  DROP VIEW dc."			 + rawViewName			 + "\n\nEND\n\n\n\nCOMMIT\n\n\n\n\n\n\n\nCREATE VIEW dc."			 + rawViewName			 + " AS\n\n\nSELECT * FROM dc."			 + rawLev2ViewName			 + "_01\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawLev2ViewName			 + "_02\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawLev2ViewName			 + "_03\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawLev2ViewName			 + "_04\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawLev2ViewName			 + "_05\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawLev2ViewName			 + "_06\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawViewName			 + "_01\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawViewName			 + "_02\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawViewName			 + "_03\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawViewName			 + "_04\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawViewName			 + "_05\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM dc."			 + rawViewName			 + "_06\n\n\n\n\n\n\nGRANT SELECT ON dc."			 + rawViewName			 + " TO dc\n\nGRANT SELECT ON dc." + rawViewName + " TO dcbo\n\n\n\nCOMMIT\n\n\n\n\n\n\n\n\n\n\n \n\n\n \n\n\n \n" ;			  
	  private final String sqlStatementForCreatingDcpublicView = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='"
	      + rawViewName + "' AND vcreator='dcpublic') > 0\nBEGIN\n  DROP VIEW dcpublic." + rawViewName
	      + "\nEND\n\ncommit\n\n\n\n\nCREATE VIEW dcpublic." + rawViewName + " AS \nSELECT\nIMSI_MCC\nFROM dc."
	      + rawViewName + "\n\nGRANT SELECT on dcpublic." + rawViewName + " TO dcpublic\n\ncommit\n\n";

	  private static Dwhtype dwhType;

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
	    stmt.execute("DROP TABLE DWHColumn");
	    stmt.execute("DROP TABLE DWHType");
	    stmt.execute("DROP TABLE Dwhtechpacks");
	    stmt.execute("DROP TABLE Versioning");
	    stmt.execute("DROP SCHEMA dwhrep");
	    dwhrepConnection = null;
	    dwhrep = null;
	    StaticProperties.giveProperties(new Properties());	    	    System.out.println("+++++++++++++++++++Stoping Test Case+++++++++++++++++++++++++++++");
	  }

	  @Before
	  public void setUp() {		  System.out.println("+++++++++++++++++++Starting Test Case+++++++++++++++++++++++++++++");
	    recreateMockeryContext();
	  }

	  @Test
	  public void checkThatCorrectSQLStatementIsExecutedForDcAndDcPublicView() throws Exception {
	    // SQL statements, sqlStatementForCreatingDcpublicView and
	    // sqlStatementForCreatingDcView should match the generated sql statement
	    // from createview.vm
	    TechPackType techPackType = TechPackType.EVENTS;
	    RockFactory mockDwhdb = createMockConnectionForDbWithTwoSqlStatements("dwhdbConnection", "dwhdbStatement", "DWHDB",
	        sqlStatementForCreatingDcView, sqlStatementForCreatingTimeRangeDcView);
	    RockFactory mockDBADwhdb = createMockConnectionAndStatementForDb("DBADwhdbConnection", "DBADwhdbStatement",
	        "DBADwhdb",
	        sqlStatementForCreatingDcpublicView);
	    new CreateViewsAction(mockDBADwhdb, mockDwhdb, dwhrep, dwhType, clog,techPackType);
	  }


	  @Test
	  public void checkCorrectSQLStatementIsCreatedForDcAndDcPublicViewWhenRawAndRawLev2PartitionsExist() throws Exception {
	    Statement stmt = dwhrepConnection.createStatement();
	    TechPackType techPackType = TechPackType.EVENTS;
	    insertIntoDwhPartition(rawLev2ViewName, rawLev2StorageId, stmt);
	    stmt.close();
	    // SQL statements, sqlStatementForCreatingDcViewWithRawLev2Tables and
	    // sqlStatementForCreatingDcpublicView should match the generated sql
	    // statement from createview.vm and createpublicview.vm
	    RockFactory mockDwhdb = createMockConnectionForDbWithTwoSqlStatements("dwhdbCon", "dwhdbStat", "DWHDB12",
	        sqlStatementForCreatingDcViewWithRawLev2Tables, sqlStatementForCreatingTimeRangeDcViewWithRawLev2Tables);
	    RockFactory mockDBADwhdb = createMockConnectionAndStatementForDb("DBADwhdbCon", "DBADwhdbStat", "DBADwhdb12",
	        sqlStatementForCreatingDcpublicView);
	    new CreateViewsAction(mockDBADwhdb, mockDwhdb, dwhrep, dwhType, clog,techPackType);
	  }

	  @Test	 
	  public void checkSQLStatementIsCorrectForViewWhenRawAndRawLev2PartitionsAreSwitchedAround() throws Exception {
	    Statement stmt = dwhrepConnection.createStatement();
	    TechPackType techPackType = TechPackType.EVENTS;
	    stmt.executeUpdate("delete from DWHPartition");
	    // Add RAW_LEV2 to DHWType
	    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_SUCCESS','RAW_LEV2', '"
	        + rawLev2StorageId
	        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
	        + "'EVENT_E_SGEH_SUCCESS_RAW_LEV2','DATE_ID'," + "'createpublicview.vm','sgeh_raw_lev2')");
	    insertIntoDwhPartition(rawLev2ViewName, rawLev2StorageId, stmt);

	    insertIntoDwhPartition(rawViewName, rawStorageId, stmt);

	    stmt.executeUpdate("insert into DWHColumn values ('" + rawLev2StorageId
	        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
	    stmt.close();
	    // Let dwhType have RAW_LEV2 storage ID instead of RAW
	    dwhType = new Dwhtype(dwhrep, rawLev2StorageId);
	    // SQL statements, sqlStatementForCreatingDcViewWithRawLev2TablesFirst and
	    // sqlStatementForCreatingDcpublicView should match the generated sql
	    // statement from createview.vm and createpublicview.vm
	    RockFactory mockDwhdb = createMockConnectionAndStatementForDb("dwhdbConnect", "dwhdbStmt", "DWHDB13",
	        sqlStatementForCreatingDcViewWithRawLev2TablesFirst);
	    RockFactory mockDBADwhdb = createMockConnectionAndStatementForDb("DBADwhdbConnect", "DBADwhdbStmt", "DBADwhdb13",
	        sqlStatementForCreatingDcpublicView);
	    new CreateViewsAction(mockDBADwhdb, mockDwhdb, dwhrep, dwhType, clog,techPackType);
	  }

	  @Test	  
	  public void checkSQLStatementIsCorrectForViewOfOneMinuteTables() throws Exception {
		  TechPackType techPackType = TechPackType.EVENTS;
	    String sqlStatementForCreating1MinViewForDc = "\nIF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_SUCCESS_1MIN' AND vcreator='dc') > 0\nBEGIN\n  DROP VIEW dc.EVENT_E_SGEH_SUCCESS_1MIN\nEND\n\ncommit\n\n\n  \n  CREATE VIEW dc.EVENT_E_SGEH_SUCCESS_1MIN AS\n      SELECT \n                            IMSI_MCC\n                                                    ,TEST_COL\n                                FROM dc.EVENT_E_SGEH_SUCCESS_1MIN_01\n    WHERE VALID=1\n          UNION ALL\n          SELECT \n                            IMSI_MCC\n                                                    ,TEST_COL\n                                FROM dc.EVENT_E_SGEH_SUCCESS_1MIN_02\n    WHERE VALID=1\n          UNION ALL\n          SELECT \n                            IMSI_MCC\n                                                    ,TEST_COL\n                                FROM dc.EVENT_E_SGEH_SUCCESS_1MIN_03\n    WHERE VALID=1\n          UNION ALL\n          SELECT \n                            IMSI_MCC\n                                                    ,TEST_COL\n                                FROM dc.EVENT_E_SGEH_SUCCESS_1MIN_04\n    WHERE VALID=1\n          UNION ALL\n          SELECT \n                            IMSI_MCC\n                                                    ,TEST_COL\n                                FROM dc.EVENT_E_SGEH_SUCCESS_1MIN_05\n    WHERE VALID=1\n          UNION ALL\n          SELECT \n                            IMSI_MCC\n                                                    ,TEST_COL\n                                FROM dc.EVENT_E_SGEH_SUCCESS_1MIN_06\n    WHERE VALID=1\n      \n\n  GRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_1MIN TO dc\n  GRANT SELECT ON dc.EVENT_E_SGEH_SUCCESS_1MIN TO dcbo\n\n  commit\n\n" ;
	    String sqlStatementForCreating1MinViewForDcPublic = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_SUCCESS_1MIN' AND vcreator='dcpublic') > 0\nBEGIN\n  DROP VIEW dcpublic.EVENT_E_SGEH_SUCCESS_1MIN\nEND\n\ncommit\n\n\n  \n                                \n      CREATE VIEW dcpublic.EVENT_E_SGEH_SUCCESS_1MIN AS \n    SELECT\n                          IMSI_MCC\n                                          ,TEST_COL\n                                  ,VALID\n              FROM dc.EVENT_E_SGEH_SUCCESS_1MIN\n\n  GRANT SELECT on dcpublic.EVENT_E_SGEH_SUCCESS_1MIN TO dcpublic\n\ncommit\n\n" ;
	    Statement stmt = dwhrepConnection.createStatement();
	    stmt.executeUpdate("delete from DWHPartition");
	    // Add RAW_LEV2 to DHWType
	    stmt
	        .executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_SUCCESS','1MIN', 'EVENT_E_SGEH_SUCCESS:1MIN'"
	            + ",-1,18,'ENABLED','PARTITIONED','dc','createeventscalcview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
	            + "'EVENT_E_SGEH_SUCCESS_1MIN','DATE_ID'," + "'createpubliceventscalcview.vm','sgeh_raw_lev2')");
	    insertIntoDwhPartition("EVENT_E_SGEH_SUCCESS_1MIN", "EVENT_E_SGEH_SUCCESS:1MIN", stmt);

	    stmt
	        .executeUpdate("insert into DWHColumn values ('EVENT_E_SGEH_SUCCESS:1MIN','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
	    stmt
	        .executeUpdate("insert into DWHColumn values ('EVENT_E_SGEH_SUCCESS:1MIN','TEST_COL',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
	    stmt
	        .executeUpdate("insert into DWHColumn values ('EVENT_E_SGEH_SUCCESS:1MIN','VALID',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
	    stmt.close();
	    // Let dwhType have RAW_LEV2 storage ID instead of RAW
	    dwhType = new Dwhtype(dwhrep, "EVENT_E_SGEH_SUCCESS:1MIN");
	    // SQL statements, sqlStatementForCreatingDcViewWithRawLev2TablesFirst and
	    // sqlStatementForCreatingDcpublicView should match the generated sql
	    // statement from createview.vm and createpublicview.vm
	    RockFactory mockDwhdb = createMockConnectionAndStatementForDb("dwhdbConn", "dwhdbS", "DWHDB123",
	        sqlStatementForCreating1MinViewForDc);
	    RockFactory mockDBADwhdb = createMockConnectionAndStatementForDb("DBADwhdbC", "DBADwhdbS", "DBADwhdb123",
	        sqlStatementForCreating1MinViewForDcPublic);
	    new CreateViewsAction(mockDBADwhdb, mockDwhdb, dwhrep, dwhType, clog,techPackType);
	  }
	  
	  


	  /****************************************************************************************************/
	  /************************************* PRIVATE METHODS **********************************************/
	  /****************************************************************************************************/
	  private static void setupDwhRep() throws SQLException, RockException {
	    dwhrep = new RockFactory(DWHREP_URL, "SA", "", TESTDB_DRIVER, "dwhrepConnection", true);

	    dwhrepConnection = dwhrep.getConnection();

	    Statement stmt1 = dwhrepConnection.createStatement();
	    stmt1.execute("CREATE SCHEMA dwhrep AUTHORIZATION DBA");
	    stmt1.close();

	    createDwhTypeTable(dwhrepConnection);
	    createDwhColumnTable(dwhrepConnection);
	    createDwhPartitionTable(dwhrepConnection, rawViewName, rawStorageId);
	    createDwhTechPacks(dwhrepConnection);
	    createVersioning(dwhrepConnection);
	  }

	  private static void createDwhTypeTable(Connection connection) throws SQLException {
	    Statement stmt = connection.createStatement();
	    /*stmt
	        .execute("CREATE TABLE DWHType (TECHPACK_NAME varchar(12), TYPENAME  varchar(12), TABLELEVEL  varchar(12), STORAGEID varchar(12),"
	            + "PARTITIONSIZE numeric, PARTITIONCOUNT numeric, STATUS  varchar(12), TYPE  varchar(12), OWNER varchar(12), VIEWTEMPLATE varchar(12),"
	            + "CREATETEMPLATE  varchar(12), NEXTPARTITIONTIME timestamp, BASETABLENAME varchar(12), DATADATECOLUMN  varchar(12), PUBLICVIEWTEMPLATE varchar(12),"
	            + "PARTITIONPLAN varchar(12))");*/
	    stmt
	    .execute("CREATE TABLE DWHType (TECHPACK_NAME varchar(30), TYPENAME  varchar(255), TABLELEVEL  varchar(50), STORAGEID varchar(255),"
	        + "PARTITIONSIZE numeric, PARTITIONCOUNT numeric, STATUS  varchar(50), TYPE  varchar(50), OWNER varchar(50), VIEWTEMPLATE varchar(255),"
	        + "CREATETEMPLATE  varchar(255), NEXTPARTITIONTIME timestamp, BASETABLENAME varchar(125), DATADATECOLUMN  varchar(128), PUBLICVIEWTEMPLATE varchar(255),"
	        + "PARTITIONPLAN varchar(128))");
	    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_SUCCESS','RAW', '" + rawStorageId
	        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
	        + "'EVENT_E_SGEH_SUCCESS_RAW','DATE_ID'," + "'createpublicview.vm','sgeh_raw')");
	    stmt.close();
	  }

	  private static void createDwhColumnTable(Connection connection) throws SQLException {
	    Statement stmt = connection.createStatement();
	    stmt
	        .execute("CREATE TABLE DWHColumn (STORAGEID varchar(255), DATANAME varchar(128), COLNUMBER numeric, DATATYPE varchar(50), DATASIZE int,"
	            + "DATASCALE int, UNIQUEVALUE numeric, NULLABLE int, INDEXES varchar(20), UNIQUEKEY int, STATUS varchar(12), INCLUDESQL int)");
	    stmt.executeUpdate("insert into DWHColumn values ('" + rawStorageId
	        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
	    stmt.close();
	  }

	  private static void createDwhPartitionTable(Connection connection, String viewName, String storageId)
	      throws SQLException {
	    Statement stmt = connection.createStatement();
	    stmt
	        .execute("CREATE TABLE DWHPartition (STORAGEID varchar(255),TABLENAME varchar(255),STARTTIME timestamp,ENDTIME timestamp,STATUS varchar(10),LOADORDER integer)");
	    insertIntoDwhPartition(viewName, storageId, stmt);
	    stmt.close();
	  }
	  
	  private static void createDwhTechPacks(Connection connection)throws SQLException{
		
		  	Statement stmt = connection.createStatement();
		    stmt.execute("CREATE TABLE Dwhtechpacks ( TECHPACK_NAME VARCHAR(31)  ,VERSIONID VARCHAR(31) ,CREATIONDATE TIMESTAMP )");
		    stmt.executeUpdate("INSERT INTO Dwhtechpacks VALUES( 'EVENT_E_SGEH' ,'EVENT_E_SGEH:((14))'  ,'2000-01-01 00:00:00.0' )");

	  }
	  
	  private static void createVersioning(Connection connection)throws SQLException{
			
		  	Statement stmt = connection.createStatement();
		    stmt.execute("CREATE TABLE Versioning ( VERSIONID VARCHAR(31)  ,DESCRIPTION VARCHAR(31) ,STATUS BIGINT  ,TECHPACK_NAME VARCHAR(31) ,TECHPACK_VERSION VARCHAR(31) ,TECHPACK_TYPE VARCHAR(31) ,PRODUCT_NUMBER VARCHAR(31) ,LOCKEDBY VARCHAR(31) ,LOCKDATE TIMESTAMP  ,BASEDEFINITION VARCHAR(31) ,BASEVERSION VARCHAR(31) ,INSTALLDESCRIPTION VARCHAR(31) ,UNIVERSENAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,ENIQ_LEVEL VARCHAR(31) ,LICENSENAME VARCHAR(31))");
		    stmt.executeUpdate("INSERT INTO Versioning VALUES( 'EVENT_E_SGEH:((14))'  ,'testDESCRIPTION'  ,1  ,'EVENT_E_SGEH'  ,'testTECHPACK_VERSION'  ,'ENIQ_EVENT'  ,'COA 252 219'  ,'test'  ,'2000-01-01 00:00:00.0'  ,'testBASEDEFINITION'  ,'testBASEVERSION'  ,'testINSTALLDESCRIPTION'  ,'testUNIVERSENAME'  ,'testUNIVERSEEXTENSION'  ,'testENIQ_LEVEL'  ,'testLICENSENAME' )");
	  }

	  private static void insertIntoDwhPartition(String viewName, String storageId, Statement stmt) throws SQLException {
	    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_01',"
	        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', '0')");
	    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_02',"
	        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', '0')");
	    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_03',"
	        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', '0')");
	    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_04',"
	        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', '0')");
	    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_05',"
	        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', '0')");
	    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_06',"
	        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', '0')");
	    // This should not be added to the view as this is a testStorageId
	    stmt.executeUpdate("insert into DWHPartition values ('" + testStorageId + "','" + testViewName + "_06',"
	        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', '0')");
	  }

	  private static void setupProperties() throws Exception {
	    Properties props = new Properties();
	    props.setProperty("DWHManager.viewCreateRetries", "2");
	    props.setProperty("DWHManager.viewCreateRetryPeriod", "1");
	    props.setProperty("DWHManager.viewCreateRetryRandom", "1");

	    StaticProperties.giveProperties(props);
	  }
	
	
	/*****************************Test cases for STATS********************************************
	 *************************Please note that any change in the createView.vm and***********************
	 *************************createPublicView.vm will cause the test cases to fail.. 
	 ***************************change the Strings accordinlgy...************************
	 **************************************************************************************/
	
	private final Mockery context = new JUnit4Mockery() {
	    {
	      setImposteriser(ClassImposteriser.INSTANCE);
	    }
	};
	 
	
	public void setUpForStats() throws Exception {
		mockLog = context.mock(Logger.class, "mockLog");
		try {
			context.checking(new Expectations() {
				{			
					allowing(mockLog).getName();
					will(returnValue("testLogger"));
				}
			});
		}catch(Exception e){
			fail(" Exception comes: " + e.toString());
		}
		
		rock = new RockFactory(DWHREP_URL, "SA", "", TESTDB_DRIVER, "test", true);
		DatabaseTestUtils.loadSetup(rock, "removeDWH");
		if(!started){
		//	setupOnce();
			started = true ;
		}
		
	    // Setting up static properties
    	final Properties p = new Properties();        
    	p.put("dwhm.debug", "true");     
    	final String currentLocation = System.getProperty("user.home");
    	if(!currentLocation.endsWith("ant_common")){        	
    		p.put("dwhm.templatePath", ".\\jar\\"); // Gets tests running on laptop        
    	}
    	StaticProperties.giveProperties(p);
    	dwhtypeInstance = new Dwhtype(rock,storageIde);

	}
	
	
	public void tearDownForStats() throws Exception {
		// Destruction work
		final Statement stmt = rock.getConnection().createStatement();        
    	stmt.executeUpdate("SHUTDOWN");        
    	stmt.close();        
    	rock.getConnection().close();
		rock = null;
		dwhtypeInstance = null;
	    mockLog = null;
	}
	
//    @Test
//	public void testCreateViewFunctionality(){
//		
//		try{
//			setUpForStats();
//				
//			final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//			final Connection c = createNiceMock(Connection.class);
//			final Statement s = createMock(Statement.class);
//			final ResultSet resultSetMock = createMock(ResultSet.class);
//			mockDwhType = context.mock(Dwhtype.class, "mockDwhType");
//			mockDwhTPType = context.mock(Dwhtechpacks.class, "mockDwhTPType");
//			final String expectedElementDeletes = getExpectedStringForAdjustViews();
//			TechPackType techPackType = TechPackType.STATS;
//			// setup the expected calls
//			// dB connection
//			expect(mDwhdb.getConnection()).andReturn(c).anyTimes();
//			// SQL
//			expect(c.createStatement()).andReturn(s).anyTimes();
//			expect(s.executeUpdate(expectedElementDeletes)).andReturn(0).times(1);
//			expect(s.executeUpdate(getExpectedStringForAdjustPublicViews())).andReturn(0).times(1);
//			
//			
//			s.close();
//			expectLastCall().atLeastOnce();
//			
//			replay(s);
//			replay(c);
//			replay(resultSetMock);
//			replay(mDwhdb);			
//			
//			// Calling constructor of CreateViewsAction class which internally calls adjustViews(Dwhtype) and adjustPublicViews(Dwhtype)
//			new CreateViewsAction(mDwhdb, mDwhdb, rock, dwhtypeInstance, mockLog,techPackType);
//			
//			verify(s);
//			
//		}catch(Exception e){
//			//fail the test case
//			fail("Fail due to exception: " + e.getMessage());
//		}
//		assertEquals(0, 0);
//
//	}	
	
	

	/**
	 * Method to Return expected SQL Query that should come in adjustViews function for executeUpdate method
	 * This string is hardcoded.
	 * If test database inside bin/setupSQL/removeDWH changes, then this string needs to be updated
	 * @return expected SQL Query that should come in adjustViews function for executeUpdate method
	 */

	  private String getExpectedStringForAdjustViews(){
		  
		  /*
		   * NOTE: The expected sql isn't actually executable - for the test, there are no records 
		   * returned by CreateViewsAction.getMeasurementKeys which leads to sql that includes:
		   * 
		   * CREATE VIEW dc.DC_E_MGW_ATMPORT_DELTA AS SELECT DISTINCT ,c.DATE_ID
		   * 
		   * The vm template that creates the sql would place any returned columns 
		   * after "DISTINCT" and before ",c.DATE_ID". As no columns are returned, we're left
		   * with SQL that would generate a syntax error IF it was executed.
		   * 
		   */
			String expected = null ;
			expected = "\n\nIF (SELECT count(*) FROM SYSVIEWS WHERE viewname='DC_E_MGW_ATMPORT_COUNT' ";
			expected += "AND vcreator='dc') > 0\n\nBEGIN\n\n  DROP VIEW dc.DC_E_MGW_ATMPORT_COUNT\n\nEND";
			expected += "\n\n\n\nCOMMIT\n\n\n\n\n\n\n\nCREATE VIEW dc.DC_E_MGW_ATMPORT_COUNT AS\n\n\n";
			expected += "SELECT * FROM dc.DC_E_MGW_ATMPORT_COUNT_01\n\n\nUNION ALL\n\n\n\n\nSELECT * ";
			expected += "FROM dc.DC_E_MGW_ATMPORT_COUNT_02\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM ";
			expected += "dc.DC_E_MGW_ATMPORT_COUNT_03\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM ";
			expected += "dc.DC_E_MGW_ATMPORT_COUNT_04\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM ";
			expected += "dc.DC_E_MGW_ATMPORT_COUNT_05\n\n\nUNION ALL\n\n\n\n\nSELECT * FROM ";
			expected += "dc.DC_E_MGW_ATMPORT_COUNT_06\n\n\n\n\n\n\nGRANT SELECT ON dc.DC_E_MGW_ATMPORT_COUNT ";
			expected += "TO dc\n\nGRANT SELECT ON dc.DC_E_MGW_ATMPORT_COUNT TO dcbo\n\n\n\nCOMMIT";
			expected += "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nIF (SELECT count(*) FROM SYSVIEWS WHERE ";
			expected += "viewname='DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES' AND vcreator='dc') > 0\n\n";
			expected += "BEGIN\n\n  DROP VIEW dc.DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES\n\nEND\n\n\n\n";
			expected += "COMMIT\n\n\n\nCREATE VIEW dc.DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES AS\n\n\n\n";
			expected += "SELECT DISTINCT date_id FROM dc.DC_E_MGW_ATMPORT_COUNT_01\n\n\nUNION ALL\n\n\n\n\n";
			expected += "SELECT DISTINCT date_id FROM dc.DC_E_MGW_ATMPORT_COUNT_02\n\n\nUNION ALL\n\n\n\n\n";
			expected += "SELECT DISTINCT date_id FROM dc.DC_E_MGW_ATMPORT_COUNT_03\n\n\nUNION ALL\n\n\n\n\n";
			expected += "SELECT DISTINCT date_id FROM dc.DC_E_MGW_ATMPORT_COUNT_04\n\n\nUNION ALL\n\n\n\n\n";
			expected += "SELECT DISTINCT date_id FROM dc.DC_E_MGW_ATMPORT_COUNT_05\n\n\nUNION ALL\n\n\n\n\n";
			expected += "SELECT DISTINCT date_id FROM dc.DC_E_MGW_ATMPORT_COUNT_06\n\n\n\n\n\n\nGRANT SELECT ";
			expected += "ON dc.DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES TO dc\n\nGRANT SELECT ON ";
			expected += "dc.DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES TO dcbo\n\nGRANT SELECT ON ";
			expected += "dc.DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES TO dcpublic\n\n\n\nCOMMIT\n\n\n\n ";
			expected += "\n\n\n\n\n\n\n\n\n\nIF (SELECT count(*) FROM SYSVIEWS WHERE viewname=";
			expected += "'DC_E_MGW_ATMPORT_DELTA' AND vcreator='dc') > 0\n\nBEGIN\n\n  DROP VIEW ";
			expected += "dc.DC_E_MGW_ATMPORT_DELTA\n\nEND\n\n\n\nCOMMIT\n\n\n\n\n\n\n\nCREATE VIEW ";
			expected += "dc.DC_E_MGW_ATMPORT_DELTA\n\nAS\n\nSELECT DISTINCT\n\n\n\n\n\n\n,c.DATE_ID\n\n";
			expected += ",c.YEAR_ID\n\n,c.MONTH_ID\n\n,c.DAY_ID\n\n,c.HOUR_ID\n\n,c.DATETIME_ID\n\n,c.MIN_ID";
			expected += "\n\n,c.TIMELEVEL\n\n,c.SESSION_ID\n\n,c.BATCH_ID\n\n,c.PERIOD_DURATION\n\n\n\n\n\n";
			expected += ",if c.ROWSTATUS like '%TBA_%' or c.ROWSTATUS like '%PBA_%' then c.ROWSTATUS   else ";
			expected += "'AGGREGATED'  endif AS ROWSTATUS\n\n,c.DC_RELEASE\n\n,c.DC_SOURCE\n\n,c.DC_TIMEZONE";
			expected += "\n\n,c.UTC_DATETIME_ID\n\n\n\n\n\nFROM  dc.DC_E_MGW_ATMPORT_RAW c \n\nJOIN (SELECT ";
			expected += "date_id, COUNT(*) raw_count FROM (SELECT date_id FROM dc.DC_E_MGW_ATMPORT_RAW_DISTINCT_DATES ";
			expected += "UNION ALL SELECT date_id FROM dc.DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES) dates GROUP BY ";
			expected += "date_id HAVING COUNT(*)=1) t1 ON (c.date_id = t1.date_id)\n\n	, dc.DC_E_MGW_ATMPORT_RAW ";
			expected += "p \n\nJOIN (SELECT date_id, COUNT(*) raw_count FROM (SELECT date_id FROM ";
			expected += "dc.DC_E_MGW_ATMPORT_RAW_DISTINCT_DATES UNION ALL SELECT date_id FROM ";
			expected += "dc.DC_E_MGW_ATMPORT_COUNT_DISTINCT_DATES) dates GROUP BY date_id HAVING COUNT(*)=1) ";
			expected += "t2 ON (p.date_id = t2.date_id OR p.date_id = DATEADD(day, -1, t2.date_id))  \n\nWHERE";
			expected += "\n\nc.ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED')\n\nAND p.ROWSTATUS NOT IN ('DUPLICATE'";
			expected += ",'SUSPECTED') \n\n\n\nAND p.DATETIME_ID = DATEADD(Minute, - c.PERIOD_DURATION, ";
			expected += "c.DATETIME_ID)\n\n\n\n\n\n\nUNION ALL\n\nSELECT \n\n\n\n\n\n\n,DATE_ID\n\n,YEAR_ID\n\n,";
			expected += "MONTH_ID\n\n,DAY_ID\n\n,HOUR_ID\n\n,DATETIME_ID\n\n,MIN_ID\n\n,TIMELEVEL\n\n,SESSION_ID";
			expected += "\n\n,BATCH_ID\n\n,PERIOD_DURATION\n\n,ROWSTATUS\n\n,DC_RELEASE\n\n,DC_SOURCE\n\n,";
			expected += "DC_TIMEZONE\n\n,UTC_DATETIME_ID\n\n\n\n\n\nFROM dc.DC_E_MGW_ATMPORT_COUNT\n\n\n\n\n\n";
			expected += "GRANT SELECT ON dc.DC_E_MGW_ATMPORT_DELTA TO dc\n\nGRANT SELECT ON dc.DC_E_MGW_ATMPORT_DELTA TO ";
			expected += "dcbo\n\nGRANT SELECT ON dc.DC_E_MGW_ATMPORT_DELTA TO dcpublic\n\n\n\nCOMMIT\n\n \n\n\n \n";
			return expected ;
		}
	
	/**
	 * Method to Return expected SQL Query that should come in adjustPublicViews function for executeUpdate method
	 * This string is hardcoded.
	 * If test database inside bin/setupSQL/removeDWH changes, then this string needs to be updated
	 * @return expected SQL Query that should come in adjustPublicViews function for executeUpdate method
	 */
  
		private String getExpectedStringForAdjustPublicViews(){
			String expected = null ;
			expected = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='DC_E_MGW_ATMPORT_COUNT' AND vcreator='dcpublic') > 0" + "\n";
			expected += "BEGIN" + "\n";
			expected += "  DROP VIEW dcpublic.DC_E_MGW_ATMPORT_COUNT" + "\n";
			expected += "END" + "\n" ;
			expected += "\n" ;
			expected += "commit" + "\n";
			expected += "\n" + "\n" + "\n" + "\n";
			expected += "CREATE VIEW dcpublic.DC_E_MGW_ATMPORT_COUNT AS " + "\n" ;
			expected += "SELECT" + "\n";
			expected += "OSS_ID" + "\n";
			expected += ",SN" + "\n";
			expected += ",NEUN" + "\n";
			expected += ",NEDN" + "\n";
			expected += ",NESW" + "\n";
			expected += ",MGW" + "\n";
			expected += ",MOID" + "\n";
			expected += ",TransportNetwork" + "\n";
			expected += ",AtmPort" + "\n";
			expected += ",userLabel" + "\n";
			expected += ",DATETIME_ID" + "\n";
			expected += ",PERIOD_DURATION" + "\n";
			expected += ",ROWSTATUS" + "\n";
			expected += ",DC_RELEASE" + "\n";
			expected += ",DC_SOURCE" + "\n";
			expected += ",DC_TIMEZONE" + "\n" ;
			expected += ",DC_SUSPECTFLAG" +"\n" ;
			expected += ",UTC_DATETIME_ID" + "\n" ;
			expected += ",pmTransmittedAtmCells" + "\n";
			expected += ",pmReceivedAtmCells" + "\n" ;
			expected += ",pmSecondsWithUnexp" + "\n";
			expected += "FROM dc.DC_E_MGW_ATMPORT_COUNT" + "\n" ;
			expected += "WHERE ROWSTATUS!='DUPLICATE' AND ROWSTATUS!='SUSPECTED'" + "\n" ;
			expected += "\n" ;
			expected += "GRANT SELECT on dcpublic.DC_E_MGW_ATMPORT_COUNT TO dcpublic" + "\n" ;
			expected += "\n" ;
			expected += "commit" + "\n";
			expected += "\n" ;
			return expected ;
		}	
}
	
