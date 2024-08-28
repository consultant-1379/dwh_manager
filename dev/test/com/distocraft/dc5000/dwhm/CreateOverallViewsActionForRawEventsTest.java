package com.distocraft.dc5000.dwhm;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;

public class CreateOverallViewsActionForRawEventsTest extends CreateViewsActionBaseUnitTestX {

  private static RockFactory dwhrep = null;

  static Connection dwhrepConnection = null;

  private final Logger clog = Logger.getLogger("OverallCreateViewsActionForEventsTest");

  private final static String TESTDB_DRIVER = "org.hsqldb.jdbcDriver";

  private final static String DWHREP_URL = "jdbc:hsqldb:mem:dwhrep";

  private static String rawSuccessStorageId = "EVENT_E_SGEH_SUC:RAW";

  private static String rawSuccessViewName = "EVENT_E_SGEH_SUC_RAW";

  private static String rawErroneousStorageId = "EVENT_E_SGEH_ERR:RAW";

  private static String rawErroneousViewName = "EVENT_E_SGEH_ERR_RAW";

  private static String rawErroneous123StorageId = "EVENT_E_SGEH_ERR123:RAW";

  private static String rawErroneous123ViewName = "EVENT_E_SGEH_ERR123_RAW";

  private static String testStorageId = "EVENT_E_SGEH_SUC:TEST";

  private static String testViewName = "EVENT_E_SGEH_SUC_TEST";

  private final String sqlStatementForCreatingDcView = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_RAW' AND vcreator='dc') > 0\nBEGIN\n  "
      + "DROP VIEW dc.EVENT_E_SGEH_RAW\nEND\n\ncommit\n\n\nCREATE VIEW dc.EVENT_E_SGEH_RAW AS\nSELECT *,1 as VIEW_NUMBER FROM dc.EVENT_E_SGEH_SUC_RAW\nUNION ALL"
      + "\nSELECT *,2 as VIEW_NUMBER FROM dc.EVENT_E_SGEH_ERR_RAW\n\nGRANT SELECT ON dc.EVENT_E_SGEH_RAW TO dc\nGRANT "
      + "SELECT ON dc.EVENT_E_SGEH_RAW TO dcbo\n\ncommit\n\n";

  private final String sqlStatementForCreatingTimeRangeDcView = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_RAW_TIMERANGE' AND vcreator='dc') > 0\n"
      + "BEGIN\n  DROP VIEW dc.EVENT_E_SGEH_RAW_TIMERANGE\nEND\n\ncommit\n\n\nCREATE VIEW dc.EVENT_E_SGEH_RAW_TIMERANGE AS\nSELECT min(datetime_id) as MIN_DATE, "
      + "max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUC_RAW_01' AS TABLENAME FROM dc.EVENT_E_SGEH_SUC_RAW_01\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, "
      + "max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUC_RAW_02' AS TABLENAME FROM dc.EVENT_E_SGEH_SUC_RAW_02\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, "
      + "max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUC_RAW_03' AS TABLENAME FROM dc.EVENT_E_SGEH_SUC_RAW_03\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, "
      + "max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUC_RAW_04' AS TABLENAME FROM dc.EVENT_E_SGEH_SUC_RAW_04\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, "
      + "max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUC_RAW_05' AS TABLENAME FROM dc.EVENT_E_SGEH_SUC_RAW_05\nUNION ALL\nSELECT min(datetime_id) as MIN_DATE, "
      + "max(datetime_id) as MAX_DATE, 'EVENT_E_SGEH_SUC_RAW_06' AS TABLENAME FROM dc.EVENT_E_SGEH_SUC_RAW_06\n\nGRANT SELECT ON dc.EVENT_E_SGEH_RAW_TIMERANGE TO "
      + "dc\nGRANT SELECT ON dc.EVENT_E_SGEH_RAW_TIMERANGE TO dcbo\n\ncommit\n\n";

  private final String sqlStatementForCreatingDcpublicView = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_RAW' AND vcreator='dcpublic') "
      + "> 0\nBEGIN\n  DROP VIEW dcpublic.EVENT_E_SGEH_RAW\nEND\n\ncommit\n\n\n\n\n\nCREATE VIEW dcpublic.EVENT_E_SGEH_RAW AS \nSELECT\nIMSI_MCC,TEST,1 as VIEW_NUMBER"
      + "\nFROM dc.EVENT_E_SGEH_SUC_RAW\nUNION ALL\nSELECT\nIMSI_MCC,TEST,2 as VIEW_NUMBER\nFROM dc.EVENT_E_SGEH_ERR_RAW\n\n"
      + "GRANT SELECT on dcpublic.EVENT_E_SGEH_RAW TO dcpublic\n\ncommit\n";

  private static Vector<Dwhtype> dwhTypes = new Vector<Dwhtype>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    StaticProperties.giveProperties(new Properties());
    setupDwhRep();
    dwhTypes.add(new Dwhtype(dwhrep, rawSuccessStorageId));
    dwhTypes.add(new Dwhtype(dwhrep, rawErroneousStorageId));
    dwhTypes.add(new Dwhtype(dwhrep, rawErroneous123StorageId));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    /* Cleaning up after test */
    Statement stmt = dwhrepConnection.createStatement();
    stmt.execute("DROP TABLE DWHPartition");
    stmt.execute("DROP TABLE DWHColumn");
    stmt.execute("DROP TABLE DWHType");
    stmt.execute("DROP SCHEMA dwhrep");
    dwhrepConnection = null;
    dwhrep = null;
  }

  @Test
  public void checkThatCorrectSQLStatementIsExecutedForDcAndDcPublicView() throws Exception {
    // SQL statements, sqlStatementForCreatingDcpublicView and
    // sqlStatementForCreatingDcView should match the generated sql statement
    // from createview.vm
    RockFactory mockDwhdb = createMockConnectionForDbWithTwoSqlStatements("createOveralldwhdbConnection",
        "createOveralldwhdbStatement", "createOverallDWHDB", sqlStatementForCreatingDcView,
        sqlStatementForCreatingTimeRangeDcView);
    RockFactory mockDBADwhdb = createMockConnectionAndStatementForDb("createOverallDBADwhdbConnection",
        "createOverallDBADwhdbStatement", "createOverallDBADwhdb", sqlStatementForCreatingDcpublicView);

    List<String> listOfCreatedViews = new ArrayList<String>();
    listOfCreatedViews.add(rawSuccessViewName);
    listOfCreatedViews.add(rawErroneousViewName);
    listOfCreatedViews.add(rawErroneous123ViewName);
    new CreateOverallViewsActionForRawEvents(mockDBADwhdb, mockDwhdb, dwhrep, dwhTypes, clog, listOfCreatedViews);
  }

  /****************************************************************************************************/
  /************************************* PRIVATE METHODS **********************************************/
  /****************************************************************************************************/

  private static void setupDwhRep() throws SQLException, RockException {
    dwhrep = new RockFactory(DWHREP_URL, "SA", "", TESTDB_DRIVER, "dwhrepConnection", true);

    dwhrepConnection = dwhrep.getConnection();

    Statement stmt1 = dwhrepConnection.createStatement();
    stmt1.execute("CREATE SCHEMA dwhrep AUTHORIZATION DBA");

    createDwhTypeTable(dwhrepConnection);
    createDwhColumnTable(dwhrepConnection);
    createDwhPartitionTable(dwhrepConnection, rawSuccessViewName, rawSuccessStorageId);
    insertIntoDwhPartition(rawErroneous123ViewName, rawErroneous123StorageId, stmt1);
    insertIntoDwhPartition(rawErroneousViewName, rawErroneousStorageId, stmt1);
    stmt1.close();
  }

  private static void createDwhTypeTable(Connection connection) throws SQLException {
    Statement stmt = connection.createStatement();
    stmt
        .execute("CREATE TABLE DWHType (TECHPACK_NAME varchar(30), TYPENAME  varchar(255), TABLELEVEL  varchar(50), STORAGEID varchar(255),"
            + "PARTITIONSIZE numeric, PARTITIONCOUNT numeric, STATUS  varchar(50), TYPE  varchar(50), OWNER varchar(50), VIEWTEMPLATE varchar(255),"
            + "CREATETEMPLATE  varchar(255), NEXTPARTITIONTIME timestamp, BASETABLENAME varchar(125), DATADATECOLUMN  varchar(128), PUBLICVIEWTEMPLATE varchar(255),"
            + "PARTITIONPLAN varchar(128))");
    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_SUC','RAW', '"
        + rawSuccessStorageId
        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
        + "'EVENT_E_SGEH_SUC_RAW','DATE_ID'," + "'createpublicview.vm','sgeh_raw')");
    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_SUC','RAW', '"
        + rawErroneous123StorageId
        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
        + "'EVENT_E_SGEH_SUC_RAW','DATE_ID'," + "'createpublicview.vm','sgeh_raw')");
    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_SUC','RAW', '"
        + rawErroneousStorageId
        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
        + "'EVENT_E_SGEH_SUC_RAW','DATE_ID'," + "'createpublicview.vm','sgeh_raw')");
    stmt.close();
  }

  private static void createDwhColumnTable(Connection connection) throws SQLException {
    Statement stmt = connection.createStatement();
    stmt
        .execute("CREATE TABLE DWHColumn (STORAGEID varchar(255), DATANAME varchar(128), COLNUMBER numeric, DATATYPE varchar(50), DATASIZE int,"
            + "DATASCALE int, UNIQUEVALUE numeric, NULLABLE int, INDEXES varchar(20), UNIQUEKEY int, STATUS varchar(12), INCLUDESQL int)");
    stmt.executeUpdate("insert into DWHColumn values ('" + rawSuccessStorageId
        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + rawSuccessStorageId
        + "','TEST',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + rawErroneousStorageId
        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + rawErroneousStorageId
        + "','TEST',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + rawErroneous123StorageId
        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + rawErroneous123StorageId
        + "','TEST',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.close();
  }

  private static void createDwhPartitionTable(Connection connection, String viewName, String storageId)
      throws SQLException {
    Statement stmt = connection.createStatement();
    stmt
        .execute("CREATE TABLE DWHPartition (STORAGEID varchar(255),TABLENAME varchar(255),STARTTIME timestamp,ENDTIME timestamp,STATUS varchar(10),LOADORDER int)");
    insertIntoDwhPartition(viewName, storageId, stmt);
    stmt.close();
  }

  private static void insertIntoDwhPartition(String viewName, String storageId, Statement stmt) throws SQLException {
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_01',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', 0)");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_02',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', 0)");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_03',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', 0)");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_04',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', 0)");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_05',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', 0)");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_06',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', 0)");
    // This should not be added to the view as this is a testStorageId
    stmt.executeUpdate("insert into DWHPartition values ('" + testStorageId + "','" + testViewName + "_06',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE', 0)");
  }
}
