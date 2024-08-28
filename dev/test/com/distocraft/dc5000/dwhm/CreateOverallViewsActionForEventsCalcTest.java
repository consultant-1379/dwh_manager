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

public class CreateOverallViewsActionForEventsCalcTest extends CreateViewsActionBaseUnitTestX {

  private static RockFactory dwhrep = null;

  static Connection dwhrepConnection = null;

  private final Logger clog = Logger.getLogger("OverallCreateViewsActionForEventsTest");

  private final static String TESTDB_DRIVER = "org.hsqldb.jdbcDriver";

  private final static String DWHREP_URL = "jdbc:hsqldb:mem:dwhrep";

  private static String fifteenMinSuccessStorageId = "EVENT_E_SGEH_SUC:15MIN";

  private static String fifteenMinSuccessViewName = "EVENT_E_SGEH_SUC_15MIN";

  private static String fifteenMinErroneousStorageId = "EVENT_E_SGEH_ERR:15MIN";

  private static String fifteenMinErroneousViewName = "EVENT_E_SGEH_ERR_15MIN";

  private static String fifteenMinErroneous123StorageId = "EVENT_E_SGEH_ERR123:15MIN";

  private static String fifteenMinErroneous123ViewName = "EVENT_E_SGEH_ERR123_15MIN";

  private static String testStorageId = "EVENT_E_SGEH_SUC:TEST";

  private static String testViewName = "EVENT_E_SGEH_SUC_TEST";

  private final String sqlStatementForCreatingDcView = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_15MIN' AND vcreator='dc') > 0"
      + "\nBEGIN\n  DROP VIEW dc.EVENT_E_SGEH_15MIN\nEND\n\ncommit\n\n\n\n  \n"
      + "                                                                                                  \n\nCREATE VIEW dc.EVENT_E_SGEH_15MIN "
      + "AS(\nSELECT\nIMSI_MCC,(SUM(NO_OF_SUBSCRIBERS)) as NO_OF_SUBSCRIBERS,TEST\nFROM(\n\n\n  SELECT \n                    IMSI_MCC\n"
      + "                                      ,NO_OF_SUBSCRIBERS\n                              ,TEST\n              "
      + "FROM dc.EVENT_E_SGEH_ERR_15MIN\n      UNION ALL\n    SELECT \n                    IMSI_MCC\n                                      "
      + ",NO_OF_SUBSCRIBERS\n                              ,TEST\n              FROM dc.EVENT_E_SGEH_SUC_15MIN\n  ) AS "
      + "EVENT_E_SGEH_15MIN\nGROUP BY\n\n\n            IMSI_MCC\n                              ,TEST\n        ) "
      + "GRANT SELECT ON dc.EVENT_E_SGEH_15MIN TO dc\nGRANT SELECT ON dc.EVENT_E_SGEH_15MIN TO dcbo\n\ncommit\n\n";

  private final String sqlStatementForCreatingDcpublicView = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_15MIN' AND "
      + "vcreator='dcpublic') > 0\nBEGIN\n  DROP VIEW dcpublic.EVENT_E_SGEH_15MIN\nEND\n\ncommit\n\n\n            \n  CREATE VIEW "
      + "dcpublic.EVENT_E_SGEH_15MIN AS \n  SELECT\n                    IMSI_MCC\n                                      ,NO_OF_SUBSCRIBERS\n"
      + "                              ,TEST\n              FROM dc.EVENT_E_SGEH_15MIN\n\n  GRANT SELECT on dcpublic.EVENT_E_SGEH_15MIN TO "
      + "dcpublic\n\ncommit";

  private static Vector<Dwhtype> dwhTypes = new Vector<Dwhtype>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    StaticProperties.giveProperties(new Properties());
    setupDwhRep();
    dwhTypes.add(new Dwhtype(dwhrep, fifteenMinSuccessStorageId));
    dwhTypes.add(new Dwhtype(dwhrep, fifteenMinErroneousStorageId));
    dwhTypes.add(new Dwhtype(dwhrep, fifteenMinErroneous123StorageId));
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
    RockFactory mockDwhdb = createMockConnectionAndStatementForDb("createOveralldwhdbConnection", "createOveralldwhdbStatement",
        "createOverallDWHDB", sqlStatementForCreatingDcView);
    RockFactory mockDBADwhdb = createMockConnectionAndStatementForDb("createOverallDBADwhdbConnection", "createOverallDBADwhdbStatement",
        "createOverallDBADwhdb", sqlStatementForCreatingDcpublicView);
    List<String> listOfCreatedViews = new ArrayList<String>();
    listOfCreatedViews.add(fifteenMinErroneousViewName);
    listOfCreatedViews.add(fifteenMinErroneous123ViewName);
    listOfCreatedViews.add(fifteenMinSuccessViewName);
    new CreateOverallViewsActionForEventsCalc(mockDBADwhdb, mockDwhdb, dwhrep, dwhTypes, clog, listOfCreatedViews);
  }

  /****************************************************************************************************/
  /************************************* PRIVATE METHODS **********************************************/
  /****************************************************************************************************/
  private static void setupDwhRep() throws SQLException, RockException {
    dwhrep = new RockFactory(DWHREP_URL, "SA", "", TESTDB_DRIVER, "dwhrepConnection", true);

    dwhrepConnection = dwhrep.getConnection();

    Statement statementForDwhRep = dwhrepConnection.createStatement();
    statementForDwhRep.execute("CREATE SCHEMA dwhrep AUTHORIZATION DBA");

    createDwhTypeTable(dwhrepConnection);
    createDwhColumnTable(dwhrepConnection);
    createDwhPartitionTable(dwhrepConnection, fifteenMinSuccessViewName, fifteenMinSuccessStorageId);
    insertIntoDwhPartition(fifteenMinErroneous123ViewName, fifteenMinErroneous123StorageId, statementForDwhRep);
    insertIntoDwhPartition(fifteenMinErroneousViewName, fifteenMinErroneousStorageId, statementForDwhRep);
    statementForDwhRep.close();
  }

  private static void createDwhTypeTable(Connection connection) throws SQLException {
    Statement stmt = connection.createStatement();
   /* stmt
        .execute("CREATE TABLE DWHType (TECHPACK_NAME varchar(12), TYPENAME  varchar(255), TABLELEVEL  varchar(12), STORAGEID varchar(255),"
            + "PARTITIONSIZE numeric, PARTITIONCOUNT numeric, STATUS  varchar(12), TYPE  varchar(12), OWNER varchar(12), VIEWTEMPLATE varchar(255),"
            + "CREATETEMPLATE  varchar(12), NEXTPARTITIONTIME timestamp, BASETABLENAME varchar(12), DATADATECOLUMN  varchar(12), PUBLICVIEWTEMPLATE varchar(12),"
            + "PARTITIONPLAN varchar(12))");*/
    stmt
    .execute("CREATE TABLE DWHType (TECHPACK_NAME varchar(30), TYPENAME  varchar(255), TABLELEVEL  varchar(50), STORAGEID varchar(255),"
        + "PARTITIONSIZE numeric, PARTITIONCOUNT numeric, STATUS  varchar(50), TYPE  varchar(50), OWNER varchar(50), VIEWTEMPLATE varchar(255),"
        + "CREATETEMPLATE  varchar(255), NEXTPARTITIONTIME timestamp, BASETABLENAME varchar(125), DATADATECOLUMN  varchar(128), PUBLICVIEWTEMPLATE varchar(255),"
        + "PARTITIONPLAN varchar(128))");
    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_SUC','15MIN', '"
        + fifteenMinSuccessStorageId
        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
        + "'EVENT_E_SGEH_SUC_15MIN','DATE_ID'," + "'createpublicview.vm','sgeh_fifteenMin')");
    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_ERR123','15MIN', '"
        + fifteenMinErroneous123StorageId
        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
        + "'EVENT_E_SGEH_ERR123_15MIN','DATE_ID'," + "'createpublicview.vm','sgeh_fifteenMin')");
    stmt.executeUpdate("INSERT INTO DWHType VALUES('EVENT_E_SGEH','EVENT_E_SGEH_ERR','15MIN', '"
        + fifteenMinErroneousStorageId
        + "',-1,18,'ENABLED','PARTITIONED','dc','createview.vm','createpartition.vm','2010-03-17 00:00:00.0',"
        + "'EVENT_E_SGEH_ERR_15MIN','DATE_ID'," + "'createpublicview.vm','sgeh_fifteenMin')");
    stmt.close();
  }

  private static void createDwhColumnTable(Connection connection) throws SQLException {
    Statement stmt = connection.createStatement();
    stmt
        .execute("CREATE TABLE DWHColumn (STORAGEID varchar(255), DATANAME varchar(128), COLNUMBER numeric, DATATYPE varchar(50), DATASIZE int,"
            + "DATASCALE int, UNIQUEVALUE numeric, NULLABLE int, INDEXES varchar(20), UNIQUEKEY int, STATUS varchar(12), INCLUDESQL int)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinSuccessStorageId
        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinSuccessStorageId
        + "','NO_OF_SUBSCRIBERS',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinSuccessStorageId
        + "','TEST',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinErroneousStorageId
        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinErroneousStorageId
        + "','NO_OF_SUBSCRIBERS',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinErroneousStorageId
        + "','TEST',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinErroneous123StorageId
        + "','IMSI_MCC',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinErroneous123StorageId
        + "','NO_OF_SUBSCRIBERS',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.executeUpdate("insert into DWHColumn values ('" + fifteenMinErroneous123StorageId
        + "','TEST',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
    stmt.close();
  }

  private static void createDwhPartitionTable(Connection connection, String viewName, String storageId)
      throws SQLException {
    Statement stmt = connection.createStatement();
    stmt
        .execute("CREATE TABLE DWHPartition (STORAGEID varchar(255),TABLENAME varchar(255),STARTTIME timestamp,ENDTIME timestamp,STATUS varchar(10))");
    insertIntoDwhPartition(viewName, storageId, stmt);
    stmt.close();
  }

  private static void insertIntoDwhPartition(String viewName, String storageId, Statement stmt) throws SQLException {
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_01',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE')");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_02',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE')");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_03',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE')");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_04',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE')");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_05',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE')");
    stmt.executeUpdate("insert into DWHPartition values ('" + storageId + "','" + viewName + "_06',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE')");
    // This should not be added to the view as this is a testStorageId
    stmt.executeUpdate("insert into DWHPartition values ('" + testStorageId + "','" + testViewName + "_06',"
        + "'2010-02-28 00:00:00.0','2010-03-01 00:00:00.0','ACTIVE')");
  }
}
