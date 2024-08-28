package com.distocraft.dc5000.dwhm;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.ericsson.eniq.common.testutilities.BaseUnitTestX;

public class CreateOverallViewsFactoryTest extends BaseUnitTestX {

  private final Logger loggerForClass = Logger.getLogger("CreateOverallViewsFactoryTest");

  private static Vector<Dwhtype> dwhTypes = new Vector<Dwhtype>();

  List<String> listOfCreatedViews = new ArrayList<String>();

  RockFactory mockForDbaConnectionToDwhdb;

  RockFactory mockForDcConnectionToDwhdb;

  RockFactory mockForDwhrepConnectiontoRepdb;

  Connection mockDcConnnection;

  Statement mockDcStatement;

  Connection mockDbaConnnection;

  Statement mockDbaStatement;

  Dwhtype mockDwhtype;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    StaticProperties.giveProperties(new Properties());
  }

  @Before
  public void setUp() {
    recreateMockeryContext();
    mockForDbaConnectionToDwhdb = context.mock(RockFactory.class, "mockForDbaConnectionToDwhdb");
    mockForDcConnectionToDwhdb = context.mock(RockFactory.class, "mockForDcConnectionToDwhdb");
    mockForDwhrepConnectiontoRepdb = context.mock(RockFactory.class, "mockForDwhrepConnectiontoRepdb");
    mockDcConnnection = context.mock(Connection.class, "mockDcConnection");
    mockDcStatement = context.mock(Statement.class, "mockDcStatement");
    mockDbaConnnection = context.mock(Connection.class, "mockDbaConnection");
    mockDbaStatement = context.mock(Statement.class, "mockDbaStatement");
    mockDwhtype = context.mock(Dwhtype.class);
  }

  @Test
  public void checkThatCorrectSqlStatementIsUsedForCreateOverallViewsActionForRawEventsWhenTechPackIsEVENT_E_SGEH() throws Exception {
    String expectedSqlStatementForDc = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_RAW' AND vcreator='dc') "
        + "> 0\nBEGIN\n  DROP VIEW dc.EVENT_E_SGEH_RAW\nEND\n\ncommit\n\n\nCREATE VIEW dc.EVENT_E_SGEH_RAW AS\nSELECT *,1 as "
        + "VIEW_NUMBER FROM dc.EVENT_E_SGEH_SUC_RAW\nUNION ALL\nSELECT *,2 as VIEW_NUMBER FROM dc.EVENT_E_SGEH_ERR_RAW\n\nGRANT "
        + "SELECT ON dc.EVENT_E_SGEH_RAW TO dc\nGRANT SELECT ON dc.EVENT_E_SGEH_RAW TO dcbo\n\ncommit\n\n";

    String expectedSqlStatementForDcPublic = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_RAW' "
        + "AND vcreator='dcpublic') > 0\nBEGIN\n  DROP VIEW dcpublic.EVENT_E_SGEH_RAW\nEND\n\ncommit\n\n\n\n\n\nCREATE "
        + "VIEW dcpublic.EVENT_E_SGEH_RAW AS \nSELECT\n,1 as VIEW_NUMBER\nFROM dc.EVENT_E_SGEH_SUC_RAW\nUNION "
        + "ALL\nSELECT\n,2 as VIEW_NUMBER\nFROM dc.EVENT_E_SGEH_ERR_RAW\n\n\ncommit\n";

    listOfCreatedViews.add("EVENT_E_SGEH_SUC_RAW");
    listOfCreatedViews.add("EVENT_E_SGEH_ERR_RAW");

    setUpExpections(expectedSqlStatementForDc, expectedSqlStatementForDcPublic);

    dwhTypes.add(mockDwhtype);

    CreateOverallViewsFactory.createOverallViewsAction(
        mockForDbaConnectionToDwhdb, mockForDcConnectionToDwhdb, mockForDwhrepConnectiontoRepdb, dwhTypes,
        loggerForClass, listOfCreatedViews, "EVENT_E_SGEH");
  }

  @Test
  public void checkThatCorrectSqlStatementIsUsedForCreateOverallViewsActionForEventsCalcWhenTechPackIsEVENT_E_SGEH()
      throws Exception {
    String expectedSqlStatementForDc = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_1MIN' AND vcreator='dc') > 0\nBEGIN\n"
        + "  DROP VIEW dc.EVENT_E_SGEH_1MIN\nEND\n\ncommit\n\n\n\n  \n  \n\nCREATE VIEW dc.EVENT_E_SGEH_1MIN AS(\nSELECT\n\nFROM(\n\n\n  "
        + "SELECT \n    FROM dc.EVENT_E_SGEH_SUC_1MIN\n      UNION ALL\n    SELECT \n    FROM dc.EVENT_E_SGEH_ERR_1MIN\n  ) AS "
        + "EVENT_E_SGEH_1MIN\nGROUP BY\n\n\n) GRANT SELECT ON dc.EVENT_E_SGEH_1MIN TO dc\nGRANT SELECT ON dc.EVENT_E_SGEH_1MIN TO "
        + "dcbo\n\ncommit\n\n";

    String expectedSqlStatementForDcPublic = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='EVENT_E_SGEH_1MIN' AND "
        + "vcreator='dcpublic') > 0\nBEGIN\n  DROP VIEW dcpublic.EVENT_E_SGEH_1MIN\nEND\n\ncommit\n\n\n\n\n\ncommit";

    listOfCreatedViews.add("EVENT_E_SGEH_SUC_1MIN");
    listOfCreatedViews.add("EVENT_E_SGEH_ERR_1MIN");

    setUpExpections(expectedSqlStatementForDc, expectedSqlStatementForDcPublic);
    dwhTypes.add(mockDwhtype);
    CreateOverallViewsFactory.createOverallViewsAction(mockForDbaConnectionToDwhdb, mockForDcConnectionToDwhdb,
        mockForDwhrepConnectiontoRepdb, dwhTypes, loggerForClass, listOfCreatedViews, "EVENT_E_SGEH");
  }

  @Test
  public void checkThatCreateOverallViewsActionDoesNothingWhenTechpackIsNOTEqualToEVENT_E_SGEH() throws Exception {
    context.checking(new Expectations() {

      {
        never(mockDwhtype);
        never(mockForDcConnectionToDwhdb);
        never(mockForDbaConnectionToDwhdb);
        never(mockForDwhrepConnectiontoRepdb);
      }
    });
    dwhTypes.add(mockDwhtype);
    CreateOverallViewsFactory.createOverallViewsAction(mockForDbaConnectionToDwhdb, mockForDcConnectionToDwhdb,
        mockForDwhrepConnectiontoRepdb, dwhTypes, loggerForClass, listOfCreatedViews, "TEST_E_TEST");
  }

  private void setUpExpections(final String expectedSqlStatementForDc, final String expectedSqlStatementForDcPublic)
      throws SQLException {
    context.checking(new Expectations() {

      {
        allowing(mockDwhtype);
        allowing(mockForDwhrepConnectiontoRepdb);

        one(mockForDcConnectionToDwhdb).getConnection();
        will(returnValue(mockDcConnnection));
        one(mockDcConnnection).createStatement();
        will(returnValue(mockDcStatement));
        allowing(mockDcStatement).executeUpdate(expectedSqlStatementForDc);
        one(mockDcStatement).close();

        one(mockForDbaConnectionToDwhdb).getConnection();
        will(returnValue(mockDbaConnnection));
        one(mockDbaConnnection).createStatement();
        will(returnValue(mockDbaStatement));
        allowing(mockDbaStatement).executeUpdate(expectedSqlStatementForDcPublic);
        one(mockDbaStatement).close();
      }
    });
  }

}
