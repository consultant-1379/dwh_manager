package com.distocraft.dc5000.dwhm;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ssc.rockfactory.RockFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DWHTableCheckActionTest {
  private RockFactory rock = null;
  private Logger mockLog;
  private DWHTableCheckAction testInstance;
  private Properties prop;

  @Before
  public void setUp() throws Exception {
    mockLog = Logger.getAnonymousLogger();

    rock = DatabaseTestUtils.getTestDbConnection();
    DatabaseTestUtils.loadSetup(rock, "tableCheckAction");
    // Setting up static properties
    prop = new Properties();
    prop.put("dwhm.debug", "true");
    final String currentLocation = System.getProperty("user.home");
    if (!currentLocation.endsWith("ant_common")) {
      prop.put("dwhm.templatePath", ".\\jar\\"); // Gets tests running on laptop
    }
    StaticProperties.giveProperties(prop);
  }

  @After
  public void tearDown() throws Exception {
    DatabaseTestUtils.close(rock);
  }

  @Test
  public void testGetDWHTables() {
    try {
      final String mode = "DELETE";

      // Creating Expected Map
      final Map<String, String> expectedMap = new HashMap<String, String>();
      expectedMap.put("DC_TABLE_A", "dc");// DC_TABLE_B is a view
      expectedMap.put("DC_TABLE_C", "dc");

      // Calling constructor
      testInstance = new DWHTableCheckAction(rock, rock, mockLog, prop, mode);
      // Asserting the output of function testInstance.getDWHTables() against our expectation
      final Map data = testInstance.getDWHTables();
      assertEquals(" Actual Map is not as expected ", expectedMap, data);
      //verify(s);
    } catch (Exception e) {
      fail("Fail due to exception: " + e.getMessage());
    }
  }

  @Test
  public void testDropTableFunctionality() throws Exception {
    final String mode = "DELETE";
    testInstance = new DWHTableCheckAction(rock, rock, mockLog, prop, mode);
    testInstance.execute();
    final String tableToBeDeleted = "DC_TABLE_A";
    final ResultSet tableList = rock.getConnection().getMetaData().getTables(null, "DC", tableToBeDeleted, null);
    while (tableList.next()) {
      final String aTable = tableList.getString("TABLE_NAME").trim();
      if (tableToBeDeleted.equalsIgnoreCase(aTable)) {
        fail("Table " + tableToBeDeleted + " should have been deleted as its not in Dwhpartition");
      }
    }
  }

}// class end