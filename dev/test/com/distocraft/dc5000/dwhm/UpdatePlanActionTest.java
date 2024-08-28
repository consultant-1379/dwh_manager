package com.distocraft.dc5000.dwhm;

import static org.junit.Assert.*;

import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.logging.Logger;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;
import org.junit.BeforeClass;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * @author ejarsok
 * 
 */

public class UpdatePlanActionTest {

  private static UpdatePlanAction upa;
  
  private static UpdatePlanAction upa2;
  
  private static Statement stm;

  private static Map<String, String> env = System.getenv();
  
  @BeforeClass
  public static void init() throws Exception {
    System.setProperty("CONF_DIR", env.get("WORKSPACE"));
    final File ini = new File(env.get("WORKSPACE"), "niq.ini");
    ini.deleteOnExit();
    PrintWriter pw = new PrintWriter(new FileWriter(ini));
    pw.write("[ENIQ_NET_INFO]\n");
    pw.write("ManagedNodesCORE=1\n");
    pw.write("[ENIQ_NET_INFO]\n");
    pw.write("ManagedNodesGRAN=1\n");
    pw.write("[ENIQ_NET_INFO]\n");
    pw.write("ManagedNodesWRAN=1\n");
    pw.close();
    RockFactory rockFact = DatabaseTestUtils.getTestDbConnection();
    Logger log = Logger.getLogger("Logger");

    stm = rockFact.getConnection().createStatement();

    stm.execute("CREATE TABLE Partitionplan (PARTITIONPLAN VARCHAR(20), DEFAULTSTORAGETIME BIGINT,"
      + "DEFAULTPARTITIONSIZE BIGINT, MAXSTORAGETIME BIGINT, PARTITIONTYPE TINYINT)");

    stm.executeUpdate("INSERT INTO Partitionplan VALUES('Plan1', 1, 0, 10, 0)");


    stm.execute("CREATE TABLE Configuration (PARAMNAME VARCHAR(32), PARAMVALUE VARCHAR(20))");

    stm.executeUpdate("INSERT INTO Configuration VALUES('dwhm.Plan1.rowsPerTable', '1000')");
    stm.executeUpdate("INSERT INTO Configuration VALUES('dwhm.Plan1.rowsPerCellPerROP', '1')");
    stm.executeUpdate("INSERT INTO Configuration VALUES('dwhm.Plan1.defaultROP', '1')");
    stm.executeUpdate("INSERT INTO Configuration VALUES('dwhm.Plan1.maxTables', '100')");
    stm.executeUpdate("INSERT INTO Configuration VALUES('dwhm.Plan1.minTables', '0')");

    upa = new UpdatePlanAction(rockFact, log);
    upa2 = new UpdatePlanAction(rockFact, log);
  }
  
  @Test
  public void testGetConfigurationValue() {
    Class secretClass = upa.getClass();
    try {
      Method method = secretClass.getDeclaredMethod("getConfigurationValue", new Class[] {String.class});
      method.setAccessible(true);
      
      //if parameter = null invoke returns 1000.0f
      assertEquals(1000.0f, (Float) method.invoke(upa, new Object[] {"dwhm.Plan1.rowsPerTable"}), 0.0f);
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetConfigurationValue() failed");
    }
  }
  
  @Test
  public void testGetNumOfCellsValue() {
    Class secretClass = upa.getClass();
    try {
      Method method = secretClass.getDeclaredMethod("getNumOfCellsValue", null);
      method.setAccessible(true);
      
      assertEquals(3.0f, (Float) method.invoke(upa, null), 0.0f);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetNumOfCellsValue() failed");
    }
  }
  
  @Test
  public void testExecute() {
    try {
      upa.execute();
    } catch (EngineException e) {
      e.printStackTrace();
      fail("testExecute() failed");
    }
    
    ResultSet rs;
    try {
      rs = stm.executeQuery("SELECT DEFAULTPARTITIONSIZE, MAXSTORAGETIME FROM Partitionplan WHERE PARTITIONPLAN='Plan1'");
      rs.next();

      String expected = "24,100";
      String actual = rs.getString(1) + "," + rs.getString(2);
      
      assertEquals(expected, actual);
      
    } catch (SQLException e) {
      e.printStackTrace();
      fail("testExecute() failed");
    }
  }

  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(UpdatePlanActionTest.class);
  }*/
}
