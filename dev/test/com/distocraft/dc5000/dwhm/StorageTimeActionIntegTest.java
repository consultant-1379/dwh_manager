package com.distocraft.dc5000.dwhm;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class StorageTimeActionIntegTest {

  private static final Logger LOGGER = Logger.getAnonymousLogger();
  private static Map<String, RockFactory> schemaConnMapping = new HashMap<String, RockFactory>();

  private RockFactory dwhrep = null;
  private RockFactory etlrep = null;
  private RockFactory dwhdb = null;
  private RockFactory dwhdb_dba = null;

  private static final Map<String, String> typemapper = new HashMap<String, String>();

  private static final List<String> lengthTypes = Arrays.asList(
    "binary"
    , "char"
    , "nchar"
    , "uniqueidentifier"
    , "varbit"
    , "nvarchar"
    , "varbinary"
    , "varchar"
    , "decimal"
    , "numeric"
  );
  private static final List<String> scaleTypes = Arrays.asList(
    "decimal"
    , "numeric"
  );

  @BeforeClass
  public static void beforeClass() {
    typemapper.put("datetime", "timestamp");
    typemapper.put("integer", "int");
  }

  private void cleanup() {
    try {
      System.out.println("Cleaning up db's");
      DatabaseTestUtils.dropAllTables(dwhrep, "dwhrep");
      DatabaseTestUtils.dropAllTables(etlrep, "etlrep");
      DatabaseTestUtils.dropAllTables(dwhdb, "dc");
      DatabaseTestUtils.dropAllTables(dwhdb_dba, "dcpublic");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.out.println("Done cleaning up db's");
  }

  @Before
  public void before() throws Exception {
	System.out.println("Creating test db for dwhrep");
    dwhrep = DatabaseTestUtils.getTestDbIntegrationConnection(DatabaseTestUtils.DbType.DWHREP, getClass().getSimpleName(), true);
	System.out.println("Creating test db for etlrep");
    etlrep = DatabaseTestUtils.getTestDbIntegrationConnection(DatabaseTestUtils.DbType.ETLREP, getClass().getSimpleName(), true);
	System.out.println("Creating test db for dc");
    dwhdb = DatabaseTestUtils.getTestDbIntegrationConnection(DatabaseTestUtils.DbType.DC, getClass().getSimpleName(), true);
	System.out.println("Creating test db for dc_dba");
    dwhdb_dba = DatabaseTestUtils.getTestDbIntegrationConnection(DatabaseTestUtils.DbType.DC_DBA, getClass().getSimpleName(), true);

	System.out.println("Add db's to schemaConnMapping");
    schemaConnMapping.put("dwhrep", dwhrep);
    schemaConnMapping.put("default", dwhrep);
    schemaConnMapping.put("etlrep", etlrep);
    schemaConnMapping.put("dc", dwhdb);
	System.out.println("Done adding db's to schemaConnMapping");
    cleanup();
  }

  @After
  public void after() throws Exception {
    System.out.println("@After");
    cleanup();
    DatabaseTestUtils.close(dwhrep, false);
    DatabaseTestUtils.close(etlrep, false);
    DatabaseTestUtils.close(dwhdb, false);
    DatabaseTestUtils.close(dwhdb_dba, false);
    System.out.println("Done @After");
  }

  private void assertEquals(final String msg, final Object expected, final Object actual, final StringBuilder buffer) {
    try {
      Assert.assertEquals(msg, expected, actual);
    } catch (AssertionError e) {
      buffer.append(e.getMessage()).append("\n");
    }
  }

  private void fail(final String msg, final StringBuilder buffer) {
    buffer.append(msg).append("\n");
  }

  private void verifyDwh(final String techpackName, final String indexFilter) throws Exception {
    System.out.println("Verifying Techpack " + techpackName + " between repdb and dwhdb.");
    final Dwhtechpacks twhere = new Dwhtechpacks(dwhrep);
    twhere.setTechpack_name(techpackName);
    final DwhtechpacksFactory tfac = new DwhtechpacksFactory(dwhrep, twhere);
    final List<Dwhtechpacks> activated = tfac.get();
    if (activated.isEmpty()) {
      Assert.fail("No activated techpack called " + techpackName + " found in Dwhtechpacks");
    }

    final List<Dwhpartition> expectedPartitions = new ArrayList<Dwhpartition>();
    final Dwhtype ftype = new Dwhtype(dwhrep);
    ftype.setTechpack_name(techpackName);
    final DwhtypeFactory type_fac = new DwhtypeFactory(dwhrep, ftype);
    final List<Dwhtype> types = type_fac.get();
    for (Dwhtype t : types) {
      final Dwhpartition pwhere = new Dwhpartition(dwhrep);
      pwhere.setStorageid(t.getStorageid());
      final DwhpartitionFactory fac = new DwhpartitionFactory(dwhrep, pwhere);
      final List<Dwhpartition> typePartitions = fac.get();
      expectedPartitions.addAll(typePartitions);
      if ("DAYBH".equals(t.getTablelevel())) {
        //TODO STA creates the partition but doesnt update the Dwhpartition table with the entry, VersionUpdateAction should??
        final Dwhpartition part = new Dwhpartition(dwhrep);
        part.setStorageid(t.getStorageid());
        part.setTablename(t.getBasetablename() + "_CALC");
        part.setStarttime(new Timestamp(System.currentTimeMillis()));
        part.setEndtime(null);
        part.setStatus("ACTIVE");
        expectedPartitions.add(part);
      }
    }

    // Report all errors at once
    final StringBuilder errors = new StringBuilder();

    final DatabaseMetaData schema = dwhdb.getConnection().getMetaData();
    final Map<String, Dwhcolumn> indexMap = new HashMap<String, Dwhcolumn>();
    long start = System.currentTimeMillis();
    //Check tables
    for (Dwhpartition expectedTable : expectedPartitions) {
      final String tName = expectedTable.getTablename();
      System.out.println("Verifying partition " + tName);
      final ResultSet table = schema.getTables(null, "dc", tName, null);
      if (table.next()) {
        final String storageId = expectedTable.getStorageid();
        final Dwhcolumn cwhere = new Dwhcolumn(dwhrep);
        cwhere.setStorageid(storageId);
        final DwhcolumnFactory cfac = new DwhcolumnFactory(dwhrep, cwhere);
        final List<Dwhcolumn> expectedColumns = cfac.get();
        final Map<String, Dwhcolumn> ecMap = new HashMap<String, Dwhcolumn>();
        for (Dwhcolumn c : expectedColumns) {
          ecMap.put(c.getStorageid() + ":" + c.getDataname(), c);
        }
        final ResultSet actualColumns = schema.getColumns(null, "dc", tName, null);
        while (actualColumns.next()) {
          final String cName = actualColumns.getString("COLUMN_NAME");
          final String tKey = storageId + ":" + cName;
          if (ecMap.containsKey(tKey)) {
            final Dwhcolumn eColumn = ecMap.get(tKey);
            final String actual_TYPE_NAME = actualColumns.getString("TYPE_NAME");
            String expected_TYPE_NAME = eColumn.getDatatype();
            if (typemapper.containsKey(expected_TYPE_NAME)) {
              expected_TYPE_NAME = typemapper.get(expected_TYPE_NAME);
            }
            final int actual_NULLABLE = actualColumns.getInt("NULLABLE");
            final int actual_COLUMN_SIZE = actualColumns.getInt("COLUMN_SIZE");
            final int actual_DECIMAL_DIGITS = actualColumns.getInt("DECIMAL_DIGITS");

            assertEquals("Datatype for " + tKey + " is not correct", expected_TYPE_NAME, actual_TYPE_NAME, errors);
            assertEquals("Nullable for " + tKey + ":" + actual_TYPE_NAME + " is not correct", eColumn.getNullable(), actual_NULLABLE, errors);
            if (lengthTypes.contains(actual_TYPE_NAME.toLowerCase())) {
              assertEquals("ColumnSize for " + tKey + ":" + actual_TYPE_NAME + " is not correct", eColumn.getDatasize(), actual_COLUMN_SIZE, errors);
            }
            if (scaleTypes.contains(actual_TYPE_NAME.toLowerCase())) {
              assertEquals("ColumnScale for " + tKey + ":" + actual_TYPE_NAME + " is not correct", eColumn.getDatascale(), actual_DECIMAL_DIGITS, errors);
            }
            if (eColumn.getIndexes() != null && eColumn.getIndexes().trim().length() > 0) {
              final String indexKey = tName + "_" + cName + "_" + eColumn.getIndexes().trim();
              if (!indexMap.containsKey(indexKey)) {
                indexMap.put(indexKey, eColumn);
              }
            }
          } else {
            fail("No Column called '" + cName + "' found in table '" + storageId + "' in dwhdb", errors);
          }
        }
      } else {
        fail("Expected table " + tName + " not found in dwhdb", errors);
      }
    }
    long stop = System.currentTimeMillis();
    System.out.println("Took " + (stop - start) + "mSec to verify all tables/columns.");

    System.out.println("Verifying Views ...");
    //Check views..
    start = System.currentTimeMillis();
    final Dwhtype vwhere = new Dwhtype(dwhrep);
    final DwhtypeFactory vfac = new DwhtypeFactory(dwhrep, vwhere);
    final List<Dwhtype> views = vfac.get();
    for (Dwhtype pView : views) {
      if (pView.getViewtemplate() != null && pView.getViewtemplate().length() > 0) {
        System.out.println("Verifying View " + pView.getBasetablename() + " exists.");
        final ResultSet viewcheck = schema.getTables(null, "dc", pView.getBasetablename(), new String[]{"VIEW"});
        if (!viewcheck.next()) {
          fail("No View found for " + pView.getStorageid(), errors);
        }
      }
    }
    stop = System.currentTimeMillis();
    System.out.println("Took " + (stop - start) + "mSec to verify all views exist.");

    System.out.println("Verifying Indexes ...");
    start = System.currentTimeMillis();
    // Check indexes
    final Statement stmt = dwhdb.getConnection().createStatement();
    final String select = "select index_name, index_type from SYSINDEX where index_owner = 'USER' and index_name like '" + indexFilter + "'";
    final ResultSet indexes = stmt.executeQuery(select);
    while (indexes.next()) {
      final String iName = indexes.getString("index_name").trim();
      final String iType = indexes.getString("index_type").trim();
      if (indexMap.containsKey(iName)) {
        final Dwhcolumn eColumn = indexMap.remove(iName);
        assertEquals("Incorrect index on '" + eColumn.getStorageid() + "_" + eColumn.getDataname() + "' in dwhdb", eColumn.getIndexes(), iType, errors);
      } else {
        fail("Real database Index " + iName + " not defined meta DWH", errors);
      }
    }
    if (!indexMap.isEmpty()) {
      for (String key : indexMap.keySet()) {
        if (key.startsWith("GROUP_TYPE_E")) {
          continue;
        }
        fail("Index '" + key + "' defined in meta dwh but not in dwhdb", errors);
      }
    }
    stop = System.currentTimeMillis();
    System.out.println("Took " + (stop - start) + "mSec to verify all indexes exist.");

    if (errors.length() > 0) {
      Assert.fail(errors.toString());
    }
  }

  private void testTechpackInstall(final String base, final String testTechpack, final String indexFilter) throws Exception {
    DatabaseTestUtils.loadSetup(schemaConnMapping, base);
    StaticProperties.giveProperties(new Properties());
    ActivationCache.initialize(etlrep);
    PhysicalTableCache.initialize(etlrep);

    final VersionUpdateAction versionUpdateAction = new VersionUpdateAction(dwhrep, dwhdb, testTechpack, LOGGER) {
      @Override
      protected Map<String, String> getEtlRepConnectionDetails() throws Exception {
        final Map<String, String> cp = new HashMap<String, String>(6);
        cp.put("url", etlrep.getDbURL());
        cp.put("user", etlrep.getUserName());
        cp.put("pass", etlrep.getPassword());
        cp.put("driver", etlrep.getDriverName());
        return cp;
      }

      @Override
      int getNumberOfPhysicalCPUs() {
        return 1;
      }

    };
    versionUpdateAction.execute(true);
    new StorageTimeAction(dwhrep, etlrep, dwhdb, dwhdb_dba, testTechpack, LOGGER);
    verifyDwh(testTechpack, indexFilter);
  }

  @Test
  public void test_events_install() throws Exception {
	System.out.println("@Test test_events_install");
    final String testTechpack = "EVENT_E_TEST";
    final String indexFilter = "%_E_TEST_%";
    final String base = "storageTimeAction/events_install";
    testTechpackInstall(base, testTechpack, indexFilter);
  }
//
//  @Test
//  public void test_events_upgrade() throws Exception {
//    System.out.println("@Test test_events_upgrade");
//    final String testTechpack = "EVENT_E_TEST";
//    final String indexFilter = "%_E_TEST_%";
//    final String base = "storageTimeAction/events_upgrade";
//    testTechpackInstall(base, testTechpack, indexFilter);
//  }
//
//  @Test
//  public void test_stats_install() throws Exception {
//	System.out.println("@Test test_stats_install");
//    final String testTechpack = "DC_E_TEST";
//    final String indexFilter = "%_E_TEST_%";
//    final String base = "storageTimeAction/stats_install";
//    testTechpackInstall(base, testTechpack, indexFilter);
//  }
//
//  @Test
//  public void test_stats_upgrade() throws Exception {
//	System.out.println("@Test test_stats_upgrade");
//    final String testTechpack = "DC_E_TEST";
//    final String indexFilter = "%_E_TEST_%";
//    final String base = "storageTimeAction/stats_upgrade";
//    testTechpackInstall(base, testTechpack, indexFilter);
//  }
}
