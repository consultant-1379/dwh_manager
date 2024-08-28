package com.distocraft.dc5000.dwhm;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.cache.GroupTypeDef;
import com.distocraft.dc5000.repository.cache.GroupTypeKeyDef;
import com.distocraft.dc5000.repository.cache.GroupTypesCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Grouptypes;
import com.distocraft.dc5000.repository.dwhrep.GrouptypesFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.Referencecolumn;
import com.distocraft.dc5000.repository.dwhrep.Typeactivation;
import com.distocraft.dc5000.repository.dwhrep.TypeactivationFactory;
import com.ericsson.eniq.common.testutilities.BaseUnitTestX;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import com.ericsson.eniq.repository.ETLCServerProperties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;
import junit.framework.Assert;
import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.jmock.Expectations;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@SuppressWarnings({"deprecation"})
public class VersionUpdateActionTest extends BaseUnitTestX {

	private static RockFactory  rock = null;
	private static final String DC_TECHPACK_NAME = "DC_E_TEST";
	private static final String EVENT_TECHPACK_NAME = "EVENT_E_TEST";

	private static File expectedStatsInstall = null;
	private static File expectesStatsUpgrade = null;

	private static File expectedEventsInstall = null;
	private static File expectedEventsUpgrade = null;

	private static int old_UNLIMITED_QUERY_TIMEOUT_IN_SECONDS = RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS;

	private VersionUpdateAction versionUpdateAction;
	private Logger mockLog = Logger.getAnonymousLogger();

	private final String tpName = "DC_E_TEST_TP";
	private final String typeName = "DC_E_TEST_TP_TYPE_NAME";
	private final String tableLevel = "1MIN";
	private final String baseTableName = "TEST_BASE_TABLE_NAME";
	private final String partitionPlan = "TEST_PARTITION_PLAN";


	private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "VersionUpdateActionTest");


	@BeforeClass
	public static void beforeClass() throws Exception {
		System.setProperty("tad.formatter", TableAlterDetailsFormatter.class.getName());
		TableAlterDetailsFormatter.replace("cast(a.MOID as varchar)", "cast(a.MOID as varchar(28))");
		TableAlterDetailsFormatter.replace("delete MOID", "drop MOID");
		TableAlterDetailsFormatter.replace("RENAME  tmp_MOID", "alter column tmp_MOID RENAME");

		expectedStatsInstall = DirectoryHelper.findParentDirectory("setupSQL/versionUpdateAction/stats_install/expected.xml");
		expectesStatsUpgrade = DirectoryHelper.findParentDirectory("setupSQL/versionUpdateAction/stats_upgrade/expected.xml");

		expectedEventsInstall = DirectoryHelper.findParentDirectory("setupSQL/versionUpdateAction/events_install/expected.xml");
		expectedEventsUpgrade = DirectoryHelper.findParentDirectory("setupSQL/versionUpdateAction/events_upgrade/expected.xml");

		RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS = 0;
		createETLCServerProperties(TMP_DIR, DatabaseTestUtils.getTestDbUser(),
				DatabaseTestUtils.getTestDbPassword(), DatabaseTestUtils.getTestDbUrl(), DatabaseTestUtils.getTestDbDriver());
	}

	public static File createETLCServerProperties(final File dir, final String etlUser, final String etlPass,
			final String etlUrl, final String driver) throws IOException {
		final File etlcServerProperties = new File(dir, "ETLCServer.properties");
		if (!etlcServerProperties.getParentFile().exists() && !etlcServerProperties.getParentFile().mkdirs()) {
			fail("Failed to create directory " + etlcServerProperties.getParent());
		}
		if (!etlcServerProperties.exists() && !etlcServerProperties.createNewFile()) {
			fail("Failed to create file " + etlcServerProperties.getPath());
		}
		final PrintWriter pw = new PrintWriter(new FileWriter(etlcServerProperties));
		pw.println("ENGINE_DB_URL = " + etlUrl);
		pw.println("ENGINE_DB_USERNAME = " + etlUser);
		pw.println("ENGINE_DB_PASSWORD = " + etlPass);
		pw.println("ENGINE_DB_DRIVERNAME = " + driver);
		//pw.println("ENGINE_HOSTNAME=localhost|engine");
		pw.close();
		System.setProperty(ETLCServerProperties.DC_CONFIG_DIR_PROPERTY_NAME, etlcServerProperties.getParent());
		System.setProperty(ETLCServerProperties.CONFIG_DIR_PROPERTY_NAME, etlcServerProperties.getParent());
		return etlcServerProperties;
	}

	@AfterClass
	public static void afterClass() {
		System.setProperty("tad.formatter", "");
		RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS = old_UNLIMITED_QUERY_TIMEOUT_IN_SECONDS;
		DirectoryHelper.delete(TMP_DIR);
	}

	@Before
	public void before() throws Exception {

		// initialise StaticProperties
		final Properties prop = new Properties();
		prop.setProperty("property1", "value1");
		prop.setProperty("SQLBatcher.maxStatementsPerBatch", "1");
		StaticProperties.giveProperties(prop);

		rock = DatabaseTestUtils.getTestDbConnection();
		versionUpdateAction = new VersionUpdateAction(rock, rock, tpName, mockLog);

		Field sqlLogField = VersionUpdateAction.class.getDeclaredField("sqlLog");
		sqlLogField.setAccessible(true);
		sqlLogField.set(versionUpdateAction, mockLog);
		Field logField = VersionUpdateAction.class.getDeclaredField("log");
		logField.setAccessible(true);
		logField.set(versionUpdateAction, mockLog);
	}

	@After
	public void after() throws Exception {
		DatabaseTestUtils.close(rock);
	}

	@Test
	public void testMaxColsPerStatementSetFromStaticProperties() throws Exception {
		final int expectedMaxColsPerStatement = 100000;

		// initialise StaticProperties
		final Properties prop = new Properties();
		prop.setProperty("VersionUpdateAction.maxColsPerStatement", String.valueOf(expectedMaxColsPerStatement));
		StaticProperties.giveProperties(prop);

		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/events_install");
		final VersionUpdateAction action = new VersionUpdateAction(
				rock, rock, EVENT_TECHPACK_NAME, Logger.getAnonymousLogger());

		assertEquals("Max cols per statement not picked up from static.properties", expectedMaxColsPerStatement, action.getMaxColsPerStatement());
	}	
	
	@Test
	public void testMaxColsPerStatementDefault() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/events_install");
		final VersionUpdateAction action = new VersionUpdateAction(
				rock, rock, EVENT_TECHPACK_NAME, Logger.getAnonymousLogger());

		assertEquals("Max cols per statement should default to 100 when it isn't declared in static.properties", 100, action.getMaxColsPerStatement());
	}	

	@Test
	public void testExecute_Install_Events() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/events_install");
		final VersionUpdateAction action = new VersionUpdateAction(
				rock, rock, EVENT_TECHPACK_NAME, Logger.getAnonymousLogger());
		action.execute();
		assertTables(expectedEventsInstall);
	}

	@Test
	public void testExecute_Upgrade_Events() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/events_upgrade");
		final VersionUpdateAction action = new VersionUpdateAction(
				rock, rock, EVENT_TECHPACK_NAME, Logger.getAnonymousLogger());
		action.execute();
		assertTables(expectedEventsUpgrade);
	}

	@Test
	public void testExecute_Install_Stats() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/stats_install");
		final VersionUpdateAction action = new VersionUpdateAction(
				rock, rock, DC_TECHPACK_NAME, Logger.getAnonymousLogger());
		action.execute();
		assertTables(expectedStatsInstall);
	}

	@Test
	public void testExecute_Upgrade_Stats() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/stats_upgrade");
		final VersionUpdateAction action = new VersionUpdateAction(
				rock, rock, DC_TECHPACK_NAME, Logger.getAnonymousLogger()) {
			@Override
			protected Map<String, String> getEtlRepConnectionDetails() throws Exception {
				final Map<String, String> cp = new HashMap<String, String>(6);
				cp.put("url", DatabaseTestUtils.getTestDbUrl());
				cp.put("user", DatabaseTestUtils.getTestDbUser());
				cp.put("pass", DatabaseTestUtils.getTestDbPassword());
				cp.put("driver", DatabaseTestUtils.getTestDbDriver());
				return cp;
			}
		};
		action.execute();
		assertTables(expectesStatsUpgrade);
	}

	@Test
	public void testExistingGroupMgtPartsUpdated() throws Exception {
		//Tests that a modified grouptype def will get synched to the dwh<> tables
		// StorageTimeAction should then generate alter table statements....
		DatabaseTestUtils.loadSetup(rock, "gpMgtChange");
		final String tpName = "EVENT_E_SGEH";
		//Change an existing group def....
		final Grouptypes where = new Grouptypes(rock);
		final String TAC = "TAC";
		final String VARCHAR = "varchar";
		final int SIZE = 128;
		where.setGrouptype(TAC);
		final GrouptypesFactory fac = new GrouptypesFactory(rock, where);
		final List<Grouptypes> toChange = fac.get();
		if (toChange.isEmpty()) {
			Assert.fail("Failed to setup testcase, Grouptype " + TAC + " not found.");
		}
		final Grouptypes tac = toChange.get(0);
		tac.setDatatype(VARCHAR);
		tac.setDatasize(SIZE);
		tac.saveDB();
		final VersionUpdateAction action = new VersionUpdateAction(rock, rock, tpName, mockLog);
		action.execute(true);

		final Dwhcolumn dwhere = new Dwhcolumn(rock);
		dwhere.setStorageid("EVENT_E_SGEH_GROUP_TYPE_E_" + TAC + ":PLAIN");
		dwhere.setDataname(TAC);
		final DwhcolumnFactory dfac = new DwhcolumnFactory(rock, dwhere);
		final List<Dwhcolumn> checks = dfac.get();
		if (checks.isEmpty()) {
			Assert.fail("No Dwhcolum for " + TAC + " found");
		}
		final Dwhcolumn toCheck = checks.get(0);
		assertEquals("Grouptypes Data type change not synched to Dwhcolumn", VARCHAR, toCheck.getDatatype());
		assertEquals("Grouptypes Data size change not synched to Dwhcolumn", SIZE, toCheck.getDatasize().intValue());
	}

	@Test
	public void testNewGroupMgtPartsDefined() throws Exception {
		// Tests that newly defined grouptypes are synched to the DWH<> tables
		// StorageTimeAction will then use these tables to synch tp dwhdb.
		final String tpName = "EVENT_E_SGEH";
		final String versionId = tpName + ":((7))";
		DatabaseTestUtils.loadSetup(rock, "dwhManagerGpMgt");
		final VersionUpdateAction action = new VersionUpdateAction(rock, rock, tpName, mockLog);
		action.execute(true);

		//Verify Dwhtype & Typeactication for storageid's:
		// -- EVENT_E_SGEH_GROUP_TYPE_E_APN:PLAIN
		// -- EVENT_E_SGEH_GROUP_TYPE_E_TAC:PLAIN
		// -- EVENT_E_SGEH_GROUP_TYPE_E_IMSI:PLAIN
		//
		//Verify Dwhcolumns created for each GroupType based on storageid

		final String base = "EVENT_E_SGEH_GROUP_TYPE_E_";
		final List<String> verifyGroups = Arrays.asList("APN", "TAC", "IMSI");
		final List<String> verifyTypes = new ArrayList<String>(verifyGroups.size() * 2);
		final List<String> verifyStorageIds = new ArrayList<String>(verifyGroups.size() * 2);
		final Map<String, String> storageIdToTypeMapping = new HashMap<String, String>();
		for (String gpTypeName : verifyGroups) {
			final String type = base + gpTypeName;
			verifyTypes.add(type);
			final String sid = type + ":PLAIN";
			verifyStorageIds.add(sid);
			storageIdToTypeMapping.put(sid, gpTypeName);
		}

		//Check each type is active...
		for (String typeToCheck : verifyTypes) {
			verifyTypesActivated(tpName, typeToCheck, rock, 1);
		}
		final Map<String, GroupTypeDef> typeDefs = GroupTypesCache.getGrouptypesDef(versionId);
		// Check that all the Dhwtypes are created....
		for (String sidToCheck : verifyStorageIds) {
			verifyDwhtypeCreated(sidToCheck, tpName, typeDefs, rock);
		}
		//Check that all the Dwhcolumns were created.
		for (String sidToCheck : verifyStorageIds) {
			final Dwhcolumn cwhere = new Dwhcolumn(rock);
			cwhere.setStorageid(sidToCheck);
			final DwhcolumnFactory fac = new DwhcolumnFactory(rock, cwhere);
			final List<Dwhcolumn> list = fac.get();
			final String gpTypeName = storageIdToTypeMapping.get(sidToCheck);
			final GroupTypeDef template = GroupTypesCache.getGrouptypesDef(versionId).get(gpTypeName);
			assertEquals(template.getKeys().size(), list.size());
			for (Dwhcolumn col : list) {
				assertTrue(template.isValidKey(col.getDataname()));
				final GroupTypeKeyDef keyTemplate = template.getKey(col.getDataname());
				assertEquals("Name not the same", keyTemplate.getKeyName(), col.getDataname());
				assertEquals("Type not the same", keyTemplate.getKeyType(), col.getDatatype());
				assertEquals("Size not the same", keyTemplate.getKeySize(), col.getDatasize().intValue());
				assertEquals("Scale not the same", keyTemplate.getKeyScale(), col.getDatascale().intValue());
				assertEquals("Index Type not the same", keyTemplate.getKeyIndexType(), col.getIndexes());
				assertEquals("Unique not the same", keyTemplate.getKeyUniqueKey(), col.getUniquekey().intValue());
				assertEquals("Unique values not the same", keyTemplate.getKeyUniqueValue(), col.getUniquevalue().longValue());
				assertEquals("Is Nullable not the same", keyTemplate.isKeyNullable(), col.getNullable().intValue());
				assertEquals("Is Nullable not the same", "ENABLED", col.getStatus());
			}
		}
	}

	@Test
	public void testGroupMgtNoneDefined() throws Exception {
		// Tests that newly defined grouptypes are synched to the DWH<> tables
		// StorageTimeAction will then use these tables to synch tp dwhdb.
		final String tpName = "DIM_E_RBS";
		final String versionId = tpName + ":((42))";
		DatabaseTestUtils.loadSetup(rock, "dwhManagerGpMgt");
		final VersionUpdateAction action = new VersionUpdateAction(rock, rock, tpName, mockLog);
		action.execute(true);

		//Verify Dwhtype & Typeactication for storageid's:
		// -- EVENT_E_SGEH_GROUP_TYPE_E_APN:PLAIN
		// -- EVENT_E_SGEH_GROUP_TYPE_E_TAC:PLAIN
		// -- EVENT_E_SGEH_GROUP_TYPE_E_IMSI:PLAIN
		//
		//Verify Dwhcolumns created for each GroupType based on storageid

		final String base = "EVENT_E_SGEH_GROUP_TYPE_E_";
		final List<String> verifyGroups = Arrays.asList("APN", "TAC", "IMSI");
		final List<String> verifyTypes = new ArrayList<String>(verifyGroups.size() * 2);
		final List<String> verifyStorageIds = new ArrayList<String>(verifyGroups.size() * 2);
		for (String gpTypeName : verifyGroups) {
			final String type = base + gpTypeName;
			verifyTypes.add(type);
			final String sid = type + ":PLAIN";
			verifyStorageIds.add(sid);
		}

		//Check each type is active...
		for (String typeToCheck : verifyTypes) {
			verifyTypesActivated(tpName, typeToCheck, rock, 0);
		}
		assertFalse("False should have been returned as there are no Grouptypes defined",
				GroupTypesCache.areGroupsDefined(versionId));
		//Check that all the Dwhcolumns were created.
		for (String sidToCheck : verifyStorageIds) {
			final Dwhcolumn cwhere = new Dwhcolumn(rock);
			cwhere.setStorageid(sidToCheck);
			final DwhcolumnFactory fac = new DwhcolumnFactory(rock, cwhere);
			final List<Dwhcolumn> list = fac.get();
			assertEquals("No Dwhcolums should exist with this storageid (its the only once that can be defined)", 0, list.size());
		}
	}

	@Test
	public void testConvertBigDecimalToInteger(){
		VersionUpdateAction versionUpdateAction = new VersionUpdateAction();
		BigDecimal bigD = new BigDecimal(123);
		int actual = versionUpdateAction.convertObjectToInteger(bigD);
		assertEquals(123, actual);
	}

	@Test
	public void testConvertIntegerToInteger(){
		VersionUpdateAction versionUpdateAction = new VersionUpdateAction();
		Integer integer = 123;
		int actual = versionUpdateAction.convertObjectToInteger(integer);
		assertEquals(123, actual);
	}

	@Test
	public void testGetTablesRealColumnList_for_DC_E_MGW_ATMPORT_DAY_01() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "dwhManagerRemoveDWH");
		VersionUpdateAction versionUpdateAction = new VersionUpdateAction();
		Field dwhrockField;
		Field sqlLogField;
		try {
			dwhrockField = VersionUpdateAction.class.getDeclaredField("dwhrock");
			dwhrockField.setAccessible(true);
			dwhrockField.set(versionUpdateAction, rock);
			sqlLogField = VersionUpdateAction.class.getDeclaredField("sqlLog");
			sqlLogField.setAccessible(true);
			sqlLogField.set(versionUpdateAction, mockLog);

			final String testTableName = "DC_E_MGW_ATMPORT_DAY_01";
			List<String> expectedResult = new ArrayList<String>();
			expectedResult.add("OSS_ID");
			expectedResult.add("SN");
			expectedResult.add("NEUN");
			expectedResult.add("NEDN");
			expectedResult.add("NESW");
			expectedResult.add("MGW");
			expectedResult.add("MOID");
			expectedResult.add("TransportNetwork");
			expectedResult.add("AtmPort");
			expectedResult.add("userLabel");
			expectedResult.add("DATE_ID");
			expectedResult.add("YEAR_ID");
			expectedResult.add("MONTH_ID");
			expectedResult.add("DAY_ID");
			expectedResult.add("WEEK_ID");
			expectedResult.add("DATACOVERAGE");
			expectedResult.add("AGG_COUNT");
			expectedResult.add("TIMELEVEL");
			expectedResult.add("SESSION_ID");
			expectedResult.add("BATCH_ID");
			expectedResult.add("PERIOD_DURATION");
			expectedResult.add("ROWSTATUS");
			expectedResult.add("DC_RELEASE");
			expectedResult.add("DC_SOURCE");
			expectedResult.add("DC_TIMEZONE");
			expectedResult.add("DC_SUSPECTFLAG");
			expectedResult.add("pmTransmittedAtmCells");
			expectedResult.add("pmReceivedAtmCells");
			expectedResult.add("pmSecondsWithUnexp");

			final List<String> actualResult = versionUpdateAction.getTablesRealColumnList(testTableName);
			assertEquals(expectedResult, actualResult);
		} catch (Exception e) {
			fail("The test failed!: "+e.getCause());
		}
	}

	@Test
	public void testGetLengthFromRealColumnWhenItsSybase15AndLengthIsABigDecimal() {
		Map<String, Object> realColumn = new HashMap<String, Object>();
		int columnLength = 4;
		BigDecimal valueAsBigDecimal = new BigDecimal(columnLength);
		realColumn.put("length", valueAsBigDecimal);
		int result = versionUpdateAction.getValueFromRealColumn(realColumn, "length");
		assertThat(result, is(columnLength));
	}

	@Test
	public void testGetLengthFromRealColumnWhenItsSybase12AndLengthIsAnInteger() {
		Map<String, Object> realColumn = new HashMap<String, Object>();
		int valueAsInteger = 3;
		realColumn.put("length", valueAsInteger);
		int result = versionUpdateAction.getValueFromRealColumn(realColumn, "length");
		assertThat(result, is(valueAsInteger));
	}

	@Test
	public void checkThatCorrectVelocityTemplateIsUsedInDwhTypeWhenEventCalcIsNull() throws Exception {
		final Measurementtype mockMeasType = mock(Measurementtype.class);
		final Measurementtable mockMeasTable = mock(Measurementtable.class);
		final RockFactory mockRockFactory = mock(RockFactory.class);

		setUpExpectationsForCreatingDwhTypeForMeasType(mockMeasType, mockMeasTable, mockRockFactory);
		final String expectedVelocityTemplateNameForView = "createview.vm";
		final String expectedVelocityTemplateNameForPublicView = "createpublicview.vm";
		expectGetEventscalctableOnMeasType(null, mockMeasType);
		final VersionUpdateAction mockedVUA = new VersionUpdateAction(mockRockFactory, mockRockFactory, tpName, mockLog);

		final Dwhtype dwhType = mockedVUA.createDwhTypeForMeasType(mockMeasType, mockMeasTable);
		checkDwhTypeIsCorrect(expectedVelocityTemplateNameForView, expectedVelocityTemplateNameForPublicView, dwhType);
	}

	@Test
	public void checkThatCorrectVelocityTemplateIsUsedInDwhTypeWhenEventCalcIsZero() throws Exception {
		final Measurementtype mockMeasType = mock(Measurementtype.class);
		final Measurementtable mockMeasTable = mock(Measurementtable.class);
		final RockFactory mockRockFactory = mock(RockFactory.class);

		setUpExpectationsForCreatingDwhTypeForMeasType(mockMeasType, mockMeasTable, mockRockFactory);
		final String expectedVelocityTemplateNameForView = "createview.vm";
		final String expectedVelocityTemplateNameForPublicView = "createpublicview.vm";
		expectGetEventscalctableOnMeasType(0, mockMeasType);
		final VersionUpdateAction mockedVUA = new VersionUpdateAction(mockRockFactory, mockRockFactory, tpName, mockLog);

		final Dwhtype dwhType = mockedVUA.createDwhTypeForMeasType(mockMeasType, mockMeasTable);
		checkDwhTypeIsCorrect(expectedVelocityTemplateNameForView, expectedVelocityTemplateNameForPublicView, dwhType);
	}

	@Test
	public void checkThatCorrectVelocityTemplateIsUsedInDwhTypeWhenEventCalcIsOne() throws Exception {
		final Measurementtype mockMeasType = mock(Measurementtype.class);
		final Measurementtable mockMeasTable = mock(Measurementtable.class);
		final RockFactory mockRockFactory = mock(RockFactory.class);

		setUpExpectationsForCreatingDwhTypeForMeasType(mockMeasType, mockMeasTable, mockRockFactory);
		final String expectedVelocityTemplateNameForView = "createeventscalcview.vm";
		final String expectedVelocityTemplateNameForPublicView = "createpubliceventscalcview.vm";
		expectGetEventscalctableOnMeasType(1, mockMeasType);
		final VersionUpdateAction mockedVUA = new VersionUpdateAction(mockRockFactory, mockRockFactory, tpName, mockLog);

		final Dwhtype dwhType = mockedVUA.createDwhTypeForMeasType(mockMeasType, mockMeasTable);
		checkDwhTypeIsCorrect(expectedVelocityTemplateNameForView, expectedVelocityTemplateNameForPublicView, dwhType);
	}

	@Test
	public void testGetTablesRealColumns_for_DC_E_MGW_ATMPORT_DAY_01() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "dwhManagerRemoveDWH");
		Field dwhrockField;
		try {
			dwhrockField = VersionUpdateAction.class.getDeclaredField("dwhrock");
			dwhrockField.setAccessible(true);
			dwhrockField.set(versionUpdateAction, rock);

			final String testTableName = "DC_E_MGW_ATMPORT_DAY_01";
			final int expectedResult = 29;

			final Map<String, HashMap<String, Object>> actualResult = versionUpdateAction.getTablesRealColumns(testTableName);
			assertEquals(expectedResult, actualResult.size());
		} catch (Exception e) {
			fail("The test failed!: "+e.getCause());
		}
	}

	@Test
	public void testVerifyTypesSetTo_OBSOLITE_IfNotInTechpack() throws Exception {
		//This test should show there there are 9 types set to OBSOLETE.
		//setup the test...
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/verifyTypes");
		final String techpackname = "EVENT_E_TEST";

		Field techpacknameField = VersionUpdateAction.class.getDeclaredField("techpackname");
		techpacknameField.setAccessible(true);
		techpacknameField.set(versionUpdateAction, techpackname);

		final Method verifyTypesMethod = VersionUpdateAction.class.getDeclaredMethod("verifyTypes");
		verifyTypesMethod.setAccessible(true);
		verifyTypesMethod.invoke(versionUpdateAction);

		final Dwhtype dt_cond = new Dwhtype(rock);
		dt_cond.setTechpack_name(techpackname);
		dt_cond.setStatus("OBSOLETE");
		final DwhtypeFactory dt_fact = new DwhtypeFactory(rock, dt_cond);
		final List<Dwhtype> existing_types = dt_fact.get();
		final int expectedListSize = 3;
		assertEquals("Expected "+expectedListSize+" but got: "+existing_types.size()+
				" Dwhtypes set to OBSOLETE.", expectedListSize, existing_types.size());

	} //testVerifyTypesSetTo_OBSOLITE_IfNotInTechpack

        @Test
        public void testVerifyTypesHistoryDynamic() throws Exception {
                //Test verifyTypes method for update policy=4=HistoryDynamic.
                // Should create _CALC & _HIST_RAW ReferenceColumns.
            DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/verifyTypes");

                final String techpackname = "EVENT_E_TEST";
            Field techpacknameField = VersionUpdateAction.class.getDeclaredField("techpackname");
            techpacknameField.setAccessible(true);
            techpacknameField.set(versionUpdateAction, techpackname);

            final String newVersionID = "DC_E_TEST:((2))";
            Field newVersionIDField = VersionUpdateAction.class.getDeclaredField("newVersionID");
            newVersionIDField.setAccessible(true);
            newVersionIDField.set(versionUpdateAction, newVersionID);
            //Run verifyTypes() method
            final Method verifyTypesMethod = VersionUpdateAction.class.getDeclaredMethod("verifyTypes");
            verifyTypesMethod.setAccessible(true);
            verifyTypesMethod.invoke(versionUpdateAction);
            // Verify result
            //Test Positive case: get _CALC & _HIST_RAW for update policy=4=HistoryDynamic.
            verifyDwhtypeCreated("DIM_E_TEST_ELEMBH_BHTYPE_CURRENT_DC:PLAIN", tpName, null, rock);
                verifyDwhtypeCreated("DIM_E_TEST_ELEMBH_BHTYPE_CALC:PLAIN", tpName, null, rock);
                verifyDwhtypeCreated("DIM_E_TEST_ELEMBH_BHTYPE_HIST_RAW:PLAIN", tpName, null, rock);
            final Dwhtype dt_cond = new Dwhtype(rock);
            dt_cond.setTypename("SELECT_E_TEST_AGGLEVEL_CALC");
            DwhtypeFactory dt_fact = new DwhtypeFactory(rock, dt_cond);
            List<Dwhtype> existing_types = dt_fact.get();
            final int expectedListSize = 0;
            assertEquals("Expected "+expectedListSize+" but got: "+existing_types.size()+" There should be no _CALC table when update policy is 0", expectedListSize, existing_types.size());
            dt_cond.setTypename("SELECT_E_TEST_AGGLEVEL_HIST_RAW");
            dt_fact = new DwhtypeFactory(rock, dt_cond);
            existing_types = dt_fact.get();
            assertEquals("Expected "+expectedListSize+" but got: "+existing_types.size()+" There should be no _HIST_RAW table when update policy is 0", expectedListSize, existing_types.size());
        } //testVerifyTypesHistoryDynamic


	/*@Test
	public void testCreateDwhTypeForMeasType(){
		fail("not implemented yet.");
	}

	@Test
	public void testGetMeasurementColumns(){
		fail("not implemented yet.");		
	}

	@Test
	public void testGetReferenceColumns(){
		fail("not implemented yet.");		
	}

	@Test
	public void testGetVectorCounterColumns(){
		fail("not implemented yet.");		
	}

	@Test
	public void testVerifyETLDuplicateCheckTable(){
		fail("not implemented yet.");		
	}

	@Test
	public void testGetActiveDWHColumns(){
		fail("not implemented yet.");		
	}

	@Test
	public void testHasParam(){
		fail("not implemented yet.");		
	}

	@Test
	public void testVerifyGroupColumns(){
		fail("not implemented yet.");		
	}

	@Test
	public void testGetTablesRealColumnList(){
		fail("not implemented yet.");				
	}

	@Test
	public void testGetTablesRealColumnsIndexes(){
		fail("not implemented yet.");				
	}

	@Test
	public void testExecuteSQL(){
		fail("not implemented yet.");				
	}

	@Test
	public void testCheckRealPartitions(){
		fail("not implemented yet.");				
	}

	@Test
	public void testDropIndex(){
		fail("not implemented yet.");				
	}

	@Test
	public void testGetDCUser(){
		fail("not implemented yet.");				
	}

	@Test
	public void testAlterTableAlterColumn(){
		fail("not implemented yet.");				
	}

	@Test
	public void testGetDWHPartitions(){
		fail("not implemented yet.");				
	}*/

	@Test
	public void testGetBHColumns() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "dwhManagerRemoveDWH");
		try{
			final String techpackname = "DC_E_MGW";
			final String newVersionID = "DC_E_MGW:((802))";
			final List<String> expectedResults = new ArrayList<String>();
			expectedResults.add("DC_E_MGW:((802)):DIM_E_MGW_ATMPORTBH_BHTYPE");
			expectedResults.add("DC_E_MGW:((802)):DIM_E_MGW_ELEMBH_BHTYPE");

			Field reprockField = VersionUpdateAction.class.getDeclaredField("reprock");
			reprockField.setAccessible(true);
			reprockField.set(versionUpdateAction, rock);

			Field techpacknameField = VersionUpdateAction.class.getDeclaredField("techpackname");
			techpacknameField.setAccessible(true);
			techpacknameField.set(versionUpdateAction, techpackname);

			Field newVersionIDField = VersionUpdateAction.class.getDeclaredField("newVersionID");
			newVersionIDField.setAccessible(true);
			newVersionIDField.set(versionUpdateAction, newVersionID);


			final Map<String, Vector<Referencecolumn>> result = versionUpdateAction.getBHColumns();
			assertFalse(result.isEmpty());
			Collection<Vector<Referencecolumn>> c = result.values();
			Iterator<Vector<Referencecolumn>> itr = c.iterator();
			int count = 0;
			while(itr.hasNext()){
				Referencecolumn referenceColumn = itr.next().get(0);
				assertEquals("Expected: "+expectedResults.get(count)+" but got "+referenceColumn.getTypeid(),expectedResults.get(count), referenceColumn.getTypeid());
				count++;
			}
		}catch(Exception e){
			fail("The test Failed: "+e.getCause());
		}
	}

	@Test
	public void testVerifyVersions() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "dwhManagerRemoveDWH");
		try{
			String techpackname = "DC_E_MGW";
			Field reprockField = VersionUpdateAction.class.getDeclaredField("reprock");
			reprockField.setAccessible(true);
			reprockField.set(versionUpdateAction, rock);
			Field techpacknameField = VersionUpdateAction.class.getDeclaredField("techpackname");
			techpacknameField.setAccessible(true);
			techpacknameField.set(versionUpdateAction, techpackname);

			final Method verifyVersionsMethod = VersionUpdateAction.class.getDeclaredMethod("verifyVersions");
			verifyVersionsMethod.setAccessible(true);
			Boolean b = (Boolean) verifyVersionsMethod.invoke(versionUpdateAction);
			assertFalse("It should not needed to verify the version.", b);
		}catch(Exception e){
			fail("The test Failed: "+e.getCause());
		}
	}

	@Test
	public void testCheckRealPartitions_AlreadyDefined() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/vua");
		final String tableName = "SOMEEXISTINGTABLE";
		Statement stmt = null;
		try {
			stmt = rock.getConnection().createStatement();
			stmt.execute("CREATE TABLE " + tableName + " (col1 VARCHAR(20), col2 BIGINT)");
		} catch (Throwable e) {
			fail("Failed to setup testcase " + e.getMessage());
			return;
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {/**/}
			}
		}
		final ExtendedVersionUpdateAction action;
		action = new ExtendedVersionUpdateAction(rock);
		final boolean result = action.do_checkRealPartitions(tableName);
		assertTrue("Check for existing table should returned true", result);
	}

	@Test
	public void testCheckRealPartitions_NonExisting() throws Exception {
		DatabaseTestUtils.loadSetup(rock, "versionUpdateAction/vua");
		final ExtendedVersionUpdateAction action = new ExtendedVersionUpdateAction(rock);
		final boolean result = action.do_checkRealPartitions("someTableName");
		assertFalse("Check for non existing table should returned false", result);
	}

	@Test
	public void testCheckRealPartitions_ErrorInCheck() {
		try {
			final String tableToCheck = "someTable";

			final ExtendedVersionUpdateAction action = new ExtendedVersionUpdateAction(rock) {
				@Override
				protected String getTableSearchFilter(String tableName) {
					return "select count(table_blaaaaaaaaa) as tableCount from INFORMATION_SCHEMA.SYSTEM_TABLES where table_name = '" + tableToCheck + "'";
				}
			};
			action.do_checkRealPartitions_CheckCausesError(tableToCheck);
			fail("The sql should have failed and throws an error, not return false (as in the table doesnt exist!!)");
		} catch (Exception e) {

		}
	}

	private void assertTables(final File expectedResults) throws MalformedURLException, DatabaseUnitException, SQLException {
		final FlatXmlDataSetBuilder builder = new FlatXmlDataSetBuilder();
		builder.setColumnSensing(true);
		final IDataSet expectedDataSet = builder.build(expectedResults);
		final IDatabaseConnection connection = new DatabaseConnection(rock.getConnection());
		final IDataSet actualDataSet = connection.createDataSet();
		for (String eTables : expectedDataSet.getTableNames()) {
			final ITable expectedTable = expectedDataSet.getTable(eTables);
			final ITable actualTable = actualDataSet.getTable(eTables);
			Assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[]{"CREATIONDATE", "NEXTPARTITIONTIME"});
		}
	}

	private void verifyTypesActivated(final String tpName, final String typeToCheck,
			final RockFactory rf, final int expectedCount) throws SQLException, RockException {
		final Typeactivation twhere = new Typeactivation(rf);
		twhere.setTechpack_name(tpName);
		twhere.setType("GroupMgt");
		twhere.setTypename(typeToCheck);
		final TypeactivationFactory fac = new TypeactivationFactory(rf, twhere);
		final List<Typeactivation> actDefined = fac.get();
		assertEquals(expectedCount, actDefined.size());
		if (expectedCount == 0) {
			return;
		}
		final Typeactivation toCheck = actDefined.get(0);
		assertEquals(typeToCheck, toCheck.getTypename());
		assertEquals("PLAIN", toCheck.getTablelevel());
		assertEquals("ACTIVE", toCheck.getStatus());
		assertEquals(tpName, toCheck.getTechpack_name());
	}

	private void verifyDwhtypeCreated(final String storageId, final String tpName,
			final Map<String, GroupTypeDef> typeDefs,
			final RockFactory rf) throws RockException, SQLException {
		final Dwhtype dwhere = new Dwhtype(rf);
		dwhere.setStorageid(storageId);
		final DwhtypeFactory fac = new DwhtypeFactory(rf, dwhere);
		final List<Dwhtype> typesDefined = fac.get();
		assertEquals(1, typesDefined.size());
		final Dwhtype toCheck = typesDefined.get(0);
		assertEquals(storageId, toCheck.getStorageid());
		if(typeDefs != null) {
		String dwhGp = toCheck.getBasetablename();
		dwhGp = dwhGp.substring(dwhGp.lastIndexOf('_') + 1);
		assertTrue(typeDefs.containsKey(dwhGp));
		final GroupTypeDef def = typeDefs.get(dwhGp);
		assertEquals(tpName, def.getTechpackName(), toCheck.getTechpack_name());
		}
		assertEquals(tpName, "PLAIN", toCheck.getTablelevel());
	}


	private void checkDwhTypeIsCorrect(String expectedVelocityTemplateNameForView,
			String expectedVelocityTemplateNameForPublicView, Dwhtype dwhType) {
		assertEquals(tpName, dwhType.getTechpack_name());
		assertEquals(typeName, dwhType.getTypename());
		assertEquals(tableLevel, dwhType.getTablelevel());
		assertEquals(typeName + ":" + tableLevel, dwhType.getStorageid());
		assertEquals(new Long(0), dwhType.getPartitioncount());
		assertEquals("ENABLED", dwhType.getStatus());
		assertEquals("dc", dwhType.getOwner());
		assertEquals(expectedVelocityTemplateNameForView, dwhType.getViewtemplate());
		assertEquals("createpartition.vm", dwhType.getCreatetemplate());
		assertEquals(null, dwhType.getNextpartitiontime());
		assertEquals(baseTableName, dwhType.getBasetablename());
		assertEquals("DATE_ID", dwhType.getDatadatecolumn());
		assertEquals(expectedVelocityTemplateNameForPublicView, dwhType.getPublicviewtemplate());
		assertEquals("PARTITIONED", dwhType.getType());
	}

	private void setUpExpectationsForCreatingDwhTypeForMeasType(final Measurementtype mockMeasType,
			final Measurementtable mockMeasTable,
			final RockFactory mockRockFactory) throws SQLException, RockException {
		context.checking(new Expectations() {
			{
				exactly(2).of(mockMeasType).getTypename();
				will(returnValue(typeName));
				exactly(2).of(mockMeasTable).getTablelevel();
				will(returnValue(tableLevel));
				one(mockMeasTable).getBasetablename();
				will(returnValue(baseTableName));
				one(mockMeasTable).getPartitionplan();
				will(returnValue(partitionPlan));
				one(mockRockFactory).insertData(with(any(Object.class)));
			}
		});
	}

	private void expectGetEventscalctableOnMeasType(final Integer valueToReturn, final Measurementtype mt) {
		context.checking(new Expectations() {
			{
				one(mt).getEventscalctable();
				will(returnValue(valueToReturn));
			}
		});
	}

	private class ExtendedVersionUpdateAction extends VersionUpdateAction {
		private boolean dummyDatabase = true;

		public ExtendedVersionUpdateAction(final RockFactory dwhrep, final RockFactory dwhdb, String tpName, final boolean dummyEtlrep) throws Exception {
			super(dwhrep, dwhdb, tpName, Logger.getAnonymousLogger());
			dummyDatabase = dummyEtlrep;
		}

		public ExtendedVersionUpdateAction(final RockFactory dwhrepConn) throws Exception {
			super(dwhrepConn, dwhrepConn, null, Logger.getAnonymousLogger());
		}

		public boolean do_checkRealPartitions(String tablename) throws Exception {
			return super.checkRealPartitions(tablename);
		}

		public boolean do_checkRealPartitions_CheckCausesError(String tablename) throws Exception {
			// this is the old way things would work, an error in the select *.... resulted in the table getting deleted...
			return super.checkRealPartitions(tablename);
		}

		@Override
		protected Map<String, String> getEtlRepConnectionDetails() throws Exception {
			final Map<String, String> cp = new HashMap<String, String>(6);
			cp.put("url", DatabaseTestUtils.getTestDbUrl());
			cp.put("user", DatabaseTestUtils.getTestDbUser());
			cp.put("pass", DatabaseTestUtils.getTestDbPassword());
			cp.put("driver", DatabaseTestUtils.getTestDbDriver());
			return cp;
		}

		@Override
		protected String getTableSearchFilter(String tableName) {
			if (dummyDatabase) {
				return "select count(table_name) as tableCount from INFORMATION_SCHEMA.SYSTEM_TABLES where table_name = '" + tableName + "'";
			} else {
				return super.getTableSearchFilter(tableName);
			}
		}
	}
}//class VersionUpdateActionTest
