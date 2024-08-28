package com.distocraft.dc5000.dwhm;
//import static org.easymock.EasyMock.expect;
//import static org.easymock.EasyMock.expectLastCall;
//import static org.easymock.EasyMock.matches;
//import static org.easymock.classextension.EasyMock.createMock;
//import static org.easymock.classextension.EasyMock.createNiceMock;
//import static org.easymock.classextension.EasyMock.replay;
//import static org.easymock.classextension.EasyMock.verify;
import static org.hamcrest.Matchers.is;
import com.distocraft.dc5000.repository.dwhrep.Aggregation;
import com.distocraft.dc5000.repository.dwhrep.AggregationFactory;
import com.distocraft.dc5000.repository.dwhrep.Aggregationrule;
import com.distocraft.dc5000.repository.dwhrep.AggregationruleFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhoursource;
import com.distocraft.dc5000.repository.dwhrep.BusyhoursourceFactory;
import com.ericsson.eniq.common.TechPackType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.jmock.Expectations;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.cache.CountingManagementCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.ericsson.eniq.common.testutilities.BaseUnitTestX;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Busyhour;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementvector;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
public class StorageTimeActionTest extends BaseUnitTestX {
	private static RockFactory dwhrep = null;
	private static RockFactory etlrep = null;
	private static RockFactory dwhdb = null;
private static RockFactory dwhdb_dba = null;
private ActivationCache mockActivationCache = null;
private StorageTimeAction testInstance = null;
private SanityChecker mockSanityChecker = null;
private static final String FILE_EXTENSION = ".sql";
private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));
private static final String LOAD_DATA_DIR = TMP_DIR.getPath() + File.separator;
private static final String LOG_DIR = TMP_DIR.getPath();
private static final String REJECTED_DIR = TMP_DIR.getPath();
  
//@BeforeClass
//public static void beforeClass(){
//  if(!TMP_DIR.exists() && !TMP_DIR.mkdirs()){
//    fail("Failed to create temporary directory " + TMP_DIR.getPath());
//  }
//  TMP_DIR.deleteOnExit();
//}
//
//  @Before
//  public void before() throws Exception {
//    dwhdb_dba = DatabaseTestUtils.getTestDbConnection();
//    dwhrep = etlrep = dwhdb = dwhdb_dba;
//
//
//    DatabaseTestUtils.loadSetup(dwhdb_dba, "dwhManagerRemoveDWH");
//    final Properties p = new Properties();
//    p.put("dwhm.debug", "true");
//    if (!TMP_DIR.getName().equals("ant_common")) {
//      p.put("dwhm.templatePath", ".\\jar\\"); // Gets tests running on laptop
//    }
//    StaticProperties.giveProperties(p);
//    System.setProperty("LOG_DIR", LOG_DIR);
//    System.setProperty("REJECTED_DIR", REJECTED_DIR);
//    testInstance = new StorageTimeAction(dwhdb, dwhrep, Logger.getAnonymousLogger());
//  }
//
//@After
//public void after(){
//  testInstance = null;
//  DatabaseTestUtils.close(dwhdb_dba);
//}
//
//
//
//private Connection createConnection(String url, String user) throws Exception {
//  Connection con = null;
//  // Creating connection for rockfactory
//  Class.forName("org.hsqldb.jdbcDriver").newInstance();
//  con = DriverManager.getConnection(url, user, "");
//
//  return con;
//}
//
//private void setupProperties() throws Exception {
//  Properties properties = new Properties();
//  properties.put("dwhm.debug", "false");
//  StaticProperties.giveProperties(properties);
//}
//
//private void createMockedSanityChecker(String techPackName) {
//  String sanityCheckerName = techPackName + "_SanityChecker";
//  mockSanityChecker = context.mock(SanityChecker.class, sanityCheckerName);
//
//  context.checking(new Expectations() {
//
//    {
//      allowing(mockSanityChecker).sanityCheck(with(any(Dwhtype.class)));
//    }
//  });
//}
//
//
//private void updateMaxRowsPerPartition(Connection dwhRepCon, String partitionPlanName, long maxRowsToStorePerPartition)
//    throws SQLException {
//  Statement statement = dwhRepCon.createStatement();
//  try {
//    statement.execute("update partitionplan set defaultpartitionsize = " + maxRowsToStorePerPartition
//        + " where partitionplan = '" + partitionPlanName + "'");
//  } finally {
//    statement.close();
//  }
//}
//
//private void insertTestDataIntoDwhRep(Connection repRockCon, String techpackName, String partitionPlanName,
//    int partitionplanType, long defaultStorageTime, long maxStorageTime, String typeName, String storageId, final String techPackType)
//    throws SQLException {
//  Statement statement = repRockCon.createStatement();
//  try {
//    final String versionId = techpackName + ":b68";
//    statement.execute("INSERT INTO DWHTechPacks VALUES('" + techpackName + "','" + versionId +"','2009-11-30 18:46:02.0')");
//    statement.execute("insert into VERSIONING (VERSIONID, STATUS, TECHPACK_NAME, TECHPACK_TYPE) values (" +
//      "'"+versionId+"', '1', '"+techpackName+"', '"+techPackType+"');");
//
//    statement
//        .execute("INSERT INTO DWHType VALUES('"
//          + techpackName
//          + "', '"
//          + typeName
//          + "', 'RAW', '"
//          + storageId
//          + "',  -1,  18,  'ENABLED', 'PARTITIONED', 'dc',  'createview.vm','createpartition.vm','2010-03-21 00:00:00.0',"
//          + "'" + techpackName + "_SUCCESS_RAW','DATE_ID','createpublicview.vm','" + partitionPlanName + "')");
//
//    statement.execute("INSERT INTO Partitionplan VALUES('" + partitionPlanName + "'," + defaultStorageTime + ","
//      + maxStorageTime + "," + defaultStorageTime + "," + partitionplanType + ")");
//
//    statement.executeUpdate("insert into DWHColumn values ('" + storageId
//      + "','TEST_COLUMN123',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
//
//  } finally {
//    statement.close();
//  }
//}
//
//private void insertTestDataIntoDwhRep1(Connection repRockCon, String techpackName, String partitionPlanName,
//	      int partitionplanType, long defaultStorageTime, long maxStorageTime, String typeName, String storageId, final String techPackType)
//	      throws SQLException {
//	    Statement statement = repRockCon.createStatement();
//	    try {
//	      final String versionId = techpackName + ":b68";
//	      statement.execute("INSERT INTO DWHTechPacks VALUES('" + techpackName + "','" + versionId +"','2009-11-30 18:46:02.0')");
//	      statement.execute("insert into VERSIONING (VERSIONID, STATUS, TECHPACK_NAME, TECHPACK_TYPE) values (" +
//	        "'"+versionId+"', '1', '"+techpackName+"', '"+techPackType+"');");
//
//	      statement
//	          .execute("INSERT INTO DWHType VALUES('"
//	            + techpackName
//	            + "', '"
//	            + typeName
//	            + "', 'RAW', '"
//	            + storageId
//	            + "',  -1,  18,  'ENABLED', 'UNPARTITIONED', 'dc',  'createview.vm','createpartition.vm','2010-03-21 00:00:00.0',"
//	            + "'" + techpackName + "_SUCCESS_RAW','DATE_ID','createpublicview.vm','" + partitionPlanName + "')");
//
//	      statement.execute("INSERT INTO Partitionplan VALUES('" + partitionPlanName + "'," + defaultStorageTime + ","
//	        + maxStorageTime + "," + defaultStorageTime + "," + partitionplanType + ")");
//
//	      statement.executeUpdate("insert into DWHColumn values ('" + storageId
//	        + "','TEST_COLUMN123',28,'smallint',0,0,255,1,'LF',0,'ENABLED',1)");
//
//	    } finally {
//	      statement.close();
//	    }
//	  }
//
//private void insertTestDataIntoDwhRep(Connection repRockCon, String storageId, String typeName, int partitionCount)
//    throws SQLException {
//  Statement statement = repRockCon.createStatement();
//  try {
//    for (int count = 1; count <= partitionCount; count++) {
//      statement.execute("insert into CountingManagement values ('" + storageId + "', '" + typeName + "_RAW_"
//          + String.format("%02d", count) + "', 1000)");
//    }
//  } finally {
//    statement.close();
//  }
//}
//
//private void createExtraTablesForDwhRep(Connection repRockCon) throws SQLException {
//  Statement statement = repRockCon.createStatement();
//  try {
//    /*statement.execute("CREATE TABLE Partitionplan (PARTITIONPLAN VARCHAR(128),DEFAULTSTORAGETIME NUMERIC(9),"
//        + "DEFAULTPARTITIONSIZE NUMERIC(9),MAXSTORAGETIME NUMERIC(9),PARTITIONTYPE TINYINT)");*/
//    /*statement.execute("CREATE TABLE Externalstatement (VERSIONID varchar(100),STATEMENTNAME varchar(100),"
//        + "EXECUTIONORDER NUMERIC,DBCONNECTION varchar(100),STATEMENT varchar(100),BASEDEF varchar(100))");*/
//    /*statement.execute("create table CountingManagement (STORAGEID varchar(255), TABLENAME varchar(255), LASTAGGREGATEDROW integer)");*/
//    statement.execute("CREATE USER \"dwhrep\" PASSWORD \"dwhrep\" ADMIN;");
//
//  } finally {
//    statement.close();
//  }
//}
//
//
//private void createMockedActivationCache(final String techPackName, final long maxStorage) throws Exception {
//  String activationCacheName = techPackName + "_ActivationCache";
//  mockActivationCache = context.mock(ActivationCache.class, activationCacheName);
//
//  context.checking(new Expectations() {
//
//    {
//				allowing(mockActivationCache).revalidate();
//      allowing(mockActivationCache).isActive(techPackName);
//      will(returnValue(true));
//      allowing(mockActivationCache).isActive(with(any(String.class)), with(any(String.class)),
//          with(any(String.class)));
//      will(returnValue(true));
//      allowing(mockActivationCache).getStorageTime(with(any(String.class)), with(any(String.class)),
//          with(any(String.class)));
//      will(returnValue(maxStorage));
//    }
//  });
//}
//private class StubbedStorageTimeAction extends StorageTimeAction {
//
//  public StubbedStorageTimeAction(RockFactory reprock, RockFactory etlrock, RockFactory dwhrock,
//      RockFactory dbadwhrock, String techPack, Logger clog) throws Exception {
//    super(reprock, etlrock, dwhrock, dbadwhrock, techPack, clog);
//    // TODO Auto-generated constructor stub
//  }
//  
//  public StubbedStorageTimeAction(final RockFactory reprock, final RockFactory dwhrock,
//  		final Logger clog) throws Exception{
//  	super(reprock,  dwhrock, clog);
//  }
//
//  @Override
//  protected ActivationCache getActivationCache() {
//    return mockActivationCache;
//  }
//
//  @Override
//  protected String createView(RockFactory reprock, RockFactory dwhrock, Dwhtype type, TechPackType tpt) throws Exception {
//    return "TEST";
//  }
//
//  @Override
//  protected void handleBHviewCreation(final Measurementtype mt, final Busyhour bh) throws Exception {
//  	System.out.println("returnin from handleBHviewCreation");
//  	
//  }
//  
//  @Override
//  protected SanityChecker createSanityChecker(RockFactory reprock, RockFactory dwhrock, final boolean nvu) {
//    return mockSanityChecker;
//  }
//
//  @Override
//  protected void executeUpdateForPartition(final Connection con, final String sql) throws SQLException {
//  }
//
//  @Override
//  protected void sortPartitions(final short partitionPlanType, final Vector<Dwhpartition> partitions) {
//    super.sortPartitions(partitionPlanType, partitions);
//  }
//}
//
//  
//  @Test
//  public void deleteSQLFilesVectorTrue() throws Exception{
//  	
//  	String loadData = "This is a test";
//  	List<String> filesToDelete = new ArrayList<String>();
//  	String fileLocation = LOAD_DATA_DIR + "test" + File.separator;
//  	File newDir = new File(fileLocation);
//  	newDir.mkdir();
//  	
//  	String filename = "";
//  	for(int i = 0; i < 10; i ++){
//  		
//  		filename = LOAD_DATA_DIR + "test" + File.separator + "tempFile" + i;
//  		File loadFile = new File(filename);
//    	  	try {
//  	  		PrintWriter pw = new PrintWriter(new FileWriter(loadFile.getCanonicalFile()));
//  	  		pw.write(loadData);
//  	  		pw.close();
//  	  		pw.flush();
//    		
//    	  	} catch (FileNotFoundException e) {
//    		  fail("File not found error");
//    	  	} catch (IOException e) {
//    		  
//    	  	}	
//    	  	
//    	  filesToDelete.add(filename);
//  	}
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("filesToDelete");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, filesToDelete);
//  	
//  	final Method getLoadTableFile = StorageTimeAction.class.getDeclaredMethod("deleteSQLFilesVector", new Class[] {});
//  	getLoadTableFile.setAccessible(true);
//	  	getLoadTableFile.invoke(testInstance);	
//	  	
//	  	boolean actual = true;
//	  	
//	  	File dir = new File(fileLocation);
//	  	File dirList[] = dir.listFiles();
//	  	
//	  	String tempFileName = "";
//	  	String actualFile = "";
//	  	for (int i = 0; i < dirList.length; i ++){
//	  		tempFileName = LOAD_DATA_DIR + "test" + File.separator + "tempFile" + i;
//	  		actualFile = dirList[i].getCanonicalPath() + File.separator + dirList[i].getName();
//	  		System.out.println("tempFileName: " + tempFileName);
//	  		System.out.println("actualFile: " + actualFile);
//	  		if (!tempFileName.equals(actualFile)){
//	  			actual = false;
//	  		}
//	  	}
//  	
//	  	assertTrue(actual); 	  	
//  }
//  
//  @Test
//  public void testDeleteFilesWithFilesDelete() throws Exception{
//  	
//  	String loadData = "This is a test";
//  	String fileLocation = LOAD_DATA_DIR + "test" + File.separator;
//  	File newDir = new File(fileLocation);
//  	newDir.mkdir();
//  	
//  	String filename = "";
//  	for(int i = 0; i < 10; i ++){
//  		
//  		filename = LOAD_DATA_DIR + "test" + File.separator + "tempFile" + i;
//  		File loadFile = new File(filename);
//    	  	try {
//  	  		PrintWriter pw = new PrintWriter(new FileWriter(loadFile.getCanonicalFile()));
//  	  		pw.write(loadData);
//  	  		pw.close();
//  	  		pw.flush();
//    		
//    	  	} catch (FileNotFoundException e) {
//    		  fail("File not found error");
//    	  	} catch (IOException e) {
//    	  	}	
//    	}
//  	
//     	final Method deleteFiles = StorageTimeAction.class.getDeclaredMethod("deleteFiles", new Class[] {String.class, boolean.class});
//  	deleteFiles.setAccessible(true);
//  	deleteFiles.invoke(testInstance, new Object[] {fileLocation, false});	
//	  	
//	  	boolean actual = true;
//	  	
//	  	File dir = new File(fileLocation);
//	  		
//	  	if (dir.exists()){
//	  		
//	  		File[] files = dir.listFiles();
//	  		if(files.length>=0)
//	  		actual = true;
//	  	}
//	  	assertTrue(actual); 	  	
//  }
//  
//  @Test
//  public void testDeleteFilesWithFolderDelete() throws Exception{
//  	
//  	String loadData = "This is a test";
//  	String fileLocation = LOAD_DATA_DIR + "test" + File.separator;
//  	File newDir = new File(fileLocation);
//  	newDir.mkdir();
//  	
//  	String filename = "";
//  	for(int i = 0; i < 10; i ++){
//  		
//  		filename = LOAD_DATA_DIR + "test" + File.separator + "tempFile" + i;
//  		File loadFile = new File(filename);
//    	  	try {
//  	  		PrintWriter pw = new PrintWriter(new FileWriter(loadFile.getCanonicalFile()));
//  	  		pw.write(loadData);
//  	  		pw.close();
//  	  		pw.flush();
//    		
//    	  	} catch (FileNotFoundException e) {
//    		  fail("File not found error");
//    	  	} catch (IOException e) {
//    		  
//    	  	}	
//    	  	
//   	}
//  	
//     	final Method deleteFiles = StorageTimeAction.class.getDeclaredMethod("deleteFiles", new Class[] {String.class, boolean.class});
//  	deleteFiles.setAccessible(true);
//  	deleteFiles.invoke(testInstance, new Object[] {fileLocation, true});	
//	  	
//	  	boolean actual = true;
//	  	
//	  	File dir = new File(fileLocation);
//	  		
//	  	if (!dir.exists()){
//	  		actual = true;
//	  	}
//	  	assertTrue(actual); 	  	
//  }
//
//  
//  @Test
//  public void testCreateBhElemViews_LOG_BusyhourHistoryEmpty1() throws Exception {
//    String tpName = "DC_E_MGW";
//    String measType = "ELEM";
//    int viewsPerType = 4;
//    final String typeName = tpName + "_" + measType + "BH_RANKBH_" + measType;
//    final Measurementtype where = new Measurementtype(dwhrep);
//    where.setTypename(tpName + "_" + measType + "BH");
//    final MeasurementtypeFactory fac = new MeasurementtypeFactory(dwhrep, where);
//    final List<Measurementtype> types = fac.get();
//    assertFalse("No Measurementtypes found, setup not correct ", types.isEmpty());
//    final Measurementtype typeToRecreate = types.get(0);
//    final Busyhour bh = new Busyhour(dwhrep);
//
//    final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//    final Connection c = createNiceMock(Connection.class);
//    final Statement s = createMock(Statement.class);
//
//    final ResultSet resultSetMock = createMock(ResultSet.class);
//
//    expect(mDwhdb.getConnection()).andReturn(c).anyTimes();
//
//    expect(c.createStatement()).andReturn(s).anyTimes();
//    expect(
//        s.execute(matches("insert into LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName + "_CP[0-4];.*create view "
//            + typeName + "_CP[0-4].*(?s)"))).andReturn(true).times(viewsPerType);
//    expect(
//        s.execute(matches("insert into LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName + "_PP[0-4];.*create view "
//            + typeName + "_PP[0-4].*(?s)"))).andReturn(true).times(viewsPerType);
//    expect(
//        s.execute(matches("(?s).* .*DROP VIEW " + typeName + "_CP[0-" + viewsPerType + "];.*create view " + typeName
//            + "_CP[0-4].*;"))).andReturn(true).times(viewsPerType);
//    expect(
//        s.execute(matches("(?s).* .*DROP VIEW " + typeName + "_PP[0-" + viewsPerType + "];.*create view " + typeName
//            + "_PP[0-4].*;"))).andReturn(true).times(viewsPerType);
//    expect(
//        s.executeQuery(matches("select id from LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName
//            + "_CP[0-4];.*create view " + typeName + "_CP[0-4].*(?s)"))).andReturn(resultSetMock).times(viewsPerType);
//    expect(
//        s.executeQuery(matches("select id from LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName
//            + "_PP[0-4];.*create view " + typeName + "_PP[0-4].*(?s)"))).andReturn(resultSetMock).times(viewsPerType);
//    expect(s.executeQuery("select max(id) as maxId from LOG_BusyhourHistory")).andReturn(resultSetMock).anyTimes();
//
//    expect(s.getMoreResults()).andReturn(false).anyTimes();
//
//    mockResultSets(resultSetMock, false);
//    
//    resultSetMock.close();
//    expectLastCall().atLeastOnce();
//
//    s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
//    expectLastCall().anyTimes();
//
//    
//    s.close();
//    expectLastCall().atLeastOnce();
//    replay(s);
//    replay(c);
//    replay(resultSetMock);
//    replay(mDwhdb);
//    
//      final StorageTimeAction mockedInstance = new StubbedStorageTimeAction(mDwhdb, dwhrep, Logger.getAnonymousLogger());
//     mockedInstance.createBhRankViews(bh); 
//
//  }
//  
//  @Test
//  public void getLoadTableFileVectorCounter() throws Exception {
//  	final String techpackName = "DC_E_RBS";
//  	final String userName = "SA.";
//  	final String tableName = "DIM_E_RBS_CARRIER_V";
//  	final String typeName = "DC_E_RBS_CARRIER_V";
//  	final String typeID = "DC_E_RBS:((120)):" + typeName;
//  	final String dataName = "pmAverageRssi";
//  	final String vendorRelease = "P7FP";
//  	final Long vIndex = 11L;
//  	final String testFileName = LOAD_DATA_DIR + tableName + "_" +dataName;
//  	final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
//
//  	final Measurementvector mv = new Measurementvector(dwhrep, typeID, dataName, vendorRelease, vIndex);
//      
//  	
//  	final StringBuilder expected = new StringBuilder();
//  	expected.append("Truncate table "); expected.append(userName); expected.append(tableName); expected.append("_"); expected.append(dataName); expected.append(";\n\n");
//  	expected.append("LOAD TABLE "); expected.append(userName); expected.append(tableName); expected.append("_"); expected.append(dataName); expected.append("\n");
//  	expected.append("(" + dataName + "_DCVECTOR, " + dataName + "_VALUE, DC_RELEASE)\n");
//  	expected.append("FROM '" + testFileName + "'\n");
//  	expected.append("ESCAPES OFF\n");
//  	expected.append("QUOTES OFF\n");
//  	expected.append("DELIMITED BY '\\x09'\n");
//  	expected.append("ROW DELIMITED BY '\\x0a'\n");
//  	expected.append("WITH CHECKPOINT ON\n");
//  	expected.append(";");
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getLoadTableFile = StorageTimeAction.class.getDeclaredMethod("getLoadTableFile", new Class[] {String.class, String.class, String.class, String.class});
//  	getLoadTableFile.setAccessible(true);
//	  	final String actual = (String)getLoadTableFile.invoke(testInstance, techpackName, dataName, typeName, testFileName);	
//	  	  	  	
//	  	assertEquals(expected.toString(), actual);  	
//  }
//  
//  @Test
//  public void getLoadTableFilePmResCounter() throws Exception {
//  	final String techpackName = "DC_E_RNC";
//  	final String userName = "SA.";
//  	final String tableName = "DIM_E_RAN_UCELL_V_PMRES";
//  	final String typeName = "DC_E_RAN_UCELL_V_PMRES";
//  	final String typeID = "DC_E_RAN:((124)):" + typeName;
//  	final String dataName = "pmRes1";
//  	final String vendorRelease = "P5";
//  	final Long vIndex = 1L;
//  	final String testFileName = LOAD_DATA_DIR + tableName + "_"+ dataName;
//  	final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
//
//  	final Measurementvector mv = new Measurementvector(dwhrep, typeID, dataName, vendorRelease, vIndex);
//      
//  	
//  	final StringBuilder expected = new StringBuilder();
//  	expected.append("Truncate table "); expected.append(userName); expected.append(tableName); expected.append("_"); expected.append(dataName); expected.append(";\n\n");
//  	expected.append("LOAD TABLE "); expected.append(userName); expected.append(tableName); expected.append("_"); expected.append(dataName); expected.append("\n");
//  	expected.append("(" + dataName + "_DCVECTOR, " + dataName + "_VALUE, DC_RELEASE, QUANTITY)\n");
//  	expected.append("FROM '" + testFileName + "'\n");
//  	expected.append("ESCAPES OFF\n");
//  	expected.append("QUOTES OFF\n");
//  	expected.append("DELIMITED BY '\\x09'\n");
//  	expected.append("ROW DELIMITED BY '\\x0a'\n");
//  	expected.append("WITH CHECKPOINT ON\n");
//  	expected.append(";");
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	    
//  	final Method getLoadTableFile = StorageTimeAction.class.getDeclaredMethod("getLoadTableFile", new Class[] {String.class, String.class, String.class, String.class});
//  	getLoadTableFile.setAccessible(true);
//	  	final String actual = (String)getLoadTableFile.invoke(testInstance, techpackName, dataName, typeName, testFileName);
//	  	
//	  	assertEquals(expected.toString(), actual);  	
//  }
//  
//  @Test
//  public void setLoadDataVectorCounter() throws Exception {
//  	
//  	final String typeID = "DC_E_RBS:((120)):DC_E_RBS_CARRIER_V";
//  	final String dimTable = "DIM_E_RBS_CARRIER_V_pmAverageRssi";
//  	final String dataName = "pmAverageRssi";
//  	final String testFileName = LOAD_DATA_DIR + dimTable + "test" + FILE_EXTENSION;
//
//  	               
//  	final StringBuffer expected = new StringBuffer();
//  	expected.append("11\t-105,0 - -104,5 dBm\tP7FP\t\n");
//  	expected.append("12\t-104,5 - -104,0 dBm\tP7FP\t\n");
//  	expected.append("13\t-104,0 - -103,5 dBm\tP7FP\t\n");
//  	expected.append("14\t-103,5 - -103,0 dBm\tP7FP\t\n");
//  	expected.append("15\t-103,0 - -102,5 dBm\tP7FP\t\n");
//  	expected.append("16\t-102,5 - -102,0 dBm\tP7FP\t\n");
//  	expected.append("17\t-102,0 - -101,5 dBm\tP7FP\t\n");
//  	expected.append("18\t-101,5 - -101,0 dBm\tP7FP\t\n");
//  	expected.append("19\t-101,0 - -100,5 dBm\tP7FP\t\n");
//  	expected.append("20\t-100,5 - -100,0 dBm\tP7FP\t\n");
//  	expected.append("21\t-100,0 - -99,5 dBm\tP7FP\t\n");
//  	expected.append("22\t-99,5 - -99,0 dBm\tP7FP\t\n");
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method setLoadData = StorageTimeAction.class.getDeclaredMethod("setLoadData", new Class[] {String.class, String.class, String.class, String.class});
//  	setLoadData.setAccessible(true);
//	  	final String fileName = (String)setLoadData.invoke(testInstance, typeID, dimTable, dataName, testFileName);
//	  	
//	  	final File testFile = new File(fileName);
//	    final StringBuffer actual = new StringBuffer();
//	    final BufferedReader br = new BufferedReader(new FileReader(testFile.getCanonicalFile()));
//	    char[] buffer = new char[1024];
//	    int numRead = 0;
//	  	
//	    while ((numRead=br.read(buffer)) != -1){
//	  		actual.append(String.valueOf(buffer, 0, numRead));
//	  		buffer = new char[1024];
//	  	}
//	  	br.close();
//	  
//	    testFile.delete();
//	  	assertEquals(expected.toString(), actual.toString());
//	  	
//  }
//  
//  @Test
//  public void setLoadDataPmResVectorCounter() throws Exception {
//  	
//  	final String typeID = "DC_E_RAN:((124)):DC_E_RAN_UCELL_V_PMRES";
//  	final String dimTable = "DIM_E_RAN_UCELL_V_PMRES_pmRes1";
//  	final String dataName = "pmRes1";
//  	final String testFileName = LOAD_DATA_DIR + dimTable + "test" + FILE_EXTENSION;
//
//  	            	
//  	final StringBuffer expected = new StringBuffer();
//  	expected.append("1\t1 - < 1.5 %\tP5\t1\t\n");
//  	expected.append("2\t1.5 - < 2 %\tP5\t1\t\n");
//  	expected.append("3\t2 - < 2.5 %\tP5\t1\t\n");
//  	expected.append("4\t2.5 - < 3 %\tP5\t1\t\n");
//  	expected.append("5\t3 - < 3.5 %\tP5\t1\t\n");
//  	expected.append("6\t3.5 - < 4 %\tP5\t1\t\n");
//  	expected.append("7\t4 - < 4.5 %\tP5\t1\t\n");
//  	expected.append("8\t4.5 - < 5 %\tP5\t1\t\n");
//  	expected.append("9\t5 - < 5.5 %\tP5\t1\t\n");
//  	expected.append("10\t5.5 - < 6 %\tP5\t1\t\n");
//  	expected.append("11\t6 - < 6.5 %\tP5\t1\t\n");
//  	expected.append("12\t6.5 - < 7 %\tP5\t1\t\n");
//  	expected.append("13\t7 - < 7.5 %\tP5\t1\t\n");
//  	expected.append("14\t7.5 - < 8 %\tP5\t1\t\n");
//  	
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method setLoadData = StorageTimeAction.class.getDeclaredMethod("setLoadData", new Class[] {String.class, String.class, String.class, String.class});
//  	setLoadData.setAccessible(true);
//	  	final String fileName = (String)setLoadData.invoke(testInstance, typeID, dimTable, dataName, testFileName);
//	  	
//	    final File testFile = new File(fileName);
//	    final StringBuffer actual = new StringBuffer();
//	    final BufferedReader br = new BufferedReader(new FileReader(testFile.getCanonicalFile()));
//	    char[] buffer = new char[1024];
//	    int numRead = 0;
//	  	while ((numRead=br.read(buffer)) != -1){
//	  		actual.append(String.valueOf(buffer, 0, numRead));
//	  		buffer = new char[1024];
//	  	}
//	  	br.close();
//	  	testFile.delete();
//	  	
//	  	assertEquals(expected.toString(), actual.toString());
//  }
//  
//  @Test    
//  public void getTableName() throws Exception {
//  	final String typeName = "DC_E_RAN_UCELL_V_PMRES";
//  	final String dataName = "pmRes1";
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getDimTableName = StorageTimeAction.class.getDeclaredMethod("getDimTableName", new Class[] {String.class, String.class});
//  	getDimTableName.setAccessible(true);
//	  	final String actual = (String)getDimTableName.invoke(testInstance, dataName, typeName);
//	  	
//	    final String expected = "DIM_E_RAN_UCELL_V_PMRES_pmRes1";
//	    
//	    
//	    assertEquals(expected, actual);
//  }
//  
//  @Test    
//  public void getTableNameEmptyTypeName() throws Exception {
//  	final String typeName = "DC_E_RAN_UCELL_V_PMRES";
//  	final String dataName = "";
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getDimTableName = StorageTimeAction.class.getDeclaredMethod("getDimTableName", new Class[] {String.class, String.class});
//  	getDimTableName.setAccessible(true);
//	  	final String actual = (String)getDimTableName.invoke(testInstance, dataName, typeName);
//	  	
//	    final String expected = "";
//	      	    
//	    assertEquals(expected, actual);
//  }
//  
//  @Test    
//  public void getTableNameEmptyTypeNameAndDataName() throws Exception {
//  	final String typeName = "";
//  	final String dataName = "";
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getDimTableName = StorageTimeAction.class.getDeclaredMethod("getDimTableName", new Class[] {String.class, String.class});
//  	getDimTableName.setAccessible(true);
//	  	final String actual = (String)getDimTableName.invoke(testInstance, dataName, typeName);
//	  	
//	    final String expected = "";
//	      	    
//	    assertEquals(expected, actual);
//  }
//  
//  @Test    
//  public void getTableNameNullTypeNameAndDataName() throws Exception {
//  	final String typeName = null;
//  	final String dataName = null;
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getDimTableName = StorageTimeAction.class.getDeclaredMethod("getDimTableName", new Class[] {String.class, String.class});
//  	getDimTableName.setAccessible(true);
//	  	final String actual = (String)getDimTableName.invoke(testInstance, dataName, typeName);
//	  	
//	    final String expected = "";
//	      	    
//	    assertEquals(expected, actual);
//  }
//  
//  
//  @Test    
//  public void getTableNameEmptyDataName() throws Exception {
//  	final String typeName = "";
//  	final String dataName = "pmRes1";
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getDimTableName = StorageTimeAction.class.getDeclaredMethod("getDimTableName", new Class[] {String.class, String.class});
//  	getDimTableName.setAccessible(true);
//	  	final String actual = (String)getDimTableName.invoke(testInstance, dataName, typeName);
//	  	
//	    final String expected = "";
//	      	    
//	    assertEquals(expected, actual);
//  }
//  
//  @Test
//  public void getVectorLoadTableInfoPMRes() throws Exception {
//  	
//  	final String techpackName = "DC_E_RAN";
//  	final String versionid="DC_E_RAN:((124))";
//  	final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
//  	final String testFileName1 = LOAD_DATA_DIR + "DIM_E_RAN_UCELL_V_PMRES_pmRes1" + FILE_EXTENSION;
//  	final String testFileName2 = LOAD_DATA_DIR + "DIM_E_RAN_UCELL_V_PMRES_pmRes2" + FILE_EXTENSION;
//  	
//  	final StringBuffer expected = new StringBuffer();
//  	expected.append("Truncate table SA.DIM_E_RAN_UCELL_V_PMRES_pmRes1;\n\n");
//  	expected.append("LOAD TABLE SA.DIM_E_RAN_UCELL_V_PMRES_pmRes1\n");
//  	expected.append("(pmRes1_DCVECTOR, pmRes1_VALUE, DC_RELEASE, QUANTITY)\n");
//  	expected.append("FROM '" + testFileName1 + "'\n");
//  	expected.append("ESCAPES OFF\n");
//  	expected.append("QUOTES OFF\n");
//  	expected.append("DELIMITED BY '\\x09'\n");
//  	expected.append("ROW DELIMITED BY '\\x0a'\n");
//  	expected.append("WITH CHECKPOINT ON\n");
//  	expected.append(";");
//  	
//  	expected.append("Truncate table SA.DIM_E_RAN_UCELL_V_PMRES_pmRes2;\n\n");
//   	expected.append("LOAD TABLE SA.DIM_E_RAN_UCELL_V_PMRES_pmRes2\n");
//  	expected.append("(pmRes2_DCVECTOR, pmRes2_VALUE, DC_RELEASE, QUANTITY)\n");
//  	expected.append("FROM '" + testFileName2 + "'\n");
//  	expected.append("ESCAPES OFF\n");
//  	expected.append("QUOTES OFF\n");
//  	expected.append("DELIMITED BY '\\x09'\n");
//  	expected.append("ROW DELIMITED BY '\\x0a'\n");
//  	expected.append("WITH CHECKPOINT ON\n");
//  	expected.append(";");
//  	
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getVectorLoadTableInfo = StorageTimeAction.class.getDeclaredMethod("getVectorLoadTableInfo", new Class[] {String.class,String.class});
//  	getVectorLoadTableInfo.setAccessible(true);
//	  	final List<String> loadFileList = (List<String>)getVectorLoadTableInfo.invoke(testInstance, techpackName,versionid);
//	  	
//	  	final Iterator<String> iter = loadFileList.iterator();
//	  	
//	  	final StringBuffer actual = new StringBuffer();
//	  	
//	  	while (iter.hasNext()){
//	  		actual.append(iter.next());
//	  	}
//	  	
//	  	File testFile1 = new File(testFileName1);
//	  	File testFile2 = new File(testFileName2);
//	  	
//	  	testFile1.delete();
//	  	testFile2.delete();
//	  	
//	  	assertEquals(expected.toString(), actual.toString());
//  }
//  
//  @Test
//  public void getVectorLoadTableInfo() throws Exception {
//  	
//  	final String techpackName = "DC_E_RBS";
//  	final String versionid = "DC_E_RBS:((120))";
//  	final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
//  	final String testFileName1 = LOAD_DATA_DIR + "DIM_E_RBS_CARRIER_V_pmAverageRssi" + FILE_EXTENSION;
//  	final String testFileName2 = LOAD_DATA_DIR + "DIM_E_RBS_CARRIER_V_pmAverageRssi2" + FILE_EXTENSION;
//  	
//  	final StringBuffer expected = new StringBuffer();
//  	expected.append("Truncate table SA.DIM_E_RBS_CARRIER_V_pmAverageRssi;\n\n");
//   	expected.append("LOAD TABLE SA.DIM_E_RBS_CARRIER_V_pmAverageRssi\n");
//  	expected.append("(pmAverageRssi_DCVECTOR, pmAverageRssi_VALUE, DC_RELEASE)\n");
//  	expected.append("FROM '" + testFileName1 + "'\n");
//  	expected.append("ESCAPES OFF\n");
//  	expected.append("QUOTES OFF\n");
//  	expected.append("DELIMITED BY '\\x09'\n");
//  	expected.append("ROW DELIMITED BY '\\x0a'\n");
//  	expected.append("WITH CHECKPOINT ON\n");
//  	expected.append(";");
//  	
//  	expected.append("Truncate table SA.DIM_E_RBS_CARRIER_V_pmAverageRssi2;\n\n");
//  	expected.append("LOAD TABLE SA.DIM_E_RBS_CARRIER_V_pmAverageRssi2\n");
//  	expected.append("(pmAverageRssi2_DCVECTOR, pmAverageRssi2_VALUE, DC_RELEASE)\n");
//  	expected.append("FROM '" + testFileName2 + "'\n");
//  	expected.append("ESCAPES OFF\n");
//  	expected.append("QUOTES OFF\n");
//  	expected.append("DELIMITED BY '\\x09'\n");
//  	expected.append("ROW DELIMITED BY '\\x0a'\n");
//  	expected.append("WITH CHECKPOINT ON\n");
//  	expected.append(";");
//  	
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	//final Method getVectorLoadTableInfo = StorageTimeAction.class.getDeclaredMethod("getVectorLoadTableInfo", new Class[] {String.class});
//  	final Method getVectorLoadTableInfo = StorageTimeAction.class.getDeclaredMethod("getVectorLoadTableInfo",new Class[] {String.class, String.class});
//  	getVectorLoadTableInfo.setAccessible(true);
//		final List<String> loadFileList = (List<String>)getVectorLoadTableInfo.invoke(testInstance,techpackName,versionid);
//	  	
//	    final Iterator<String> iter = loadFileList.iterator();
//	  	
//	    final StringBuffer actual = new StringBuffer();
//	  	
//	  	while (iter.hasNext()){
//	  		actual.append(iter.next());
//	  	}
//	  	
//	    File testFile1 = new File(testFileName1);
//	  	File testFile2 = new File(testFileName2);
//	  	
//	  	testFile1.delete();
//	  	testFile2.delete();
//	  	
//	  	assertEquals(expected.toString(), actual.toString());
//  }
//  
//  @Test
//  public void getVectorLoadTableInfoCheckFileVector() throws Exception {
//  	final String techpackName = "DC_E_RBS";
//  	final String versionid="DC_E_RBS:((120))";
//  	final String testFileName = LOAD_DATA_DIR + "DIM_E_RBS_CARRIER_V_pmAverageRssi" + FILE_EXTENSION;
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getVectorLoadTableInfo = StorageTimeAction.class.getDeclaredMethod("getVectorLoadTableInfo", new Class[] {String.class,String.class});
//  	getVectorLoadTableInfo.setAccessible(true);
//	  	getVectorLoadTableInfo.invoke(testInstance, techpackName,versionid);
//	  	
//	  	final StringBuffer expected = new StringBuffer();
//	  	expected.append("11\t-105,0 - -104,5 dBm\tP7FP\t\n");
//	  	expected.append("12\t-104,5 - -104,0 dBm\tP7FP\t\n");
//	  	expected.append("13\t-104,0 - -103,5 dBm\tP7FP\t\n");
//	  	expected.append("14\t-103,5 - -103,0 dBm\tP7FP\t\n");
//	  	expected.append("15\t-103,0 - -102,5 dBm\tP7FP\t\n");
//	  	expected.append("16\t-102,5 - -102,0 dBm\tP7FP\t\n");
//	  	expected.append("17\t-102,0 - -101,5 dBm\tP7FP\t\n");
//	  	expected.append("18\t-101,5 - -101,0 dBm\tP7FP\t\n");
//	  	expected.append("19\t-101,0 - -100,5 dBm\tP7FP\t\n");
//	  	expected.append("20\t-100,5 - -100,0 dBm\tP7FP\t\n");
//	  	expected.append("21\t-100,0 - -99,5 dBm\tP7FP\t\n");
//	  	expected.append("22\t-99,5 - -99,0 dBm\tP7FP\t\n"); 	
//	  	
//	  	final File testFile = new File(testFileName);
//	    final StringBuffer actualFile = new StringBuffer();
//	    final BufferedReader brFile = new BufferedReader(new FileReader(testFile.getCanonicalFile()));
//	    char[] bufferFile = new char[1024];
//	    int numReadFile = 0;
//	  	while ((numReadFile=brFile.read(bufferFile)) != -1){
//	  		actualFile.append(String.valueOf(bufferFile, 0, numReadFile));
//	  		bufferFile = new char[1024];
//	  	}
//	  	brFile.close();	  	  	
//	  	testFile.delete();
//	  	
//	  	assertEquals(expected.toString(), actualFile.toString());
//  }
//  
//  @Test
//  public void getVectorLoadTableInfoCheckFileVectorSecondFile() throws Exception {
//  	final String techpackName = "DC_E_RBS";
//  	final String versionid = "DC_E_RBS:((120))";
//  	final String testFileName = LOAD_DATA_DIR + "DIM_E_RBS_CARRIER_V_pmAverageRssi2" + FILE_EXTENSION;
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getVectorLoadTableInfo = StorageTimeAction.class.getDeclaredMethod("getVectorLoadTableInfo", new Class[] {String.class,String.class});
//  	getVectorLoadTableInfo.setAccessible(true);
//	  	getVectorLoadTableInfo.invoke(testInstance, techpackName,versionid);
//	  	
//	  	final StringBuffer expected = new StringBuffer();
//	  	expected.append("11\t-105,0 - -104,5 dBm\tP8FP\t\n");
//	  	expected.append("12\t-104,5 - -104,0 dBm\tP8FP\t\n");
//	  	expected.append("13\t-104,0 - -103,5 dBm\tP8FP\t\n");
//	  	expected.append("14\t-103,5 - -103,0 dBm\tP8FP\t\n");
//	  	expected.append("15\t-103,0 - -102,5 dBm\tP8FP\t\n");
//	  	expected.append("16\t-102,5 - -102,0 dBm\tP8FP\t\n");
//	  	expected.append("17\t-102,0 - -101,5 dBm\tP8FP\t\n");
//	  	expected.append("18\t-101,5 - -101,0 dBm\tP8FP\t\n");
//	  	expected.append("19\t-101,0 - -100,5 dBm\tP8FP\t\n");
//	  	expected.append("20\t-100,5 - -100,0 dBm\tP8FP\t\n");
//	  	expected.append("21\t-100,0 - -99,5 dBm\tP8FP\t\n");
//	  	expected.append("22\t-99,5 - -99,0 dBm\tP8FP\t\n");	  		  	
//	  	
//	  	final File testFile = new File(testFileName);
//	    final StringBuffer actualFile = new StringBuffer();
//	    final BufferedReader brFile = new BufferedReader(new FileReader(testFile.getCanonicalFile()));
//	    char[] bufferFile = new char[1024];
//	    int numReadFile = 0;
//	  	while ((numReadFile=brFile.read(bufferFile)) != -1){
//	  		actualFile.append(String.valueOf(bufferFile, 0, numReadFile));
//	  		bufferFile = new char[1024];
//	  	}
//	  	brFile.close();	  	  	
//	  	testFile.delete();
//	  	
//	  	assertEquals(expected.toString(), actualFile.toString());  
//  }
//  	
//  @Test
//  public void getVectorLoadTableInfoCheckFilePMResVector() throws Exception {
//  	final String techpackName = "DC_E_RAN";
//  	final String versionid = "DC_E_RAN:((124))";
//  	final String testFileName = LOAD_DATA_DIR + "DIM_E_RAN_UCELL_V_PMRES_pmRes1" + FILE_EXTENSION;
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getVectorLoadTableInfo = StorageTimeAction.class.getDeclaredMethod("getVectorLoadTableInfo", new Class[] {String.class,String.class});
//  	getVectorLoadTableInfo.setAccessible(true);
//	  	getVectorLoadTableInfo.invoke(testInstance, techpackName,versionid);
//	  	
//	  	final StringBuffer expected = new StringBuffer();
//	  	expected.append("1\t1 - < 1.5 %\tP5\t1\t\n");
//	  	expected.append("2\t1.5 - < 2 %\tP5\t1\t\n");
//	  	expected.append("3\t2 - < 2.5 %\tP5\t1\t\n");
//	  	expected.append("4\t2.5 - < 3 %\tP5\t1\t\n");
//	  	expected.append("5\t3 - < 3.5 %\tP5\t1\t\n");
//	  	expected.append("6\t3.5 - < 4 %\tP5\t1\t\n");
//	  	expected.append("7\t4 - < 4.5 %\tP5\t1\t\n");
//	  	expected.append("8\t4.5 - < 5 %\tP5\t1\t\n");
//	  	expected.append("9\t5 - < 5.5 %\tP5\t1\t\n");
//	  	expected.append("10\t5.5 - < 6 %\tP5\t1\t\n");
//	  	expected.append("11\t6 - < 6.5 %\tP5\t1\t\n");
//	  	expected.append("12\t6.5 - < 7 %\tP5\t1\t\n");
//	  	expected.append("13\t7 - < 7.5 %\tP5\t1\t\n");
//	  	expected.append("14\t7.5 - < 8 %\tP5\t1\t\n");	
//	  	
//	  	final File testFile = new File(testFileName);
//	    final StringBuffer actualFile = new StringBuffer();
//	    final BufferedReader brFile = new BufferedReader(new FileReader(testFile.getCanonicalFile()));
//	    char[] bufferFile = new char[1024];
//	    int numReadFile = 0;
//	  	while ((numReadFile=brFile.read(bufferFile)) != -1){
//	  		actualFile.append(String.valueOf(bufferFile, 0, numReadFile));
//	  		bufferFile = new char[1024];
//	  	}
//	  	brFile.close();	  	  	
//	  	testFile.delete();
//	  	
//	  	assertEquals(expected.toString(), actualFile.toString());
//  }
//  
//  @Test
//  public void getVectorLoadTableInfoCheckFilePMResVectorSecondFile() throws Exception {
//  	final String techpackName = "DC_E_RAN";
//  	final String versionid = "DC_E_RAN:((124))";
//  	final String testFileName = LOAD_DATA_DIR +"DIM_E_RAN_UCELL_V_PMRES_pmRes2" + FILE_EXTENSION;
//
//  	
//  	final Field loadDataLoc = StorageTimeAction.class.getDeclaredField("LOAD_FILE_LOC");
//  	loadDataLoc.setAccessible(true);
//  	loadDataLoc.set(testInstance, LOAD_DATA_DIR);
//  	
//  	final Method getVectorLoadTableInfo = StorageTimeAction.class.getDeclaredMethod("getVectorLoadTableInfo", new Class[] {String.class,String.class});
//  	getVectorLoadTableInfo.setAccessible(true);
//	  	getVectorLoadTableInfo.invoke(testInstance, techpackName,versionid);
//	  	
//	  	final StringBuffer expected = new StringBuffer();
//	  	expected.append("1\t1 - < 1.5 %\tP6\t1\t\n");
//	  	expected.append("2\t1.5 - < 2 %\tP6\t1\t\n");
//	  	expected.append("3\t2 - < 2.5 %\tP6\t1\t\n");
//	  	expected.append("4\t2.5 - < 3 %\tP6\t1\t\n");
//	  	expected.append("5\t3 - < 3.5 %\tP6\t1\t\n");
//	  	expected.append("6\t3.5 - < 4 %\tP6\t1\t\n");
//	  	expected.append("7\t4 - < 4.5 %\tP6\t1\t\n");
//	  	expected.append("8\t4.5 - < 5 %\tP6\t1\t\n");
//	  	expected.append("9\t5 - < 5.5 %\tP6\t1\t\n");
//	  	expected.append("10\t5.5 - < 6 %\tP6\t1\t\n");
//	  	expected.append("11\t6 - < 6.5 %\tP6\t1\t\n");
//	  	expected.append("12\t6.5 - < 7 %\tP6\t1\t\n");
//	  	expected.append("13\t7 - < 7.5 %\tP6\t1\t\n");
//	  	expected.append("14\t7.5 - < 8 %\tP6\t1\t\n");	
//	  	
//	  	final File testFile = new File(testFileName);
//	    final StringBuffer actualFile = new StringBuffer();
//	    final BufferedReader brFile = new BufferedReader(new FileReader(testFile.getCanonicalFile()));
//	    char[] bufferFile = new char[1024];
//	    int numReadFile = 0;
//	  	while ((numReadFile=brFile.read(bufferFile)) != -1){
//	  		actualFile.append(String.valueOf(bufferFile, 0, numReadFile));
//	  		bufferFile = new char[1024];
//	  	}
//	  	brFile.close();	  	  	
//	  	testFile.delete();
//	  	
//	  	assertEquals(expected.toString(), actualFile.toString());
//	}        	
//  
//  @Test
//  public void testGetDwhtypes() throws Exception {
//  	final String techpack = "DC_E_MGW";
//		assertNotNull("StorageTimeAction instance should not be null", testInstance);
//		final Vector<Dwhtype> types = testInstance.getDwhtypes(techpack);
//		final Vector<Dwhtype> typesEnabled = testInstance.getDwhtypes(techpack, "ENABLED");
//		assertEquals("By default should select same as 'ENABLED'", typesEnabled.size(), types.size());
//  } // testGetDwhtypes
//  
//  @Test
//  public void testGetDwhtypesStatus() throws Exception {
//  	final String techpack = "DC_E_MGW";
//		assertNotNull("StorageTimeAction instance should not be null", testInstance);
//		final Vector<Dwhtype> typesAll = testInstance.getDwhtypes(techpack, null);
//		final int expectedAll = 11; // select * from dwhtype
//		assertEquals("select * from dwhtype", expectedAll, typesAll.size());
//		final Vector<Dwhtype> typesObsolete = testInstance.getDwhtypes(techpack, "OBSOLETE");
//		final int expectedObsolete = 3; // select * from dwhtype where status='OBSOLETE'
//		assertEquals("select * from dwhtype where status='OBSOLETE'", expectedObsolete, typesObsolete.size());
//		final Vector<Dwhtype> typesEnabled = testInstance.getDwhtypes(techpack, "ENABLED");
//		final int expectedEnabled = 8; // select * from dwhtype where status='ENABLED'
//		assertEquals("select * from dwhtype where status='ENABLED'", expectedEnabled, typesEnabled.size());
//  } // testGetDwhtypesStatus
//
//  /**
//   * Default setup has 4 endabled Placeholders & 1 disabled
//   */
//  @Test
//  public void testCreateBhElemViews() throws Exception {
//      doCreateViews("DC_E_MGW", "ELEM", 4, false);
//  }
//
//  @Test
//  public void testCreateObjViews() throws Exception {
//      doCreateViews("DC_E_MGW", "ATMPORT", 4, false);
//  }
//
//  @Test
//  public void testCreateBHViews(){
////  	final RockFactory mDwhdb = context.mock(RockFactory.class, "rockFactory");
////  	final Dwhtechpacks dwhtechpacksMock = context.mock(Dwhtechpacks.class, "dwhTechpacks");
//    final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//    final Dwhtechpacks dwhtechpacksMock = createNiceMock(Dwhtechpacks.class);
//    
////    //setup the expected calls
////  	try{
////  		context.checking(new Expectations(){{
////  			one(dwhtechpacksMock).getTechpack_name();
////  			will(returnValue("TEST"));
////  			
////  			one(dwhtechpacksMock).getVersionid();
////  			will(returnValue("TEST:((101))"));
////  		}});
////  	}catch(Exception e){
////  		fail(e.getMessage());
////  	}
//    expect(dwhtechpacksMock.getTechpack_name()).andReturn("TEST");
//    expect(dwhtechpacksMock.getVersionid()).andReturn("TEST:((101))");
//    replay(dwhtechpacksMock);
//    replay(mDwhdb);
//    
//   try {
//    testInstance.createBHViews(dwhtechpacksMock);
//  } catch (Exception e) {
//    fail("Exception Thrown" + e.getMessage());
//  }
//  }
//  
//  @Test
//public void testCreateBhElemViews_LOG_BusyhourHistoryEmpty() throws Exception {
//  String tpName = "DC_E_MGW";
//  String measType = "ELEM";
//  int viewsPerType = 4;
//  final String typeName = tpName + "_" + measType + "BH_RANKBH_" + measType;
//  final Measurementtype where = new Measurementtype(dwhrep);
//  where.setTypename(tpName + "_" + measType + "BH");
//  final MeasurementtypeFactory fac = new MeasurementtypeFactory(dwhrep, where);
//  final List<Measurementtype> types = fac.get();
//  assertFalse("No Measurementtypes found, setup not correct ", types.isEmpty());
//  final Measurementtype typeToRecreate = types.get(0);
//
//  final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//  final Connection c = createNiceMock(Connection.class);
//  final Statement s = createMock(Statement.class);
//
//  final ResultSet resultSetMock = createMock(ResultSet.class);
//
//  expect(mDwhdb.getConnection()).andReturn(c).anyTimes();
//
//  expect(c.createStatement()).andReturn(s).anyTimes();
//  expect(
//      s.execute(matches("insert into LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName + "_CP[0-4];.*create view "
//          + typeName + "_CP[0-4].*(?s)"))).andReturn(true).times(viewsPerType);
//  expect(
//      s.execute(matches("insert into LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName + "_PP[0-4];.*create view "
//          + typeName + "_PP[0-4].*(?s)"))).andReturn(true).times(viewsPerType);
//  expect(
//      s.execute(matches("(?s).* .*DROP VIEW " + typeName + "_CP[0-" + viewsPerType + "];.*create view " + typeName
//          + "_CP[0-4].*;"))).andReturn(true).times(viewsPerType);
//  expect(
//      s.execute(matches("(?s).* .*DROP VIEW " + typeName + "_PP[0-" + viewsPerType + "];.*create view " + typeName
//          + "_PP[0-4].*;"))).andReturn(true).times(viewsPerType);
//  expect(
//      s.executeQuery(matches("select id from LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName
//          + "_CP[0-4];.*create view " + typeName + "_CP[0-4].*(?s)"))).andReturn(resultSetMock).times(viewsPerType);
//  expect(
//      s.executeQuery(matches("select id from LOG_BusyhourHistory.*(?s).*DROP VIEW " + typeName
//          + "_PP[0-4];.*create view " + typeName + "_PP[0-4].*(?s)"))).andReturn(resultSetMock).times(viewsPerType);
//  expect(s.executeQuery("select max(id) as maxId from LOG_BusyhourHistory")).andReturn(resultSetMock).anyTimes();
//
//  expect(s.getMoreResults()).andReturn(false).anyTimes();
//
//  mockResultSets(resultSetMock, false);
//
//  resultSetMock.close();
//  expectLastCall().atLeastOnce();
//
//  s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
//  expectLastCall().anyTimes();
//
//  s.close();
//  expectLastCall().atLeastOnce();
//  replay(s);
//  replay(c);
//  replay(resultSetMock);
//  replay(mDwhdb);
//
//    final StorageTimeAction mockedInstance = new StorageTimeAction(mDwhdb, dwhrep, Logger.getAnonymousLogger());
//  mockedInstance.createBhRankViews(typeToRecreate);
//  verify(s);
//}
//
//@Ignore
//public void testGetPlaceholderCreateStatement() throws Exception {
//
//  final String expects = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='DC_E_MGW_ATMPORTBH_RANKBH_ATMPORT_PP0' AND vcreator='dc') > 0 THEN\n" +
//    "  DROP VIEW DC_E_MGW_ATMPORTBH_RANKBH_ATMPORT_PP0;\n" +
//    "END IF;\n" +
//    "create view DC_E_MGW_ATMPORTBH_RANKBH_ATMPORT_PP0 (ID, BHOBJECT, ATMPORT, MGW, OSS_ID, TRANSPORTNETWORK, DATE_ID, HOUR_ID, ROWSTATUS, BHTYPE, BHVALUE, PERIOD_DURATION, DC_RELEASE, DC_SOURCE, DC_TIMEZONE, MINUTE_OFFSET, SLIDING_WINDOW_SIZE, P_THRESHOLD, N_THRESHOLD, LOOKBACK_DAYS ) as select $ID, 'ATMPORT', ATMPORT, MGW, OSS_ID, TRANSPORTNETWORK, DC_E_MGW_ATMPORT_COUNT.DATE_ID, DC_E_MGW_ATMPORT_COUNT.HOUR_ID, DC_E_MGW_ATMPORT_COUNT.ROWSTATUS, 'ATMPORT_PP0', cast(cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8)) as numeric(18,8)), sum(DC_E_MGW_ATMPORT_COUNT.PERIOD_DURATION), DC_E_MGW_ATMPORT_COUNT.DC_RELEASE, DC_E_MGW_ATMPORT_COUNT.DC_SOURCE, DC_E_MGW_ATMPORT_COUNT.DC_TIMEZONE, 15, 60, 0, 0, 0  from DC_E_MGW_ATMPORT_COUNT where ROWSTATUS NOT IN (''SUSPECTED'') group by $ID, 'ATMPORT', ATMPORT, MGW, OSS_ID, TRANSPORTNETWORK, DC_E_MGW_ATMPORT_COUNT.DATE_ID, DC_E_MGW_ATMPORT_COUNT.HOUR_ID, DC_E_MGW_ATMPORT_COUNT.ROWSTATUS, 'ATMPORT_PP0', DC_E_MGW_ATMPORT_COUNT.DC_RELEASE, DC_E_MGW_ATMPORT_COUNT.DC_SOURCE, DC_E_MGW_ATMPORT_COUNT.DC_TIMEZONE;";
//
//  final String versionid = "DC_E_MGW:((802))";
//  final String bhlevel = "DC_E_MGW_ATMPORTBH";
//  final String bhtype = "PP0";
//  final String bhcriteria = "cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8))";
//  final String whereclause = "ROWSTATUS NOT IN (''SUSPECTED'') ";
//  final String description = "Atmport Rx maximum hour.";
//  final String targetversionid = "DC_E_MGW:((802))";
//  final String bhobject = "ATMPORT";
//  final Integer bhelement = 0;
//  final Integer enable = 1;
//  final String aggregationtype = "RANKBH_TIMELIMITED";
//  final Integer offset = 15;
//  final Integer windowsize = 60;
//  final Integer lookback = 0;
//  final Integer p_threshold = 0;
//  final Integer n_threshold = 0;
//  final String clause = "";
//  final String placeholdertype = "pp";
//  //final String grouping = "Time";
//  final Integer reactivateviews = 0;
//
//  final String tpName = "DC_E_MGW";
//
//  final Busyhour bh = new Busyhour(dwhrep);
//
//  bh.setVersionid(versionid);
//  bh.setBhlevel(bhlevel);
//  bh.setBhtype(bhtype);
//  bh.setBhcriteria(bhcriteria);
//  bh.setWhereclause(whereclause);
//  bh.setDescription(description);
//  bh.setTargetversionid(targetversionid);
//  bh.setBhobject(bhobject);
//  bh.setBhelement(bhelement);
//  bh.setEnable(enable);
//  bh.setAggregationtype(aggregationtype);
//  bh.setOffset(offset);
//  bh.setWindowsize(windowsize);
//  bh.setLookback(lookback);
//  bh.setP_threshold(p_threshold);
//  bh.setN_threshold(n_threshold);
//  bh.setClause(clause);
//  bh.setPlaceholdertype(placeholdertype);
//  //bh.setGrouping(grouping);
//  bh.setReactivateviews(reactivateviews);
//
//  Statement s = dwhrep.getConnection().createStatement();
//  final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//  final Connection c = createNiceMock(Connection.class);
//
//  mDwhdb.getConnection();
//  expectLastCall().andReturn(c);
//  c.createStatement();
//  expectLastCall().andReturn(s);
//
//  //Test method
//  String resultSQL = StorageTimeAction.getPlaceholderCreateStatement(bh, versionid, tpName, dwhrep);
//  assertEquals(expects, resultSQL);
//}
//
///**
// * This will test that the Aggreagations and AggregationRules are created correctly.
// */
//@Test
//public void testCreateRankbhAggregationsForBusyhour() {
//  try {
//    //Setup...
//    final StorageTimeAction testInstance = new StorageTimeAction(dwhdb, dwhrep, Logger.getAnonymousLogger());
//    final String targetVersionId = "DC_E_CPP:((333))";
//
//    Busyhour bh = new Busyhour(dwhrep);
//    bh.setVersionid("CUSTOM_DC_E_CPP:((888))");
//    bh.setTargetversionid(targetVersionId);
//    bh.setBhlevel("DC_E_CPP_AAL2APBH");
//    bh.setBhobject("Aal2Ap");
//    bh.setBhtype("CTP_PP0");
//
//    final Busyhoursource whereBusyhoursource = new Busyhoursource(dwhrep);
//    whereBusyhoursource.setVersionid(bh.getVersionid());
//    whereBusyhoursource.setTargetversionid(bh.getTargetversionid());
//    whereBusyhoursource.setBhlevel(bh.getBhlevel());
//    whereBusyhoursource.setBhobject(bh.getBhobject());
//    whereBusyhoursource.setBhtype(bh.getBhtype());
//    BusyhoursourceFactory busyhoursourceFactory;
//    busyhoursourceFactory = new BusyhoursourceFactory(dwhrep, whereBusyhoursource, true);
//    Vector<Busyhoursource> busyhoursources = busyhoursourceFactory.get();
//
//    //Pre-Execution Check...
//    //Store all Aggreagations and AggregationRules. This is to be used to compare the output from Execution.
//    //To make sure the existing Aggregations and Aggregation Rules were not modified in any way.
//    Aggregation searchAggregation = new Aggregation(dwhrep);
//    searchAggregation.setVersionid(bh.getVersionid());
//    AggregationFactory aggregationFactory = new AggregationFactory(dwhrep, searchAggregation, true);
//
//    Vector<Aggregation> beforeAggregations = aggregationFactory.get();
//
//    //Execution...
//    testInstance.createRankbhAggregationsForBusyhour(bh, busyhoursources, bh.getVersionid(), dwhrep);
//
//    //Verification...
//    //Test that all Aggreagations and AggregationRules are created correctly.
//    //Get all the aggregations after
//    searchAggregation.setVersionid(bh.getVersionid());
//    aggregationFactory = new AggregationFactory(dwhrep, searchAggregation, true);
//    Vector<Aggregation> afterAggregations = aggregationFactory.get();
//    assertTrue(isAggregationContinedInAggregations("DC_E_CPP_AAL2APBH_RANKBH_Aal2Ap_CTP_PP0", afterAggregations));
//    assertTrue(isAggregationContinedInAggregations("DC_E_CPP_AAL2APBH_WEEKRANKBH_Aal2Ap_CTP_PP0", afterAggregations));
//    assertTrue(isAggregationContinedInAggregations("DC_E_CPP_AAL2APBH_MONTHRANKBH_Aal2Ap_CTP_PP0", afterAggregations));
//
//    Aggregationrule searchAggregationrule = new Aggregationrule(dwhrep);
//    searchAggregationrule.setVersionid(bh.getVersionid());
//    AggregationruleFactory aggregationruleFactory = new AggregationruleFactory(dwhrep, searchAggregationrule, true);
//    Vector<Aggregationrule> afterAggregationrules = aggregationruleFactory.get();
//
//    String message = null;
//    message = isTargetVersionIdContinedInAggregationRules(targetVersionId + ":DC_E_CPP_AAL2APBH", "DC_E_CPP_AAL2APBH_WEEKRANKBH_Aal2Ap_CTP_PP0", afterAggregationrules);
//    assertNull(message, message);
//    message = isTargetVersionIdContinedInAggregationRules(targetVersionId + ":DC_E_CPP_AAL2APBH", "DC_E_CPP_AAL2APBH_MONTHRANKBH_Aal2Ap_CTP_PP0", afterAggregationrules);
//    assertNull(message, message);
//    //The source_mtableid for the RANKBH (DAY) should be blank.
//    message = isTargetVersionIdContinedInAggregationRules(targetVersionId + ":DC_E_CPP_AAL2APBH", "DC_E_CPP_AAL2APBH_RANKBH_Aal2Ap_CTP_PP0", afterAggregationrules);
//    String expected = "Aggregationrule: The source_mtableid is not correct for (DC_E_CPP_AAL2APBH_RANKBH_Aal2Ap_CTP_PP0). Expected it to start with - (" + targetVersionId + ":DC_E_CPP_AAL2APBH) but got - ()";
//    assertEquals(expected, message);
//
//  } catch (SQLException e) {
//    fail(e.getMessage());
//  } catch (RockException e) {
//    fail(e.getMessage());
//  } catch (Exception e) {
//    fail(e.getMessage());
//  }
//
//}
//
///**
// * This is a utility method. It is used to check if the Generated
// * Aggregations contain the aggregations we expect.
// *
// * @param aggregationName
// * @param theContainer
// * @return
// */
//private boolean isAggregationContinedInAggregations(final String aggregationName, final Vector<Aggregation> theContainer) {
//  Iterator<Aggregation> itr = theContainer.iterator();
//  while (itr.hasNext()) {
//    if (itr.next().getAggregation().equalsIgnoreCase(aggregationName)) {
//      return true; //found the aggregation.
//    }
//  }
//  return false; //Coundn't find the aggregation in the aggregations!
//}
//
///**
// * This is a utility method. It is used to check if the Generated
// * Aggregationrules contain the targetVersionId we expect.
// *
// * @param targetVersionId
// * @param theContainer
// * @return
// */
//private String isTargetVersionIdContinedInAggregationRules(final String targetVersionId, final String aggregation, final Vector<Aggregationrule> theContainer) {
//  Aggregationrule aggregationRule = null;
//  Iterator<Aggregationrule> itr = theContainer.iterator();
//  while (itr.hasNext()) {
//    aggregationRule = itr.next();
//    //is the aggregation rule correct?
//    if (aggregationRule.getAggregation().equalsIgnoreCase(aggregation)) {
//      //is the target_mtableId correct?
//      if (aggregationRule.getTarget_mtableid().startsWith(targetVersionId)) {
//        //is the source_mtableid correct?
//        if (aggregationRule.getSource_mtableid().startsWith(targetVersionId)) {
//          return null; //everything ok!
//        }
//        return "Aggregationrule: The source_mtableid is not correct for (" + aggregation + "). Expected it to start with - (" + targetVersionId + ") but got - (" + aggregationRule.getSource_mtableid() + ")"; //source_mtableid not correct!
//      }
//      return "Aggregationrule: The target_mtableid is not correct for (" + aggregation + "). Expected it to start with - (" + targetVersionId + ") but got - (" + aggregationRule.getTarget_mtableid() + ")"; //target_mtableId not correct!
//    }
//  }
//  return "Aggregationrule: The aggregation (" + aggregation + ") couldn't be found."; //Couldn't find the aggregation in the Aggregationrules!
//}
//
//@Test
//public void checkThatNumberOfPartitionsIsCorrectForTimeBasedPartition() throws Exception {
//
//  String techPackName = "DC_E_TEST";
//  String measTypeName = techPackName + "_SUCCESS";
//  // maxStorage is in days here
//  long maxStorage = 90;
//  int maxStorageTimeInHoursPerPartition = 168;
//  int partitionplanType = 0;
//  String partitionPlanName = "large_raw";
//  String storageId = measTypeName + ":RAW";
//
//  setupProperties();
//
//  insertTestDataIntoDwhRep(dwhrep.getConnection(), techPackName, partitionPlanName, partitionplanType, maxStorage,
//      maxStorageTimeInHoursPerPartition, measTypeName, storageId, "PM");
//
//  // Create a Mocked ActivationCache for test
//  createMockedActivationCache(techPackName, maxStorage);
//  createMockedSanityChecker(techPackName);
//  new StubbedStorageTimeAction(dwhrep, etlrep, dwhdb, dwhdb_dba, techPackName, Logger.getAnonymousLogger());
//
//  // maxStorage is in days. Therfore covert to hours and add 48 hours for
//  // buffer, then divide by maxStorge of hours per partition. Finally, add one
//  // more partition for extra buffer.
//  double noOfPartitions = Math.ceil(((double) (maxStorage * 24) + 48) / maxStorageTimeInHoursPerPartition);
//  int expectedNoOfPartitions = (int) (noOfPartitions + 1);
//
//  checkThatCorrectNoOfPartitionsWereCreated(techPackName, dwhrep.getConnection(), measTypeName, expectedNoOfPartitions, storageId,
//      StorageTimeAction.TIME_BASED_PARTITION_TYPE);
//  checkThatPartitionSizeIsStillMinusOne(techPackName, dwhrep.getConnection());
//}
//
//@Test
//public void checkThatNumberOfPartitionsIsCorrectForTimeBasedPartitionForUnpartitioned() throws Exception {
//
//  String techPackName = "DC_E_TEST_UNPART";
//  String measTypeName = techPackName + "_SUCCESS";
//  // maxStorage is in days here
//  long maxStorage = 90;
//  int maxStorageTimeInHoursPerPartition = 168;
//  int partitionplanType = 0;
//  String partitionPlanName = "large_raw";
//  String storageId = measTypeName + ":RAW";
//
//  setupProperties();
//
//  insertTestDataIntoDwhRep1(dwhrep.getConnection(), techPackName, partitionPlanName, partitionplanType, maxStorage,
//      maxStorageTimeInHoursPerPartition, measTypeName, storageId, "PM");
//
//  // Create a Mocked ActivationCache for test
//  createMockedActivationCache(techPackName, maxStorage);
//  createMockedSanityChecker(techPackName);
//  new StubbedStorageTimeAction(dwhrep, etlrep, dwhdb, dwhdb_dba, techPackName, Logger.getAnonymousLogger());
//
//  // maxStorage is in days. Therfore covert to hours and add 48 hours for
//  // buffer, then divide by maxStorge of hours per partition. Finally, add one
//  // more partition for extra buffer.
// // double noOfPartitions = Math.ceil(((double) (maxStorage * 24) + 48) / maxStorageTimeInHoursPerPartition);
//  //int expectedNoOfPartitions = (int) (noOfPartitions + 1);
//
// // checkThatCorrectNoOfPartitionsWereCreated(techPackName, dwhrep.getConnection(), measTypeName, expectedNoOfPartitions, storageId,
//    //  StorageTimeAction.TIME_BASED_PARTITION_TYPE);
//  //checkThatPartitionSizeIsStillMinusOne(techPackName, dwhrep.getConnection());
//}
//
//@Test
//public void checkThatNumberOfPartitionsIsCorrectForVolumeBasedPartition() throws Exception {
//  String techPackName = "EVENT_E_TEST";
//  String measTypeName = techPackName + "_SUCCESS";
//  // maxStorage is in rows here
//  long maxStorage = 29675782080L;
//  long maxRowsToStorePerPartition = 2500000000L;
//  int partitionplanType = 1;
//  String partitionPlanName = "sgeh_raw";
//  String storageId = measTypeName + ":RAW";
//
//  setupProperties();
//
//  createExtraTablesForDwhRep(dwhrep.getConnection());
//  insertTestDataIntoDwhRep(dwhrep.getConnection(), techPackName, partitionPlanName, partitionplanType, maxStorage,
//      maxRowsToStorePerPartition, measTypeName, storageId, "ENIQ_EVENT");
//
//
//  // Create a Mocked ActivationCache for test
//  createMockedActivationCache(techPackName, maxStorage);
//  createMockedSanityChecker(techPackName);
//  new StubbedStorageTimeAction(dwhrep, etlrep, dwhdb, dwhdb_dba, techPackName, Logger.getAnonymousLogger());
//
//  double noOfPartitions = Math.ceil((double) maxStorage / maxRowsToStorePerPartition);
//  int expectedNoOfPartitions = (int) (noOfPartitions + 1);
//
//  checkThatCorrectNoOfPartitionsWereCreated(techPackName, dwhrep.getConnection(), measTypeName, expectedNoOfPartitions, storageId,
//      StorageTimeAction.VOLUME_BASED_PARTITION_TYPE);
//  checkThatPartitionSizeIsStillMinusOne(techPackName, dwhrep.getConnection());
//}
//
//@Test
//public void checkThatNumberOfPartitionsIsReducedForVolumeBasedPartition() throws Exception {
//  String techPackName = "EVENT_E_TEST1";
//  String measTypeName = techPackName + "_SUCCESS";
//  // maxStorage is in rows here
//  long maxStorage = 29675782080L;
//  long maxRowsToStorePerPartition = 2500000000L;
//  int partitionplanType = 1;
//  String partitionPlanName = "sgeh_raw";
//  String storageId = measTypeName + ":RAW";
//
//  setupProperties();
//
//  createExtraTablesForDwhRep(dwhrep.getConnection());
//
//  insertTestDataIntoDwhRep(dwhrep.getConnection(), techPackName, partitionPlanName, partitionplanType, maxStorage,
//      maxRowsToStorePerPartition, measTypeName, storageId, "ENIQ_EVENT");
//
//  final List<String> tablesDropped = Arrays.asList("EVENT_E_TEST1_SUCCESS_RAW_08",
//    "EVENT_E_TEST1_SUCCESS_RAW_09", "EVENT_E_TEST1_SUCCESS_RAW_10", "EVENT_E_TEST1_SUCCESS_RAW_11",
//    "EVENT_E_TEST1_SUCCESS_RAW_12", "EVENT_E_TEST1_SUCCESS_RAW_13");
//  final Statement stmt = dwhrep.getConnection().createStatement();
//  for(String table : tablesDropped){
//    stmt.execute("create table "+table+" (aa smallint)");
//  }
//  stmt.close();
//
//  double noOfPartitions = Math.ceil((double) maxStorage / maxRowsToStorePerPartition);
//  int expectedNoOfPartitions = (int) (noOfPartitions + 1);
//
//  insertTestDataIntoDwhRep(dwhrep.getConnection(), storageId, measTypeName, expectedNoOfPartitions);
//
//  CountingManagementCache.initializeCache(etlrep);
//
//  // Create a Mocked ActivationCache for test
//  createMockedActivationCache(techPackName, maxStorage);
//  createMockedSanityChecker(techPackName);
//  new StubbedStorageTimeAction(dwhrep, etlrep, dwhdb, dwhdb_dba, techPackName, Logger.getAnonymousLogger());
//
//  checkThatCorrectNoOfPartitionsWereCreated(techPackName, dwhrep.getConnection(), measTypeName, expectedNoOfPartitions, storageId,
//      StorageTimeAction.VOLUME_BASED_PARTITION_TYPE);
//  checkThatPartitionSizeIsStillMinusOne(techPackName, dwhrep.getConnection());
//
//  // Reduction test
//  maxRowsToStorePerPartition = maxRowsToStorePerPartition * 2;
//
//  updateMaxRowsPerPartition(dwhrep.getConnection(), partitionPlanName, maxRowsToStorePerPartition);
//
//  try{
//    System.setProperty("dwhm.test", "yes");
//    new StubbedStorageTimeAction(dwhrep, etlrep, dwhdb, dwhdb_dba, techPackName, Logger.getAnonymousLogger());
//  } finally {
//    System.clearProperty("dwhm.test");
//  }
//
//  noOfPartitions = Math.ceil((double) maxStorage / maxRowsToStorePerPartition);
//  expectedNoOfPartitions = (int) (noOfPartitions + 1);
//
//  checkThatCorrectNoOfPartitionsWereCreated(techPackName, dwhrep.getConnection(), measTypeName, expectedNoOfPartitions, storageId,
//      StorageTimeAction.VOLUME_BASED_PARTITION_TYPE);
//  checkThatPartitionSizeIsStillMinusOne(techPackName, dwhrep.getConnection());
//
//  checkThatCountingManagmentDataIsClearedFromDB(storageId, expectedNoOfPartitions, dwhrep.getConnection());
//
//  final Statement stmt1 = dwhrep.getConnection().createStatement();
//  try{
//    for(String table : tablesDropped) {
//      try{
//        stmt1.execute("select count(*) from " + table);
//        fail("Table "+table+" was not deleted.");
//      } catch (SQLException e){
//        // error 42501 is a table not found error, good, it should have been deleted above.
//        if(!"42501".equals(e.getSQLState())){
//          throw e;
//        }
//      }
//    }
//  } finally {
//    stmt1.close();
//  }
//}
//
//@Test
//public void checkThatPartitionsAreSortedCorrectly() throws Exception {
//  String techPackName = "EVENT_E_TEST123";
//  long maxStorage = 29675782080L;
//
//  // Create a Mocked ActivationCache for test
//  createMockedActivationCache(techPackName, maxStorage);
//  createMockedSanityChecker(techPackName);
//  StubbedStorageTimeAction stubbedStorageTimeAction = new StubbedStorageTimeAction(dwhrep, etlrep, dwhdb, dwhdb_dba,
//      techPackName, Logger.getAnonymousLogger());
//
//  Vector<Dwhpartition> dwhPartitions = new Vector<Dwhpartition>();
//
//  Dwhpartition dwhPartition1 = new Dwhpartition(dwhrep);
//  dwhPartition1.setTablename("DC_E_TEST_RAW_01");
//  dwhPartition1.setLoadorder(20);
//  dwhPartition1.setStarttime(new Timestamp(System.currentTimeMillis()));
//  dwhPartitions.add(dwhPartition1);
//
//  Dwhpartition dwhPartition2 = new Dwhpartition(dwhrep);
//  dwhPartition2.setTablename("DC_E_TEST_RAW_02");
//  dwhPartition2.setLoadorder(null);
//  dwhPartition2.setStarttime(new Timestamp(System.currentTimeMillis() - 100000L));
//  dwhPartitions.add(dwhPartition2);
//
//  Dwhpartition dwhPartition3 = new Dwhpartition(dwhrep);
//  dwhPartition3.setTablename("DC_E_TEST_RAW_03");
//  dwhPartition3.setLoadorder(1);
//  dwhPartition3.setStarttime(new Timestamp(System.currentTimeMillis() + 300000L));
//  dwhPartitions.add(dwhPartition3);
//
//  Dwhpartition dwhPartition4 = new Dwhpartition(dwhrep);
//  dwhPartition4.setTablename("DC_E_TEST_RAW_04");
//  dwhPartition4.setLoadorder(null);
//  dwhPartition4.setStarttime(null);
//  dwhPartitions.add(dwhPartition4);
//
//  Dwhpartition dwhPartition5 = new Dwhpartition(dwhrep);
//  dwhPartition5.setTablename("DC_E_TEST_RAW_05");
//  dwhPartition5.setLoadorder(null);
//  dwhPartition5.setStarttime(null);
//  dwhPartitions.add(dwhPartition5);
//
//  // Check order is correct if VOLUME BASED
//  stubbedStorageTimeAction.sortPartitions(StorageTimeAction.VOLUME_BASED_PARTITION_TYPE, dwhPartitions);
//
//  Dwhpartition volumeSortedPartition1 = dwhPartitions.get(0);
//  assertEquals(volumeSortedPartition1.getLoadorder(), dwhPartition1.getLoadorder());
//  assertEquals(volumeSortedPartition1.getTablename(), dwhPartition1.getTablename());
//
//  Dwhpartition volumeSortedPartition2 = dwhPartitions.get(1);
//  assertEquals(volumeSortedPartition2.getLoadorder(), dwhPartition3.getLoadorder());
//  assertEquals(volumeSortedPartition2.getTablename(), dwhPartition3.getTablename());
//
//  Dwhpartition volumeSortedPartition3 = dwhPartitions.get(2);
//  assertEquals(volumeSortedPartition3.getLoadorder(), dwhPartition2.getLoadorder());
//  assertEquals(volumeSortedPartition3.getTablename(), dwhPartition2.getTablename());
//
//  Dwhpartition volumeSortedPartition4 = dwhPartitions.get(3);
//  assertEquals(volumeSortedPartition4.getLoadorder(), dwhPartition4.getLoadorder());
//  assertEquals(volumeSortedPartition4.getTablename(), dwhPartition4.getTablename());
//
//  Dwhpartition volumeSortedPartition5 = dwhPartitions.get(4);
//  assertEquals(volumeSortedPartition5.getLoadorder(), dwhPartition5.getLoadorder());
//  assertEquals(volumeSortedPartition5.getTablename(), dwhPartition5.getTablename());
//
//  // Check order is correct if TIME BASED
//  stubbedStorageTimeAction.sortPartitions(StorageTimeAction.TIME_BASED_PARTITION_TYPE, dwhPartitions);
//
//  Dwhpartition timeSortedPartition1 = dwhPartitions.get(0);
//  assertEquals(timeSortedPartition1.getStarttime(), dwhPartition3.getStarttime());
//  assertEquals(timeSortedPartition1.getTablename(), dwhPartition3.getTablename());
//
//  Dwhpartition timeSortedPartition2 = dwhPartitions.get(1);
//  assertEquals(timeSortedPartition2.getStarttime(), dwhPartition1.getStarttime());
//  assertEquals(timeSortedPartition2.getTablename(), dwhPartition1.getTablename());
//
//  Dwhpartition timeSortedPartition3 = dwhPartitions.get(2);
//  assertEquals(timeSortedPartition3.getStarttime(), dwhPartition2.getStarttime());
//  assertEquals(timeSortedPartition3.getTablename(), dwhPartition2.getTablename());
//
//  Dwhpartition timeSortedPartition4 = dwhPartitions.get(3);
//  assertEquals(timeSortedPartition4.getLoadorder(), dwhPartition4.getLoadorder());
//  assertEquals(timeSortedPartition4.getTablename(), dwhPartition4.getTablename());
//
//  Dwhpartition timeSortedPartition5 = dwhPartitions.get(4);
//  assertEquals(timeSortedPartition5.getLoadorder(), dwhPartition5.getLoadorder());
//  assertEquals(timeSortedPartition5.getTablename(), dwhPartition5.getTablename());
//}
//
//private void checkThatCorrectNoOfPartitionsWereCreated(String techPackName, Connection dwhRepCon,
//    String measTypeName, int expectedNoOfPartitions, String storageId, short partitionType) throws SQLException {
//  Statement statement = dwhRepCon.createStatement();
//
//  try {
//    // Check that DWHPartition table has been updated
//    int actualNoOfPartitions = 0;
//    final ResultSet dwhPartitionResult = statement.executeQuery("SELECT * FROM DWHPartition where STORAGEID like '%"
//        + techPackName + "%'");
//    while (dwhPartitionResult.next()) {
//      assertThat(dwhPartitionResult.getString(1), is(storageId));
//      assertThat(dwhPartitionResult.getString(2).startsWith(measTypeName + "_RAW_"), is(true));
//      assertNull(dwhPartitionResult.getTimestamp(4));
//      if (partitionType == StorageTimeAction.TIME_BASED_PARTITION_TYPE) {
//        assertNull(dwhPartitionResult.getTimestamp(3));
//        assertThat(dwhPartitionResult.getString(5), is("NEW"));
//      } else {
//        assertNotNull(dwhPartitionResult.getTimestamp(3));
//        assertThat(dwhPartitionResult.getString(5), is("ACTIVE"));
//      }
//
//      assertNull(dwhPartitionResult.getString(6));
//      actualNoOfPartitions++;
//    }
//    assertEquals(expectedNoOfPartitions, actualNoOfPartitions);
//  } catch(SQLException e){
//    throw e;
//  }finally {
//    statement.close();
//
//  }
//}
//
//private void checkThatPartitionSizeIsStillMinusOne(String techPackName, Connection dwhRepCon) throws SQLException {
//  Statement statement = dwhRepCon.createStatement();
//  try {
//    // Check that PARTITIONSIZE in DWHType is not changed and left as -1
//    final ResultSet dwhTypeResult = statement
//        .executeQuery("SELECT PARTITIONSIZE FROM DWHType where STORAGEID like '%" + techPackName + "%'");
//
//    while (dwhTypeResult.next()) {
//      int actualPartitionSize = dwhTypeResult.getInt(1);
//      assertEquals(-1, actualPartitionSize);
//    }
//  } finally {
//    statement.close();
//  }
//}
//
//private void checkThatCountingManagmentDataIsClearedFromDB(String storageId, int expectedNoOfPartitions,
//    Connection dwhRepCon) throws SQLException {
//  Statement statement = dwhRepCon.createStatement();
//  try {
//    // Check that PARTITIONSIZE in DWHType is not changed and left as -1
//    final ResultSet dwhTypeResult = statement
//        .executeQuery("SELECT count(*) FROM CountingManagement where STORAGEID = '" + storageId + "'");
//
//    while (dwhTypeResult.next()) {
//      int actualPartitionCount = dwhTypeResult.getInt(1);
//      assertThat("updateCountingManagementRows", actualPartitionCount, is(expectedNoOfPartitions));
//    }
//  } finally {
//    statement.close();
//  }
//}
//  
//  private void doCreateViews(final String tpName, final String measType, int viewsPerType, final boolean force) throws Exception {
//    final String typeName = tpName + "_" + measType + "BH";
//    final Measurementtype where = new Measurementtype(dwhrep);
//    where.setTypename(typeName);
//    final MeasurementtypeFactory fac = new MeasurementtypeFactory(dwhrep, where);
//    final List<Measurementtype> types = fac.get();
//    assertFalse("No Measurementtypes found, setup not correct ", types.isEmpty());
//    final Measurementtype typeToRecreate = types.get(0);
//
//    final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//    final Connection c = createNiceMock(Connection.class);
//    final Statement s = createMock(Statement.class);
//
//    final ResultSet resultSetMock = createMock(ResultSet.class);
//    
//    expect(mDwhdb.getConnection()).andReturn(c).anyTimes();
//
//    expect(c.createStatement()).andReturn(s).anyTimes();
//
//    expect(s.executeQuery(matches("select id from LOG_BusyhourHistory.*(?s).*DROP VIEW "+typeName+"_RANKBH_"+measType+"_CP[0-"+viewsPerType+"];.*create view "+typeName+"_RANKBH_"+measType+"_CP[0-4].*(?s)"))).andReturn(resultSetMock).anyTimes();
//    expect(s.executeQuery(matches("select id from LOG_BusyhourHistory.*(?s).*DROP VIEW "+typeName+"_RANKBH_"+measType+"_PP[0-"+viewsPerType+"];.*create view "+typeName+"_RANKBH_"+measType+"_PP[0-4].*(?s)"))).andReturn(resultSetMock).anyTimes();
//
//    mockResultSets(resultSetMock, true);
//    mockPlaceHolderDrops(s, tpName + "_"+measType+"BH_RANKBH_"+measType, viewsPerType);
//
//    s.close();
//    expectLastCall().atLeastOnce();
//    replay(s);
//    replay(c);
//    replay(resultSetMock);
//    replay(mDwhdb);
//    final StorageTimeAction mockedInstance = new StorageTimeAction(mDwhdb, dwhrep, Logger.getAnonymousLogger());
//
//    mockedInstance.createBhRankViews(typeToRecreate);
//    verify(s);
//  }
//
//  private void mockResultSets(ResultSet resultSetMock, boolean switchItOn) throws SQLException {
//    if(switchItOn){
//      for(int i = 0; i < 11; i++){
//        expect(resultSetMock.next()).andReturn(switchItOn);
//        expect(resultSetMock.getLong("id")).andReturn(new Long(i));
//      }
//    }
//    else{
//      for(int i = 0; i < 11; i++){
//        expect(resultSetMock.next()).andReturn(false);// we did not found a old value in the table
//        expect(resultSetMock.next()).andReturn(true); // we have to find the mad id in the table.
//        expect(resultSetMock.getLong("maxId")).andReturn(new Long(i)); // we r.
//      }
//    }
//  }
//
//  private void mockHistoryCheck(final Statement s, final String tName, final int pCount) throws Exception {
//    final ResultSet rsMock = createNiceMock(ResultSet.class);
//    for(int i=0;i<pCount;i++){
//        s.executeQuery(matches(getMatchString("LOG_BusyhourHistory", tName, i, "CP")));
//        expectLastCall().andReturn(rsMock);
//    }
//    for(int i=0;i<pCount;i++){
//        s.executeQuery(matches(getMatchString("LOG_BusyhourHistory", tName, i, "PP")));
//        expectLastCall().andReturn(rsMock);
//    }
//}
//
//  private void mockPlaceHolderDrops(final Statement s, final String tName, final int pCount) throws Exception {
//      for(int i=0;i<pCount;i++){
//          s.execute(matches(getMatchString("", tName, i, "CP")));
//          expectLastCall().andReturn(true);
//      }
//      for(int i=0;i<pCount;i++){
//          s.execute(matches(getMatchString("", tName, i, "PP")));
//          expectLastCall().andReturn(true);
//      }
//  }
//  
//  private String getMatchString(final String tempName,final String tName, final int pindex, final String phType){
//    final String viewName = tName+"_"+phType+pindex;
//    return "(?s).*"+tempName+" .*DROP VIEW "+viewName+";.*create view "+viewName+".*;";
//}
//
//  
//  
//	/**
//	 * Test method for {@link com.distocraft.dc5000.StorageTimeAction#replaceDollarId(String sql)}.
//	 */
//  @Test
//  public void testReplaceDollarId() throws Exception {
//  	final String sql = "select $ID,'IUBLINK', DC_E_RAN_IUBLINK_RAW.IubLink, DC_E_RAN_IUBLINK_RAW.OSS_ID, DC_E_RAN_IUBLINK_RAW.RNC,"+ 
//  					" DC_E_RAN_IUBLINK_RAW.DATE_ID, DC_E_RAN_IUBLINK_RAW.HOUR_ID, DC_E_RAN_IUBLINK_RAW.ROWSTATUS, 'IUBLINK_RBS_UL_CE',"+ 
//  					" cast(sum(pmSumUlCredits) / sum(pmSamplesUlCredits) as numeric(18,8)), sum(DC_E_RAN_IUBLINK_RAW.PERIOD_DURATION),"+ 
//  					" DC_E_RAN_IUBLINK_RAW.DC_RELEASE, DC_E_RAN_IUBLINK_RAW.DC_SOURCE, DC_E_RAN_IUBLINK_RAW.DC_TIMEZONE, 0, 60, 0, 0, 0"+  
//  					" from DC_E_RAN_IUBLINK_RAW group by $ID,'IUBLINK', DC_E_RAN_IUBLINK_RAW.IubLink, DC_E_RAN_IUBLINK_RAW.OSS_ID," +
//  					" DC_E_RAN_IUBLINK_RAW.RNC, DC_E_RAN_IUBLINK_RAW.DATE_ID, DC_E_RAN_IUBLINK_RAW.HOUR_ID," +
//  					" DC_E_RAN_IUBLINK_RAW.ROWSTATUS, 'IUBLINK_RBS_UL_CE', DC_E_RAN_IUBLINK_RAW.DC_RELEASE," +
//  					" DC_E_RAN_IUBLINK_RAW.DC_SOURCE, DC_E_RAN_IUBLINK_RAW.DC_TIMEZONE;";    	
//
//		assertNotNull("StorageTimeAction instance should not be null", testInstance);		
//		
//		final String expected = "select 2,'IUBLINK', DC_E_RAN_IUBLINK_RAW.IubLink, DC_E_RAN_IUBLINK_RAW.OSS_ID, DC_E_RAN_IUBLINK_RAW.RNC,"+ 
//		" DC_E_RAN_IUBLINK_RAW.DATE_ID, DC_E_RAN_IUBLINK_RAW.HOUR_ID, DC_E_RAN_IUBLINK_RAW.ROWSTATUS, 'IUBLINK_RBS_UL_CE',"+ 
//		" cast(sum(pmSumUlCredits) / sum(pmSamplesUlCredits) as numeric(18,8)), sum(DC_E_RAN_IUBLINK_RAW.PERIOD_DURATION),"+ 
//		" DC_E_RAN_IUBLINK_RAW.DC_RELEASE, DC_E_RAN_IUBLINK_RAW.DC_SOURCE, DC_E_RAN_IUBLINK_RAW.DC_TIMEZONE, 0, 60, 0, 0, 0"+  
//		" from DC_E_RAN_IUBLINK_RAW group by 2,'IUBLINK', DC_E_RAN_IUBLINK_RAW.IubLink, DC_E_RAN_IUBLINK_RAW.OSS_ID," +
//		" DC_E_RAN_IUBLINK_RAW.RNC, DC_E_RAN_IUBLINK_RAW.DATE_ID, DC_E_RAN_IUBLINK_RAW.HOUR_ID," +
//		" DC_E_RAN_IUBLINK_RAW.ROWSTATUS, 'IUBLINK_RBS_UL_CE', DC_E_RAN_IUBLINK_RAW.DC_RELEASE," +
//		" DC_E_RAN_IUBLINK_RAW.DC_SOURCE, DC_E_RAN_IUBLINK_RAW.DC_TIMEZONE;";		
//
//		final Class pcClass = testInstance.getClass();
//		final Method replaceDollarId = pcClass.getDeclaredMethod("replaceDollarId", new Class[] {String.class});
//		replaceDollarId.setAccessible(true);	
//		final String actual = (String)replaceDollarId.invoke(testInstance, new Object[] {sql});		
//		assertEquals( expected, actual);
//  } 
//
//@Ignore
//	public void testUpdateBHCounters() {
//		try {
//			final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//			final Dwhtechpacks dwhtechpacksMock = createNiceMock(Dwhtechpacks.class);
//			final Connection c = createNiceMock(Connection.class);
//			final Statement s = createMock(Statement.class);
//			final ResultSet resultSetMock = createMock(ResultSet.class);
//    final StorageTimeAction mockedInstance = new StorageTimeAction(mDwhdb, dwhrep, Logger.getAnonymousLogger());
//
//			final String expectedElementDeletes = "DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RBS_RBSVclTpRxAtmCells';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RBS_RBSVclTpTxAtmCells';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RNC_RNCVclTpRxAtmCells';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RNC_RNCVclTpTxAtmCells';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RXI_RXIVclTpRxAtmCells';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RXI_RXIVclTpTxAtmCells';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='ELEM_CP0';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='ELEM_CP1';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='ELEM_CP2';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='ELEM_CP3';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='ELEM_CP4';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RBS_PP0';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RBS_PP1';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RNC_PP2';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RNC_PP3';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RXI_PP4';"+"\n"+
//												"DELETE FROM DIM_E_CPP_ELEMBH_BHTYPE WHERE BHTYPE='RXI_PP5';"+"\n";
//			final String expectedElementInserts = "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RBS_RBSVclTpRxAtmCells','RBSVclTpRxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RBS_RBSVclTpTxAtmCells','RBSVclTpTxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RNC_RNCVclTpRxAtmCells','RNCVclTpRxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RNC_RNCVclTpTxAtmCells','RNCVclTpTxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RXI_RXIVclTpRxAtmCells','RXIVclTpRxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RXI_RXIVclTpTxAtmCells','RXIVclTpTxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('ELEM_CP0',' ');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('ELEM_CP1',' ');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('ELEM_CP2',' ');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('ELEM_CP3',' ');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('ELEM_CP4',' ');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RBS_PP0','RBSVclTpRxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RBS_PP1','RBSVclTpTxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RNC_PP2','RNCVclTpRxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RNC_PP3','RNCVclTpTxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RXI_PP4','RXIVclTpRxAtmCells');"+"\n"+
//										   "insert into DIM_E_CPP_ELEMBH_BHTYPE (BHTYPE, DESCRIPTION) values ('RXI_PP5','RXIVclTpTxAtmCells');"+"\n";
//			final String expectedObjectDeletes = "DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_TOTAL_CID';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_NODEB_TRAF';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_CP0';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_CP1';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_CP2';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_CP3';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_CP4';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_PP0';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_PP1';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_PP2';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_PP3';"+"\n"+
//											"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='AAL2AP_PP4';"+"\n";
//			final String expectedObjectInserts = "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_TOTAL_CID','Total CID');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_NODEB_TRAF','NodeB Traffic Busy Hour');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_CP0',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_CP1',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_CP2',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_CP3',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_CP4',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_PP0',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_PP1',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_PP2',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_PP3',' ');"+"\n"+
//										  "insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('AAL2AP_PP4',' ');"+"\n";
//
//			// setup the expected calls
//			// TechPack
//			expect(dwhtechpacksMock.getTechpack_name()).andReturn("DC_E_CPP");
//			expect(dwhtechpacksMock.getVersionid()).andReturn("DC_E_CPP:((119))");
//			replay(dwhtechpacksMock);
//			// dB connection
//			expect(mDwhdb.getConnection()).andReturn(c).anyTimes();
//			// SQL
//			expect(c.createStatement()).andReturn(s).anyTimes();
//    s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
//			expect(s.execute(expectedElementDeletes)).andReturn(true).times(1);
//    s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
//			expect(s.execute(expectedElementInserts)).andReturn(true).times(1);
//    s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
//			expect(s.execute(expectedObjectDeletes)).andReturn(true).times(1);
//    s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
//			expect(s.execute(expectedObjectInserts)).andReturn(true).times(1);
//    expect(s.getMoreResults()).andReturn(false);
//    expectLastCall().atLeastOnce();
//			s.close();
//			expectLastCall().atLeastOnce();
//
//			replay(s);
//			replay(c);
//			replay(resultSetMock);
//			replay(mDwhdb);
//
//			// Test method
//			mockedInstance.updateBHCounters(dwhtechpacksMock);
//			verify(s);
//		} catch (Exception e) {
//			fail("Exception Thrown" + e.getMessage());
//		}
//	} // testUpdateBHCounters
//  
//  @Test
//	public void testBHCountersForCustomTechpack() {
//		try {
//			final RockFactory mDwhdb = createNiceMock(RockFactory.class);
//			final Dwhtechpacks dwhtechpacksMock = createNiceMock(Dwhtechpacks.class);
//			final Connection c = createNiceMock(Connection.class);
//			final Statement s = createMock(Statement.class);
//			final ResultSet resultSetMock = createMock(ResultSet.class);
//    final StorageTimeAction mockedInstance = new StorageTimeAction(mDwhdb, dwhrep, Logger.getAnonymousLogger());
//			
//			final String expected = "DELETE FROM DIM_E_CPP_VCLTPBH_BHTYPE WHERE BHTYPE='Vcltp_REC_ATM_CELLS';"+"\n"+
//									"insert into DIM_E_CPP_VCLTPBH_BHTYPE (BHTYPE, DESCRIPTION) values ('Vcltp_REC_ATM_CELLS','Received ATM cells per VCI');"+"\n"+
//									"DELETE FROM DIM_E_CPP_VCLTPBH_BHTYPE WHERE BHTYPE='Vcltp_TRANS_ATM_CELLS';"+"\n"+
//									"insert into DIM_E_CPP_VCLTPBH_BHTYPE (BHTYPE, DESCRIPTION) values ('Vcltp_TRANS_ATM_CELLS','Transmitted ATM cells per VCI');"+"\n"+
//									"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='Aal2Ap_TOTAL_CID';"+"\n"+
//									"insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('Aal2Ap_TOTAL_CID','Total CID');"+"\n"+
//									"DELETE FROM DIM_E_CPP_AAL2APBH_BHTYPE WHERE BHTYPE='Aal2Ap_NODEB_TRAF';"+"\n"+
//									"insert into DIM_E_CPP_AAL2APBH_BHTYPE (BHTYPE, DESCRIPTION) values ('Aal2Ap_NODEB_TRAF','NodeB Traffic Busy Hour');"+"\n";
//			
//			// setup the expected calls
//			// TechPack
//			expect(dwhtechpacksMock.getVersionid()).andReturn("CUSTOM_DC_E_CPP:((118))");
//			replay(dwhtechpacksMock);
//			// dB connection
//			expect(mDwhdb.getConnection()).andReturn(c).anyTimes();
//			// SQL
//			expect(c.createStatement()).andReturn(s).anyTimes();
//    s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
//    expectLastCall().atLeastOnce();
//			expect(s.execute(expected)).andReturn(true).times(1);
//    expect(s.getMoreResults()).andReturn(false);
//    expectLastCall().atLeastOnce();
//			s.close();
//			expectLastCall().atLeastOnce();
//			
//			replay(s);
//			replay(c);
//			replay(resultSetMock);
//			replay(mDwhdb);
//			
//			// Test method
//			mockedInstance.updateBHCountersForCustomTechpack(dwhtechpacksMock);
//			verify(s);
//		} catch (Exception e) {
//			fail("Exception Thrown" + e.getMessage());
//		}
//	} // testBHCountersForCustomTechpack
}