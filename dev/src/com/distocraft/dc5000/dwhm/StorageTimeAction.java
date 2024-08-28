package com.distocraft.dc5000.dwhm;

import com.distocraft.dc5000.repository.cache.CountingManagementCache;
import com.distocraft.dc5000.repository.cache.GroupTypesCache;
import com.distocraft.dc5000.repository.dwhrep.Countingmanagement;
import com.distocraft.dc5000.repository.dwhrep.CountingmanagementFactory;
import com.ericsson.eniq.common.TechPackType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;

import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockException;

import com.ericsson.eniq.common.Constants;
import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.Utils;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.dwhrep.Aggregation;
import com.distocraft.dc5000.repository.dwhrep.AggregationFactory;
import com.distocraft.dc5000.repository.dwhrep.Aggregationrule;
import com.distocraft.dc5000.repository.dwhrep.Busyhour;
import com.distocraft.dc5000.repository.dwhrep.BusyhourFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhourrankkeys;
import com.distocraft.dc5000.repository.dwhrep.BusyhourrankkeysFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhoursource;
import com.distocraft.dc5000.repository.dwhrep.BusyhoursourceFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Externalstatement;
import com.distocraft.dc5000.repository.dwhrep.ExternalstatementFactory;
import com.distocraft.dc5000.repository.dwhrep.Externalstatementstatus;
import com.distocraft.dc5000.repository.dwhrep.ExternalstatementstatusFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementobjbhsupport;
import com.distocraft.dc5000.repository.dwhrep.MeasurementobjbhsupportFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Partitionplan;
import com.distocraft.dc5000.repository.dwhrep.PartitionplanFactory;
import com.distocraft.dc5000.repository.dwhrep.Referencetable;
import com.distocraft.dc5000.repository.dwhrep.ReferencetableFactory;
import com.distocraft.dc5000.repository.dwhrep.Versioning;
import com.distocraft.dc5000.repository.dwhrep.VersioningFactory;

/**
 * @author ejarsav
 * 
 */
@SuppressWarnings({ "PMD.CloseResource", "ResultOfMethodCallIgnored" })
public class StorageTimeAction {

	private static final String DIM = "DIM";

	private transient RockFactory dwhrepRock = null;

	private transient RockFactory etlrepRock = null;

	private transient RockFactory dcRock = null;

	private transient RockFactory dbaRock = null;

	private Logger clog;

	private Logger log = null;

	private boolean debug = false;

	private final static int ROWSPEREXECUTE = 50;

	public final static short TIME_BASED_PARTITION_TYPE = 0;

	public final static short VOLUME_BASED_PARTITION_TYPE = 1;

	private static final SimpleDateFormat SDF = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	// NOPMD : eeipca : SuspiciousConstant
	private static String LOAD_FILE_LOC = "/eniq/upgrade/install/"; 

	private static final String FILE_EXTENSION = ".sql";

	private final List<String> filesToDelete = new ArrayList<String>();

	// New field for Generic SIU implementation. Used to filter the execution of
	// this class for a specific meas type.
	private String measurementType;

	/*
	 * New field for NVU(Generic SIU) implementation. Used to ignore
	 * PhysicalTableCache revalidation if initiated by Generic SIU. Is set only
	 * by one constructor used by NVU(Generic SIU). Flow of call is as below.
	 * CommonUpgradeUtil(afj_manager) -> StorageTimeAction -> SanityChecker ->
	 * PhysicalTableCache(repository)
	 */
	private boolean isCalledByNVU = false;
	
	private boolean customInstallFlag = false;
	  
	private List<String> restrictedTableLevels = new ArrayList<String>();
	  
	private int totalNumberOfDays = 0;
	  
	private PartitionAction pa = null;
	
	private static final String VECTORSFOLDER = "/eniq/sw/installer/vectors/";
	//For keeping new code dormant for old techpacks
	private boolean newVectorFlag = false;
	private static final String CONF_DIR = "/eniq/sw/conf/";
	//private static final String NEWVECTOR = "NEWVECTOR";
	
	/**
	 * @param reprock
	 *            The dwhrep connection - passed from STNUpgrade class in
	 *            AFJManager for Node Version Update
	 * @param etlrock
	 *            The etlrep connection
	 * @param dwhrock
	 *            The dwh connection
	 * @param dbadwhrock
	 *            The dbadwh connection
	 * @param techPack
	 *            The name of techpack
	 * @param clog
	 *            The parent logger
	 * @param typeName
	 *            The measurement type name for which StorageTimeAction needs to
	 *            be run.
	 * @throws Exception
	 *             On errors
	 */
	public StorageTimeAction(final RockFactory reprock,
			final RockFactory etlrock, final RockFactory dwhrock,
			final RockFactory dbadwhrock, final String techPack,
			final Logger clog, final String typeName,
			final boolean isCalledByNVU) throws Exception {
		this.measurementType = typeName;
		this.isCalledByNVU = isCalledByNVU;
		initVelocity();
		storageTimeAction(reprock, etlrock, dwhrock, dbadwhrock, techPack,
				clog, false);
	}

	public StorageTimeAction(final RockFactory reprock,
			final RockFactory etlrock, final RockFactory dwhrock,
			final RockFactory dbadwhrock, final String techPack,
			final Logger clog) throws Exception {
		initVelocity();
		storageTimeAction(reprock, etlrock, dwhrock, dbadwhrock, techPack,
				clog, false);
	}

	public StorageTimeAction(final RockFactory reprock,
			final RockFactory etlrock, final RockFactory dwhrock,
			final RockFactory dbadwhrock, final String techPack,
			final Logger clog, final boolean reDirectConnections)
			throws Exception {
		initVelocity();
		storageTimeAction(reprock, etlrock, dwhrock, dbadwhrock, techPack,
				clog, reDirectConnections);
	}

	/**
	 * Used be the GRoup Management Importer.
	 * 
	 * @param logger
	 *            Logger instance
	 */
	public StorageTimeAction(final Logger logger) {
		this.clog = logger;
		log = Logger.getLogger(this.clog.getName() + ".dwhm.StorageTimeAction");
	}

	/**
	 * Constructor for TPIDE to recreate the BH placeholder views. This
	 * constructor does not run the storageTimeAction() method.
	 * 
	 * @param dwhdbRock
	 *            A DWH warehouse connection
	 * @param dwhrep
	 *            A dwhrep connection
	 * @param logger
	 *            The parent logger
	 * @throws Exception
	 *             If there were errors initializing Velocity
	 */
	public StorageTimeAction(final RockFactory dwhdbRock,
			final RockFactory dwhrep, final Logger logger) throws Exception {
		this.dcRock = dwhdbRock;
		this.dwhrepRock = dwhrep;
		this.debug = "true".equalsIgnoreCase(StaticProperties.getProperty(
				"dwhm.debug", "false"));
		this.clog = logger;
		log = Logger.getLogger(this.clog.getName() + ".dwhm.StorageTimeAction");
		initVelocity();
	}
	
	public StorageTimeAction(final List<String> restrictedTableLevels, final int totalNumberOfDays,final RockFactory reprock, final RockFactory etlrock, final RockFactory dwhrock, final RockFactory dbadwhrock,
		final String techPack, final Logger clog, final boolean reDirectConnections) throws Exception{
		customInstallFlag = true;
		this.restrictedTableLevels = restrictedTableLevels;
		this.totalNumberOfDays = totalNumberOfDays;
		/*ActivationCache.initialize(etlrock);
		StaticProperties.reload();
		Properties.reload();*/
		pa = new PartitionAction(clog,customInstallFlag);
		storageTimeAction(reprock,etlrock,dwhrock,dbadwhrock,techPack,clog,reDirectConnections);
		
	  }

	private void storageTimeAction(final RockFactory reprock,
			final RockFactory etlrock, final RockFactory dwhrock,
			final RockFactory dbadwhrock, final String techPack,
			final Logger clog, final boolean reDirectConnections)
			throws Exception {
		this.dwhrepRock = reprock;
		this.etlrepRock = etlrock;
		this.dcRock = dwhrock;
		this.dbaRock = dbadwhrock;
		this.clog = clog;
		this.debug = "true".equalsIgnoreCase(StaticProperties.getProperty("dwhm.debug", "false"));
		log = Logger.getLogger(this.clog.getName() + ".dwhm.StorageTimeAction");
		final ActivationCache activationCache = getActivationCache();
		
		// Cache revalidation not required for NVU.
		if (!isCalledByNVU) {
			activationCache.revalidate(); // TODO :: NEEDED>>>
		}
		if (!activationCache.isActive(techPack)) {
			log.fine("Techpack " + techPack + " is not active (or not exists)");
			return;
		}
		initVelocity();
		final Dwhtechpacks dtp_cond = new Dwhtechpacks(reprock);
		dtp_cond.setTechpack_name(techPack);
		final DwhtechpacksFactory dtp_fact = new DwhtechpacksFactory(reprock, dtp_cond);
		final Vector<Dwhtechpacks> tps = dtp_fact.get();
		if (tps.size() <= 0) {
			log.info("TechPack " + techPack + " not found from DWH");
			return;
		} else if (tps.size() > 1) {
			log.severe("Panic: Found multiple techpacks from DWH with name " + techPack);
			return;
		}
		final Dwhtechpacks dwhtechpack = tps.get(0);
		log.fine("DWHTechpack found version " + dwhtechpack.getVersionid());
		
		//For new vector handling code
		checkVectorFlag(dwhtechpack.getTechpack_name());		
				
		// Only want ENABLED (ignore OBSOLETE)
		// 20100809,eeoidiv,HM57922:upgrade failure
		final Vector<Dwhtype> types = getDwhtypes(techPack); 
		final Enumeration<Dwhtype> enu = types.elements();
		final TechPackType techPackType = Utils.getTechPackType(reprock, dwhtechpack.getVersionid());
		final SanityChecker sanityChecker = createSanityChecker(reprock, dwhrock, this.isCalledByNVU);
		log.finer("Found " + types.size() + " types");
		final List<String> listOfCreatedViewNames = new ArrayList<String>();
		if (TechPackType.EVENTS == techPackType) {
			GroupTypesCache.init(reprock);
		}

		while (enu.hasMoreElements()) {
			final Dwhtype type = enu.nextElement();
			log.finest("Type " + type.getTypename() + " (" + type.getTablelevel() + ") " + type.getType());
			if (!activationCache.isActive(techPack, type.getTypename(), type.getTablelevel())) {
				log.info("Type " + type.getTypename() + " (" + type.getTablelevel() + ") is not active");
				continue;
			}
			if(customInstallFlag && restrictedTableLevels.contains(type.getTablelevel())){
				log.info("Ignoring the "+type.getStorageid()+" as this tablelevel is restricted for custom Installaion.");
				continue;
			}
			// Get the value for PARTITIONSIZE from table PartitionPlan if
			// PARTITIONSIZE is null or -1.
			boolean usingDefPartitionSize = false;
			short partitionPlanType = TIME_BASED_PARTITION_TYPE;
			Long defaultPartitionSize = new Long(-1);
			if (type.getPartitionsize() == null || type.getPartitionsize() == -1) {
				this.log.fine("No partitionsize specified for " + type.getStorageid() + ". Using default value from PartitionPlan.");
				final Partitionplan targetPartitionPlan = getPartitionPlan(type);
				partitionPlanType = targetPartitionPlan.getPartitiontype();
				defaultPartitionSize = targetPartitionPlan.getDefaultpartitionsize();
				this.log.fine("Using defaultPartitionSize " + defaultPartitionSize);
				usingDefPartitionSize = true;
				type.setPartitionsize(defaultPartitionSize);
			}

			validateTimeBasedPartition(type, partitionPlanType);

			List<Dwhpartition> tobedeleted = null;
			try {
				if ("PARTITIONED".equalsIgnoreCase(type.getType())) {
					long desired_storagetime = 0;
		        	if(!customInstallFlag){
		        		desired_storagetime = activationCache.getStorageTime(type.getTechpack_name(), type.getTypename(), type.getTablelevel());
		        	} else {
		        		//We minus 2 days from the total days needed because it is already adding 2 days in getDesiredNumberOfPartitions() method .
		            	desired_storagetime = totalNumberOfDays - 2;
		        	}
					try {
						// Get the desired number of partitions
						final int desired_partitions = getDesiredNumberOfPartitions(type, desired_storagetime, partitionPlanType);
						tobedeleted = adjustPartitioned(type, desired_partitions, usingDefPartitionSize, partitionPlanType); 
						clearCountingPartitionInfo(type.getStorageid(), tobedeleted);
					} finally {
						// Set the DWHType's partitionsize back to -1, before forwarding it
						// to SanityChecker which updates the DWHType to database.
						if (usingDefPartitionSize) {
							type.setPartitionsize((long) -1);
						}
						sanityChecker.sanityCheck(type);
					}
				} else if ("UNPARTITIONED".equalsIgnoreCase(type.getType())) {
					try {
						tobedeleted = adjustUnpartitioned(type, partitionPlanType);
					} finally {
						// Set the DWHType's partitionsize back to -1, before forwarding it
						// to SanityChecker which updates the DWHType to database.
						if (usingDefPartitionSize) {
							type.setPartitionsize((long) -1);
						}
						sanityChecker.sanityCheck(type);
					}
				} else if ("SIMPLE".equalsIgnoreCase(type.getType())) {
					try {
						adjustSimple(type);
					} finally {
						// Set the DWHType's partitionsize back to -1, before forwarding it
						// to SanityChecker which updates the DWHType to database.
						if (usingDefPartitionSize) {
							type.setPartitionsize((long) -1);
						}
						sanityChecker.sanityCheck(type);
					}
				}
			} catch (SQLException e) {
				// -21 --> table already exists...
				if (e.getErrorCode() == -21) { 
					log.log(Level.INFO, "Type already exists " + type.getStorageid(), e);
				} else {
					log.log(Level.WARNING, "Failed for type " + type.getStorageid(), e);
				}
			} catch (Exception err) {
				log.log(Level.WARNING, "Failed for type " + type.getStorageid(), err);
			}
			
			//restrictedTableLevels check is not necessarry as we are already ignoring in earlier stage.
			if (type.getTablelevel().equals("DAYBH") && !restrictedTableLevels.contains("DAYBH")) {
				createDaybhTempTable(type);
			}
			
			boolean isGroupMgtTable = false;
			if (techPackType == TechPackType.EVENTS) {
				// Groups are only available in Event techpacks
				isGroupMgtTable = GroupTypesCache.isGroupMgtDwhType(dwhtechpack.getVersionid(), type.getTypename());
			}
			if (!isGroupMgtTable) {
				// No public views for the GroupMgt data
				final String viewName = createView(reprock, dwhrock, type, techPackType); 
				if (!listOfCreatedViewNames.contains(viewName)) {
					listOfCreatedViewNames.add(viewName);
				}
			}
			if (tobedeleted != null && tobedeleted.size() > 0) {
				deletePartitions(tobedeleted);
			}
		} // foreach type

		if (!listOfCreatedViewNames.isEmpty()) {
			CreateOverallViewsFactory.createOverallViewsAction(dbadwhrock,
					dwhrock, reprock, types, log, listOfCreatedViewNames, techPack);
		}

		if (!customInstallFlag && !restrictedTableLevels.contains("RANKBH")) {
			// Create the busy hour views for busy hours in this techpack.
			createBHViews(dwhtechpack);
		} else {
			log.info("Ignoring the BusyHour View creation as RANKBH tablelevel is restricted for custom Installaion.");
		}

		if (!customInstallFlag && !restrictedTableLevels.contains("RANKBH")) {
			// Create the busy hour views for busy hours referring to other
			// techpacks (custom).
			createBHViewsForCustomTechpack(dwhtechpack);
		} else {
			log.info("Ignoring the BusyHour View creation for Custom Techpacks as RANKBH tablelevel is restricted for custom Installaion.");
		}
		 
		// Create the CURRENT views for the CURRENT_DC tables.
		createCurrentViews(dwhtechpack);
		
		// Load data to vector reference table
		// And deleting old vector tables and replace with views
		if(newVectorFlag) {
			loadVectorReference(dwhtechpack);
			handleOldVectors(techPack);
		}
		// Update the vector counter data. (Old handling)
		else {
			updateVectorCounters(dwhtechpack);
		}
		
		if(!customInstallFlag && !restrictedTableLevels.contains("RANKBH")){
			// Update the busy hour counter data.
			updateBHCounters(dwhtechpack);
		} else {
			log.info("Ignoring the loading BusyHour counter data as RANKBH tablelevel is restricted for custom Installaion.");
		}

		if(!customInstallFlag && !restrictedTableLevels.contains("RANKBH")){
			//Update the busy hour counter data for custom techpacks.
			updateBHCountersForCustomTechpack(dwhtechpack);
		} else {
			log.info("Ignoring the loading BusyHour counter data for custom techpacks as RANKBH tablelevel is restricted for custom Installaion.");
		}

		// Execute the external statements
		executeExternalStatements(dwhtechpack, reDirectConnections);
		
		//Handle custom DC View 
		createCustomDCViews(dwhtechpack);
		
		LockTable lt = new LockTable(reprock, dcRock, log);
		List<String> newViews = lt.getAllView(techPack);
		if (newViews != null) {
			log.info("Size of newly created Views are " + newViews.size() + " and list are : " + newViews);
		} else {
			log.warning("No views found for " +techPack);
		}
	}

	/**
	 * Load vector reference data to the vector reference table
	 * 
	 * @param dwhtechpack
	 */
	private void loadVectorReference(final Dwhtechpacks dwhtechpack) {
		final String techPackName = dwhtechpack.getTechpack_name();
		final String tableName = DIM + techPackName.substring(techPackName.indexOf("_")) 
		+ "_VECTOR_REFERENCE";
		final File loadFile = new File(VECTORSFOLDER + "Tech_Pack_" + techPackName + ".txt");
		final String getCountSql = "Select count(*) from " + tableName;
		final String deleteSql = "Delete from " + tableName;
		final String loadSql = "LOAD TABLE " + tableName + 
				" (TABLE_COUNTER, DCVECTOR, DCRELEASE, QUANTITY, VALUE)" +
				" FROM '" + loadFile.getAbsolutePath() + "'" +
				" ESCAPES OFF" +
				" QUOTES OFF" +
				" DELIMITED BY '\t'" +
				" ROW DELIMITED BY '\n'" +
				" WITH CHECKPOINT ON;";
		
		if(!isVectorTP(dwhtechpack.getVersionid())) {
			log.info("There are no vector counters for Techpack " + dwhtechpack.getTechpack_name());
		}
		else if(!loadFile.exists()){
			log.severe("Load file " + loadFile.getAbsolutePath() + " for vector reference does not exist!");
		}
		else {
			int loadedRows = 0;
			Statement stmt = null;
			ResultSet count = null;
			try {
				stmt = this.dcRock.getConnection().createStatement();
				count = stmt.executeQuery(getCountSql);
				if (count.next()) {
					log.info(count.getInt(1) + " existing rows in " + tableName);
					if (count.getInt(1) != 0) {
						int deleteRows = stmt.executeUpdate(deleteSql);
						log.info("Deleted " + deleteRows + " rows from " + tableName);
					}
				}
				loadedRows = stmt.executeUpdate(loadSql);
				log.info(loadedRows + " rows loaded to " + tableName);
			}
			catch (SQLException sqlEx) {
				log.severe("Database update failed because " + sqlEx.getMessage());
			}
			catch (Exception ex) {
				log.severe("Loading rows failed because " + ex.getMessage());
			}
			finally {
				try {
					if (count != null) {
						count.close();
					}
					if (stmt != null) {
						stmt.close();
					}
				}
				catch (final Exception e) {
					log.warning("SQL Objects cleanup error " + e.getMessage());
				}
			}
		}
	}
	
	
	/**
	 * Checks if techpack has vector counters
	 * 
	 * @param versionid
	 * @return 	true if techpack has vector counters
	 * 			false if techpack does not have vector counters 
	 */
	private boolean isVectorTP(String versionid) {
		final String isVectorSql = "Select count(*)"
				+ " From MeasurementType"
				+ " Where VECTORSUPPORT = 1"
				+ " And VERSIONID = '" + versionid + "'";
		
		Statement stmt = null;
		ResultSet rsVector = null;
		try {
			log.finest("Running SQL Statement: " + isVectorSql);
			stmt = this.dwhrepRock.getConnection().createStatement();
			rsVector = stmt.executeQuery(isVectorSql);
			log.finest("Executed SQl statement:" + isVectorSql);
			if (rsVector.next()) {
				if (rsVector.getInt(1) != 0) {
					return true;
				}
			}
		}
		catch (SQLException sqlEx) {
			log.severe("Database update failed because " + sqlEx.getMessage());
		}
		catch (Exception ex) {
			log.severe("Loading rows failed because " + ex.getMessage());
		}
		finally {
			try {
				if (rsVector != null) {
					rsVector.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			}
			catch (final Exception e) {
				log.warning("SQL Objects cleanup error " + e.getMessage());
			}
		}

		return false;
	}

	/**
	 * Method to create custom DC Views which was taken backup during alter partition
	 * 
	 * @param dwhtechpack
	 */
	private void createCustomDCViews(final Dwhtechpacks dwhtechpack) {
		String techpackName = dwhtechpack.getTechpack_name();
		String customViewDir = "/eniq/backup/customViewStorage/";
		log.info("Handling Custom views for " + techpackName);
		try {
			long startTime = System.currentTimeMillis();
			File custDir = new File(customViewDir);
			Statement s = null;
			ResultSet rs = null;
			log.info("Going to check if any custom view backup has been taken for " +techpackName);
			if (custDir.exists() && custDir.isDirectory()) {
				for (String eachDir : custDir.list()) {
					log.info("Iterating directory : " +eachDir);
					if (eachDir.startsWith(techpackName + "_R")) {
						boolean dirDelete = true;
						File restoreDir = new File(customViewDir + eachDir);
						if (!restoreDir.exists() || !restoreDir.isDirectory()) {
							continue;
						}

						for (String eachCustomVFile : restoreDir.list()) {
							log.info("Reading SQL from file " +eachCustomVFile);
							String viewSQL = "";
							String eachLine = null;
							
							// Check view exists
							String viewName = eachCustomVFile.replace(".sql", "");
							String viewExistsSQL = "SELECT count(*) as VIEW_COUNT FROM SYSVIEWS WHERE viewname = '" + viewName + "' AND vcreator = 'dc'";
							try {
								s = dcRock.getConnection().createStatement();
								rs = s.executeQuery(viewExistsSQL);
								int existsCount = 0;
								if (rs != null){
									while (rs.next()){
										existsCount = rs.getInt("VIEW_COUNT");
									}
								}
								if (existsCount != 0 ) {
									log.info("View " + viewName + " already exists. Hence skipping view creation. ");
									// Delete view file
									File viewSQLFile = new File(restoreDir + File.separator + eachCustomVFile);
									if (!viewSQLFile.delete()){
										log.warning("Not able to delete custom view file :" + viewSQLFile.getName());
									}
									continue;
								} else {
									log.info("View " + viewName + " not exists. Creating now.");
								}
							} catch (SQLException e) {
								log.log(Level.WARNING, "Exception while check view exists. " , e);
							} finally {
								cleanUp(s, rs);
							}
							
							Path viewFile = Paths.get(restoreDir + File.separator + eachCustomVFile);
							Charset charset = Charset.forName("US-ASCII");
							try (BufferedReader reader = Files.newBufferedReader( viewFile, charset)) {
							    while ((eachLine = reader.readLine()) != null) {
							        viewSQL += eachLine;
							    }
							} catch (IOException x) {
							    log.log(Level.WARNING, "IOException while reading file " +viewFile + ". Reason : " , x);
							}
							if (viewSQL != null && viewSQL.length() > 0) {
								log.fine("Custom view SQL is " +viewSQL.replaceAll("\"", ""));
								try {
									s = dbaRock.getConnection().createStatement();
									s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
									s.execute(viewSQL.replaceAll("\"", ""));
									
									s = dcRock.getConnection().createStatement();
									rs = s.executeQuery(viewExistsSQL);
									int existsCount = 0;
									if (rs != null) {
										while (rs.next()) {
											existsCount = rs.getInt("VIEW_COUNT");
										}
									}
									if (existsCount != 0 ) {
										log.info("View creation for " + viewName + " is successful.");
										// Delete view file
										File viewSQLFile = new File(restoreDir + File.separator + eachCustomVFile);
										if (!viewSQLFile.delete()){
											log.warning("Not able to delete custom view file :" + viewSQLFile.getName());
										}
									} else {
										log.warning("View " + viewName + " creation failed. Please check the view text and create manually.");
										dirDelete = false;
									}
								} catch (SQLException e) {
									log.log(Level.WARNING, "Exception while creating custom view :  " , e);
									dirDelete = false;
								} finally {
									try {
										if (rs != null) {
											rs.close();
										}
										if ( s != null){
											s.close();
										}
									} catch (SQLException e) {
										log.log(Level.WARNING, "Not able to close statement in createCustomDCViews :  " , e);
									}
								}
							} else {
								log.warning("File " +eachCustomVFile+ " content is empty. Please check the custom view backup. ");
							}
						}
						// Delete Directory
						if (dirDelete){
							if (!restoreDir.delete()){
								log.warning("Not able to delete directory : " + restoreDir.getName());
							}
						}
					}
				}
				log.info("Total Time taken to restore custom views are : " + (System.currentTimeMillis() - startTime) + " ms. ");
			}
			
		} catch (Exception ex) {
			log.warning("General Exception during restore of Custom Views. " + ex.getStackTrace().toString());
			log.log(Level.WARNING, "Exception during restore of Custom Views :  " , ex);
		}
		
	}

	/**
	 * Remove counting management info (if applicable for the storage id) for
	 * the deleted partitions directly from the DB
	 * 
	 * @param storageId
	 *            the storage id that identifies the targeted partition set
	 * @param deleteList
	 *            the partitions to be deleted.
	 * @throws SQLException .
	 * @throws RockException .
	 */
	private void clearCountingPartitionInfo(final String storageId,
			final List<Dwhpartition> deleteList) throws SQLException,
			RockException {
		if (storageId.endsWith(Constants.TYPESEPARATOR + Constants.RAW)) {
			final Countingmanagement countingManagementCondition = new Countingmanagement(dwhrepRock);
			countingManagementCondition.setStorageid(storageId);
			final CountingmanagementFactory countingManagementFactory = new CountingmanagementFactory(
					dwhrepRock, countingManagementCondition);
			final Vector<Countingmanagement> countingManagementList = countingManagementFactory.get();
			for (final Dwhpartition partition : deleteList) {
				// Remove CountingManagment info for each partition from the DB.
				for (Countingmanagement element : countingManagementList) {
					if (element.getTablename().equals(partition.getTablename())) {
						element.deleteDB();
						CountingManagementCache.clearCache(storageId);
						break;
					}
				}
			}
		}
	}

	private void validateTimeBasedPartition(final Dwhtype type,
			final short partitionPlanType) throws Exception {
		if (partitionPlanType == TIME_BASED_PARTITION_TYPE
				&& type.getPartitionsize().intValue() > 0
				&& type.getPartitionsize().intValue() % 24 != 0) {
			throw new Exception("Type " + type.getTypename() + " ("
					+ type.getTablelevel() + ") " + type.getType()
					+ ": Partition size for tablelevel " + type.getTablelevel()
					+ " must be days (n * 24h).");
		}
	}

	private Partitionplan getPartitionPlan(final Dwhtype type) throws Exception {
		final Partitionplan wherePartitionPlan = new Partitionplan(this.dwhrepRock);
		wherePartitionPlan.setPartitionplan(type.getPartitionplan());
		final PartitionplanFactory partitionPlanFactory = new PartitionplanFactory(
				this.dwhrepRock, wherePartitionPlan);
		final Vector<Partitionplan> partPlanVector = partitionPlanFactory.get();
		if (partPlanVector.size() == 0) {
			throw new Exception("No default partitionplan named "
					+ type.getPartitionplan() + " found for type "
					+ type.getTypename() + ". Aborting execution.");
		}

		return partPlanVector.get(0);
	}

	/**
	 * Creates a Sanity Checker. Extracted out for testing purposes
	 * 
	 * @param reprock
	 *            repdb.dwhrep
	 * @param dwhrock
	 *            dwhdb.dc
	 * @param isNvu
	 *            is being called by NVU
	 * @return SanityChecker
	 */
	protected SanityChecker createSanityChecker(final RockFactory reprock,
			final RockFactory dwhrock, final boolean isNvu) {
		// Fix for TR - HN66650.
		return new SanityChecker(reprock, dwhrock, isNvu, this.clog);
	}

	protected Vector<Dwhtype> getDwhtypes(final String techPack)
			throws SQLException, RockException {
		// Only want ENABLED (ignore OBSOLETE)
		final String status = "ENABLED";
		return getDwhtypes(techPack, status);
	} // getDwhtypes

	protected Vector<Dwhtype> getDwhtypes(final String techPack,
			final String status) throws SQLException, RockException {
		final Dwhtype dt_cond = new Dwhtype(dwhrepRock);
		dt_cond.setTechpack_name(techPack);
		dt_cond.setStatus(status);
		// For Node Version Update - Filter out the exact meas type.
		filterMeasType(dt_cond);
		final DwhtypeFactory dt_fact = new DwhtypeFactory(dwhrepRock, dt_cond,
				" ORDER BY typename, tablelevel DESC ");
		return dt_fact.get();
	} // getDwhtypes

	/**
	 * Creates the view and returns the viewName
	 * 
	 * @param dwhrepConnectiontoRepdb
	 *            repdb.dwhrep
	 * @param dcConnectiontoDwhdb
	 *            dwhdb.dc
	 * @param dwhType
	 *            dwh type
	 * @param techPackType
	 *            stats or events techpack
	 * @return view name
	 * @throws Exception .
	 */
	protected String createView(final RockFactory dwhrepConnectiontoRepdb,
			final RockFactory dcConnectiontoDwhdb, final Dwhtype dwhType,
			final TechPackType techPackType) throws Exception {
		final CreateViewsAction createdView = new CreateViewsAction(
				this.dbaRock, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb,
				dwhType, this.clog, techPackType);
		return createdView.getViewName();
	}

	/**
	 * Extracted out for testing purposes
	 * 
	 * @return ActivationCache
	 */
	protected ActivationCache getActivationCache() {
		return ActivationCache.getCache();
	}

	private void createDaybhTempTable(final Dwhtype type) throws Exception {
		final Dwhcolumn dwhColumnCondition = new Dwhcolumn(dwhrepRock);
		dwhColumnCondition.setStorageid(type.getStorageid());
		final DwhcolumnFactory dwhColumnFactory = new DwhcolumnFactory(dwhrepRock,
				dwhColumnCondition);
		final Vector<Dwhcolumn> columns = dwhColumnFactory.get();
		sortColumns(columns);
		final Calendar cal = new GregorianCalendar();
		cal.setTime(new Date(0L));
		final Dwhpartition part = new Dwhpartition(dwhrepRock);
		part.setStorageid(type.getStorageid());
		part.setTablename(type.getBasetablename() + "_CALC");
		part.setStarttime(new Timestamp(cal.getTimeInMillis()));
		part.setEndtime(null);
		part.setStatus("ACTIVE");
		createPartition(type, part, columns);
		log.info("CALC partition created");
	}

	/**
	 * This method updates the vector counters
	 * 
	 * @param dwhtechpack
	 *            activated dwh techpack
	 */
	public void updateVectorCounters(final Dwhtechpacks dwhtechpack) {
		boolean deleteFolder = false;
		try {
			log.finest("Checking the directory:" + LOAD_FILE_LOC
					+ "exist or not");
			deleteFiles(LOAD_FILE_LOC, deleteFolder);
		} catch (Exception e) {
			log.severe(e.getMessage());
		}
		log.info("Updating Vector Counters for Techpack");
		final List<String> vectorTables = getVectorLoadTableInfo(
				dwhtechpack.getTechpack_name(), dwhtechpack.getVersionid());

		if (vectorTables.isEmpty()) {
			log.info("There are no vector counters for Techpack "
					+ dwhtechpack.getTechpack_name() + ".");
		} else {
			log.info("There are " + vectorTables.size()
					+ " vector counters for this Techpack.");

			final Iterator<String> iter = vectorTables.iterator();
			final Connection con = this.dcRock.getConnection();
			Statement stmt = null;
			String loadStatement = "";

			try {
				stmt = con.createStatement();
				final String temp = "set temporary option CONVERSION_ERROR = OFF;\n"
						+ "set temporary option escape_character = ON;\n"
						+ "commit;\n";
				stmt.execute(temp);

				while (iter.hasNext()) {
					loadStatement = iter.next();
					stmt.execute(loadStatement);
					log.fine("Vector Load Statement has been executed: "
							+ loadStatement);
				}

			} catch (SQLException e) {
				log.severe("Load Statement failed during upgrade/installation: "
						+ loadStatement);
				log.severe(e.getMessage());
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						log.info("Exception while closing statement "
								+ e.getMessage());
					}
			}
			}
			deleteSQLFilesVector();
			deleteFolder = true;
			try {
				deleteFiles(LOAD_FILE_LOC, deleteFolder);
			} catch (Exception e) {
				log.severe(e.getMessage());
			}
			
			}
	}

	/**
	 * Deletes the files created for the Vector Counters Bulk Loading
	 */
	private void deleteSQLFilesVector() {
		if (filesToDelete.isEmpty()) {
			log.info("No files to delete");
		} else {
			for (String tempFileName : filesToDelete) {
				final File temp = new File(tempFileName);
				log.fine("Deleting: " + tempFileName);
				temp.delete();
			}
		}
	}

	/**
	 * Delete the files if exist before updating Vector Counters Delete the
	 * folder after loading the all the files to Database
	 * 
	 * @param filename
	 *            : name of the folder
	 * @param deleteFolder
	 *            : this boolean values will be true once all files gets deleted
	 *            by method deleteSQLFilesVector
	 * @throws Exception .
	 */
	private void deleteFiles(final String filename, final boolean deleteFolder)
			throws Exception {

		final File file = new File(filename);

		if (!file.exists()) {
			log.info("No such file or directory exist: " + filename);
		}
		if (file.isDirectory()) {
			final File[] files = file.listFiles();
			if (files.length > 0) {
				for (File file1 : files) {
					file1.delete();
					log.finest("Deleted file:" + file1.getName());
				}
			}
			if (deleteFolder) {
				file.delete();
			}
		}
	}

	/**
	 * This retrieves the table name for the vector counter
	 * 
	 * @param dataName
	 *            dataname
	 * @param typeName
	 *            type name
	 * 
	 * @return vector counter table
	 */
	private String getDimTableName(final String dataName, final String typeName) {
		String dimTableName = "";
		if (dataName != null && typeName != null && !dataName.equals("")
				&& !typeName.equals("")) { // NOPMD : eeipca : ConfusingTernary
			dimTableName = DIM + typeName.substring(typeName.indexOf("_"))
					+ "_" + dataName;
		} else {
			log.warning("Dim Table could not be retrieves as dataname and type name are null");
		}

		log.finest("Vector table Name is: " + dimTableName);

		return dimTableName;
	}

	/**
	 * This method generates all the bulk load statements for vector counters
	 * for a techpack.
	 * 
	 * @param techpackName
	 *            techpackName
	 * @return list of bulk load statements
	 */
	private List<String> getVectorLoadTableInfo(final String techpackName,
			final String versionid) {

		log.info("Generating load statements for vector counters for "
				+ techpackName);
		final List<String> loadFiles = new ArrayList<String>();

		final StringBuffer sqlVectorTypes = new StringBuffer(100);
		sqlVectorTypes.append("Select TYPEID, TYPENAME ");
		sqlVectorTypes.append("From MeasurementType ");
		sqlVectorTypes.append("Where VECTORSUPPORT = 1 ");
		sqlVectorTypes.append("And VERSIONID = '");
		sqlVectorTypes.append(versionid);
		sqlVectorTypes.append("'");

		final Connection con = this.dwhrepRock.getConnection();
		Statement stmt = null;
		ResultSet rsVectorTypes = null;
		ResultSet rsDataName = null;

		try {
			log.finest("Running SQL Statement: " + sqlVectorTypes.toString());
			stmt = con.createStatement();
			rsVectorTypes = stmt.executeQuery(sqlVectorTypes.toString());

			log.finest("Executed SQl statement:" + sqlVectorTypes.toString());
			String typeID;
			String typeName;
			String dataName;
			String dimTableName;
			String fileName;
			final File installDir = new File(LOAD_FILE_LOC);
			installDir.mkdirs();

			while (rsVectorTypes.next()) {
				typeID = rsVectorTypes.getString("TYPEID");
				typeName = rsVectorTypes.getString("TYPENAME");

				final StringBuilder sqlDataName = new StringBuilder(100);
				sqlDataName.append("Select DATANAME ");
				sqlDataName.append("From MeasurementVector ");
				sqlDataName.append("Where TYPEID = '");
				sqlDataName.append(typeID);
				sqlDataName.append("' ");
				sqlDataName.append("Group By DATANAME");

				log.info("Finding vector data types for vector type: "
						+ typeName);
				log.finest("SQL: " + sqlDataName.toString());
				stmt = con.createStatement();
				rsDataName = stmt.executeQuery(sqlDataName.toString());

				while (rsDataName.next()) {
					dataName = rsDataName.getString("DATANAME");
					dimTableName = getDimTableName(dataName, typeName);
					fileName = LOAD_FILE_LOC + dimTableName + FILE_EXTENSION;
					filesToDelete.add(fileName);
					fileName = setLoadData(typeID, dimTableName, dataName,
							fileName);
					loadFiles.add(getLoadTableFile(techpackName, dataName,
							typeName, fileName));
				}
				if (stmt != null) {
					stmt.close();
				}
			}

		} catch (SQLException e) {
			log.warning("Cannot run SQL to retrieve Vector Types: "
					+ e.getMessage());
			log.warning("SQL: " + sqlVectorTypes);
		} finally {
			try {
				if (rsVectorTypes != null) {
					rsVectorTypes.close();
				}
				if (rsDataName != null) {
					rsDataName.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				//
			}
		}

		return loadFiles;
	}

	/**
	 * This method creates a Sybase IQ bulk load statement to load data into a
	 * table in the Data Warehouse.
	 * 
	 * @param techpackName
	 *            .
	 * @param dataName
	 *            .
	 * @param typeName
	 *            .
	 * @param fileName
	 *            .
	 * 
	 * @return loadFile
	 */
	private String getLoadTableFile(final String techpackName,
			final String dataName, final String typeName, final String fileName) {
		String loaderFile = "";
		try {
			final String dimTableName = DIM
					+ typeName.substring(typeName.indexOf('_')) + "_"
					+ dataName;

			final DateFormat dateFormat = new SimpleDateFormat(
					"yyyyMMdd-HH:mm:ss");

			final VelocityContext vctx = new VelocityContext();
			vctx.put("user_name", dcRock.getUserName());
			vctx.put("typename", dimTableName);
			vctx.put("meas", dataName);
			vctx.put("colDelimiter", "\t");
			vctx.put("TECHPACK", techpackName);
			vctx.put("LOG_DIR", System.getProperty("LOG_DIR"));
			vctx.put("REJECTED_DIR", System.getProperty("REJECTED_DIR"));
			vctx.put("FILE_NAME", fileName);
			vctx.put("DATE", dateFormat.format(new Date()));
			final StringWriter sqlWriter = new StringWriter();

			if (dimTableName.contains("pmRes")) {
				log.fine("Creating load statement for pmRes vector counter.");
				final boolean isMergeOk = Velocity.mergeTemplate(
						"vectorCounterpmRes.vm", Velocity.ENCODING_DEFAULT,
						vctx, sqlWriter);
				if (!isMergeOk) {
					throw new Exception("pmRes Velocity failed");
				}
			} else {
				log.fine("Creating load statement for vector counter.");
				final boolean isMergeOk = Velocity.mergeTemplate(
						"vectorCounter.vm", Velocity.ENCODING_DEFAULT, vctx,
						sqlWriter);
				if (!isMergeOk) {
					throw new Exception("Velocity failed");
				}
			}

			loaderFile = sqlWriter.toString();
			log.finest("Load Statement for " + dimTableName + " is "
					+ loaderFile);
		} catch (Exception e) {
			log.warning("Error creating vector counter data");
		}

		return loaderFile;
	}

	/**
	 * This method builds a file for bulk loading data into a vector counter
	 * table in the data warehouse.
	 * 
	 * @param typeID
	 *            .
	 * @param dimTableName
	 *            .
	 * @param dataName
	 *            .
	 * @param fileName
	 *            .
	 * 
	 * @return fileName
	 */
	private String setLoadData(final String typeID, final String dimTableName,
			final String dataName, final String fileName) {
		log.info("Creating insert data for " + dimTableName); // changed to info
																// for HO79350
		String loadData = "";

		final StringBuilder sql = new StringBuilder();
		final String selectVector = "Select VINDEX, VFROM, VTO, MEASURE, VENDORRELEASE ";
		String velocityTemplate;
		boolean isPmRes = false;
		Statement stmt = null;
		ResultSet rs = null;
		sql.append(selectVector);

		if (dimTableName.contains("pmRes")) {

			sql.append(", QUANTITY");
			velocityTemplate = "vectorCounterpmResLoadData.vm";
			isPmRes = true;
		} else {
			velocityTemplate = "vectorCounterLoadData.vm";
		}

		sql.append(" From MeasurementVector ");
		sql.append(" Where TYPEID = '");
		sql.append(typeID);
		sql.append("' ");
		sql.append(" And DATANAME = '");
		sql.append(dataName);
		sql.append("' ");
		int rowCount = 0;
		try {
			stmt = dwhrepRock.getConnection().createStatement();
			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {

				final VelocityContext vctx = new VelocityContext();

				vctx.put("index", rs.getInt("VINDEX"));
				vctx.put("value",
						rs.getString("VFROM") + " - " + rs.getString("VTO")
								+ " " + rs.getString("MEASURE"));
				vctx.put("vendorrelease", rs.getString("VENDORRELEASE"));
				vctx.put("colDelimiter", "\t");
				vctx.put("row_delimiter", "\n");
				if (isPmRes) {
					vctx.put("quantity", rs.getInt("QUANTITY"));
				}

				final StringWriter sqlWriter = new StringWriter();

				final boolean isMergeOk = Velocity.mergeTemplate(
						velocityTemplate, Velocity.ENCODING_DEFAULT, vctx,
						sqlWriter);

				if (!isMergeOk) {
					throw new Exception(velocityTemplate + " Velocity failed");
				}

				loadData += sqlWriter.toString();
				rowCount = rowCount + 1;
			}
		} catch (Exception e) {
			log.warning("Error creating vector counter data" + e.getMessage());
		} finally {
			cleanUp(stmt, rs);
		}

		log.finest("Data row count to write to table : " + dimTableName + "is:"
				+ rowCount);
		log.finest("Data to write to table: " + dimTableName + "\n" + loadData);

		final File loadFile = new File(fileName);
		try {
			final PrintWriter pw = new PrintWriter(new FileWriter(
					loadFile.getCanonicalFile()));
			pw.write(loadData);
			pw.close();
			pw.flush();

		} catch (FileNotFoundException e) {
			log.warning("File not found: " + fileName);
			log.warning(e.getMessage());
		} catch (IOException e) {
			log.warning(e.getMessage());
		}

		log.fine("File with data for " + dimTableName + " is " + fileName);
		return fileName;
	}

	/**
	 * Creates the busy hour views for the current techpack (busy hours pointing
	 * to ranking tables in the current techpack). Busyhour's that are enabled
	 * are skipped
	 * 
	 * @param dwhtechpack
	 *            .
	 */
	public void createBHViews(final Dwhtechpacks dwhtechpack) {
		log.log(Level.INFO, "Creating busy hour views for techpack "
				+ dwhtechpack.getTechpack_name() + ".");
		try {
			// Measurementtype
			final Measurementtype mt_cond = new Measurementtype(dwhrepRock);
			mt_cond.setVersionid(dwhtechpack.getVersionid());
			final MeasurementtypeFactory mt_condF = new MeasurementtypeFactory(
					dwhrepRock, mt_cond);
			final List<Measurementtype> mTypes = mt_condF.get();
			for (Measurementtype mt : mTypes) {
				try {
					createBhRankViews(mt);
				} catch (SQLException e) {
					log.warning("Error while creating views: " + e + " : "
							+ e.getNextException());
				} catch (Exception e) {
					log.warning("Error while creating views: " + e + " : "
							+ e.getMessage());
				}
			}
		} catch (Exception e) {
			log.warning("Error creating vector counter data");
		}
	}

	/**
	 * Generate the placeholder (Busyhour) view definition sql from the
	 * templates
	 * 
	 * @param bh
	 *            The Busyhour defining the view
	 * @param versionId
	 *            THe techpacks versionid
	 * @param tpName
	 *            The tech pack name
	 * @param reprock
	 *            repdb.dwhrep
	 * @return SQL to regenerate the view
	 * @throws Exception
	 *             Any errors
	 */
	public static String getPlaceholderCreateStatement(final Busyhour bh,
			final String versionId, final String tpName,
			final RockFactory reprock) throws Exception {
		initVelocity();
		String keyCounters = "";
		String keyValues = "";
		boolean first = true;
		// NOTE: BusyhourJoinColumn table is not used in the DB anymore,
		// so the where clause is not formulated based on joins anymore.
		String sources = "";
		String firstSource = "";
		final Busyhoursource bhs = new Busyhoursource(reprock);
		bhs.setVersionid(versionId);
		bhs.setBhlevel(bh.getBhlevel());
		bhs.setBhtype(bh.getBhtype());
		bhs.setBhobject(bh.getBhobject());
		bhs.setTargetversionid(bh.getTargetversionid());
		// eeoidiv 20100505, HL82220,Busyhour views not created if DIM_E_ table
		// returned first. (So ordering result)
		final BusyhoursourceFactory bhsF = new BusyhoursourceFactory(reprock,
				bhs, " ORDER BY TYPENAME ASC ");
		final List<Busyhoursource> bhResults = bhsF.get();
		for (Busyhoursource bsorce : bhResults) {
			if (first) {
				sources = " from " + bsorce.getTypename();
				firstSource = bsorce.getTypename() + ".";
				first = false;
			} else {
				sources += ", " + bsorce.getTypename();
			}
		}
		// keys
		final Busyhourrankkeys bhrk = new Busyhourrankkeys(reprock);
		bhrk.setVersionid(versionId);
		bhrk.setBhlevel(bh.getBhlevel());
		bhrk.setBhtype(bh.getBhtype());
		bhrk.setBhobject(bh.getBhobject());
		bhrk.setTargetversionid(bh.getTargetversionid());
		keyCounters += "ID";
		keyValues += "$ID";
		keyCounters += ", BHOBJECT";
		keyValues += ", '" + bh.getBhobject() + "'";
		final BusyhourrankkeysFactory bhrkF = new BusyhourrankkeysFactory(
				reprock, bhrk);
		for (Busyhourrankkeys bkey : bhrkF.get()) {
			keyCounters += ", " + bkey.getKeyname();
			keyValues += ", " + bkey.getKeyvalue();
		}
		final String viewName;
		if (bh.getBhelement() == 0) {
			viewName = bh.getBhlevel() + "_RANKBH_" + bh.getBhobject() + "_"
					+ bh.getBhtype();
		} else {
			viewName = tpName + "_ELEMBH_RANKBH_" + bh.getBhobject() + "_"
					+ bh.getBhtype();
		}
		final VelocityContext vctx = new VelocityContext();
		vctx.put("name", viewName);
		vctx.put("keyColumns", keyCounters);
		vctx.put("keyValues", keyValues);
		vctx.put("from", sources);
		vctx.put("from_COMMA", sources + ", ");
		// Where clause: If the 'where ' is missing from the beginning, it
		// will be added.
		String whereClause = bh.getWhereclause();
		if (whereClause == null) {
			vctx.put("where", "");
			vctx.put("where_ALLWAYS", "where ");
		} else {
			whereClause = whereClause.trim();
			if (whereClause.length() > 0) {
				if (whereClause.contains("where ")) {
					vctx.put("where", whereClause);
					if (bh.getBhelement() == 0) { // obj bh
						vctx.put("where_ALLWAYS", whereClause);
					} else { // elem bh
						vctx.put("where_ALLWAYS", whereClause + " and ");
					}
				} else {
					vctx.put("where", "where " + whereClause);
					vctx.put("where_ALLWAYS", "where " + whereClause + " and ");
				}
			} else {
				vctx.put("where", "");
				vctx.put("where_ALLWAYS", "where ");
			}
		}
		if (bh.getBhcriteria() == null) {
			vctx.put("bhcriteria", "null");
		} else {
			if (bh.getBhcriteria().trim().length() > 0) {
				vctx.put("bhcriteria", bh.getBhcriteria());
			} else {
				vctx.put("bhcriteria", "null");
			}
		}
		vctx.put("bhtype", "'" + bh.getBhobject() + "_" + bh.getBhtype() + "'");
		vctx.put("firsttable", firstSource);
		vctx.put("windowsize", bh.getWindowsize());
		vctx.put("offset", bh.getOffset());
		vctx.put("lookback", bh.getLookback());
		vctx.put("pthreshold", bh.getP_threshold());
		vctx.put("nthreshold", bh.getN_threshold());
		String template;
		if (bh.getAggregationtype() != null
				&& bh.getAggregationtype().length() > 0) {
			template = bh.getAggregationtype() + "_View.vm";
		} else {
			template = "RANKBH_View.vm";
		}
		final StringWriter sqlWriter = new StringWriter();
		try {
			final boolean isMergeOk = Velocity.mergeTemplate(template,
					Velocity.ENCODING_DEFAULT, vctx, sqlWriter);
			if (!isMergeOk) {
				throw new Exception("Velocity failed");
			}
		} catch (ResourceNotFoundException e) {
			throw new Exception(
					"Velocity failed in StorageTimeAction.getPlaceholderCreateStatement: "
							+ e.getMessage());
		}
		return sqlWriter.toString();
	}

	/**
	 * Recreate the BH placeholder views for RANKBH & ELEMBH Busyhours
	 * 
	 * @param mt
	 *            The measurement type the placeholders are defined for
	 * @throws Exception
	 *             If there were any errors setting up the view creation sql or
	 *             the sql execution fails
	 */
	public void createBhRankViews(final Measurementtype mt) throws Exception {
		// OBJBH
		final Measurementobjbhsupport mobhs = new Measurementobjbhsupport(
				dwhrepRock);
		mobhs.setTypeid(mt.getTypeid());
		final MeasurementobjbhsupportFactory mobhsF = new MeasurementobjbhsupportFactory(
				dwhrepRock, mobhs);
		if (mobhsF.get() != null && mobhsF.get().size() > 0) {
			final List<Measurementobjbhsupport> objBhSupports = mobhsF.get();
			for (Measurementobjbhsupport measObjbhs : objBhSupports) {
				// OBJBH
				final String bhlevel = mt.getTypename();
				// Busyhour
				final Busyhour obj_bh = new Busyhour(dwhrepRock);
				obj_bh.setVersionid(mt.getVersionid());
				obj_bh.setBhlevel(bhlevel);
				obj_bh.setBhobject(measObjbhs.getObjbhsupport());
				obj_bh.setBhelement(0); // 0 = obj
				final BusyhourFactory vc_condF = new BusyhourFactory(dwhrepRock,
						obj_bh);
				final List<Busyhour> bHours = vc_condF.get();
				for (Busyhour bh : bHours) {
					// eninkar: 06012011 HN29647: View will be created always if
					// Busy hour is enabled or not
					// Only check the condition that Busy hour criteria should
					// not be empty
					// OBJBH view creation starts
					if (bh.getBhcriteria() == null
							|| bh.getBhcriteria().length() == 0) {
						continue;
					}
					handleBHviewCreation(mt, bh);
				}
			}
		}
		// ELEMBH
		if (mt.getRankingtable() != null && mt.getElementbhsupport() != null
				&& mt.getElementbhsupport() > 0 && mt.getRankingtable() > 0) {
			// Busyhour
			final Busyhour elem_bh = new Busyhour(dwhrepRock);
			elem_bh.setVersionid(mt.getVersionid());
			elem_bh.setBhelement(1); // 1 = elem
			final BusyhourFactory vc_condF = new BusyhourFactory(dwhrepRock,
					elem_bh);
			final List<Busyhour> bhList = vc_condF.get();
			for (Busyhour bh : bhList) {
				// eninkar: 06012011 HN29647 View will be created always if Busy
				// hour is enabled or not
				// Only check the condition that Busy hour criteria should not
				// be empty
				if (bh.getBhcriteria() == null
						|| bh.getBhcriteria().length() == 0) {
					continue;
				}
				handleBHviewCreation(mt, bh);
			}
		}
	}

	/**
	 * Execute a series of SQL statements using the connection from the
	 * Rockfactory parameter
	 * 
	 * @param rf
	 *            The rockFactory to use to get the db connection
	 * @param statements
	 *            The SQL statements to execute
	 * @throws Exception
	 *             Id the SQL execution fails
	 */
	protected void executeSql(final RockFactory rf, final String... statements)
			throws Exception {
		final Statement s = rf.getConnection().createStatement();
		try {
			for (String sql : statements) {
				log.finest("Executing Statement : " + sql);
				s.execute(sql);
			}
		} finally {
			s.close();
		}
	}

	/**
	 * Generate & execute the sql needed to drop a Busyhour placeholder view
	 * from dwhdb
	 * 
	 * @param bh
	 *            The Busyhour defining the view/placeholder
	 * @throws Exception
	 *             If the drop view fails.
	 */
	public void dropBhRankViews(final Busyhour bh) throws Exception {
		final String viewName = bh.getBhlevel() + "_RANKBH_" + bh.getBhobject()
				+ "_" + bh.getBhtype();
		final String sql = "IF (SELECT count(*) FROM SYSVIEWS WHERE viewname='"
				+ viewName + "' AND vcreator='dc') > 0 " + "THEN\n DROP VIEW "
				+ viewName + ";\nEND IF;";
		executeSql(dcRock, sql);
	}

	/**
	 * Generate and create a Busyhour placeholder view
	 * 
	 * @param bh
	 *            The Busyhour defining the view
	 * @throws Exception
	 *             If the create fails
	 */
	public void createBhRankViews(final Busyhour bh) throws Exception {
		// get the MeasType for the BH
		final Measurementtype mt = new Measurementtype(dwhrepRock);
		mt.setVersionid(bh.getVersionid());
		mt.setTypename(bh.getBhlevel());
		final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock,
				mt);
		final List<Measurementtype> mts = mtf.get();
		if (mts.isEmpty()) {
			throw new Exception("No Measurementtypes found for "
					+ bh.getVersionid() + " " + bh.getBhlevel());
		}
		final Measurementtype toUse = mts.get(0);
		handleBHviewCreation(toUse, bh);
	}

	protected void handleBHviewCreation(final Measurementtype mt,
			final Busyhour bh) throws Exception {
		try {
			String bhsql;
			bhsql = getPlaceholderCreateStatement(bh, mt.getVersionid(),
					mt.getVendorid(), dwhrepRock);
			if (bhsql.length() > 0) {
				log.finest("bhSql:" + bhsql);
				bhsql = replaceDollarId(bhsql);
				log.finest("filtered bhSql:" + bhsql);
				// create the view
				executeSql(dcRock, bhsql);
			}
		} catch (Exception e) {
			log.warning("Getting error while creating views: " + e);
		}
	}

	/**
	 * Creates the views for busy hours for custom techpacks, i.e. techpacks
	 * with busy hours with ranking tables pointing to other techpacks.
	 * 
	 * @param dwhtechpack
	 *            active techpack
	 */
	public void createBHViewsForCustomTechpack(final Dwhtechpacks dwhtechpack) {
		log.log(Level.INFO, "Creating custom busy hour views for techpack "
				+ dwhtechpack.getTechpack_name() + ".");
		try {
			// Iterate through all busy hour entries for this techpack.
			final Busyhour obj_bh = new Busyhour(dwhrepRock);
			obj_bh.setVersionid(dwhtechpack.getVersionid());
			final BusyhourFactory vc_condF = new BusyhourFactory(dwhrepRock,
					obj_bh);
			for (Busyhour bh : vc_condF.get()) {
				// eninkar: 06012011 HN29647 View will be created always if Busy
				// hour is enabled or not
				// Only check the condition that Busy hour criteria should not
				// be empty
				if (bh.getBhcriteria() == null
						|| bh.getBhcriteria().length() == 0) {
					continue;
				}
				createCustomBhViewCreates(bh);
			}
		} catch (Exception e) {
			log.warning("Error while creating view creation clauses: " + e);
		}
	}

	public static String getCustomBhViewCreates(final Busyhour bh,
			final RockFactory reprock) throws Exception {
		// Get the busy hour sources
		final String sources = createBusyHourSourcesStatement(bh, reprock);
		String keyCounters = "ID, ";
		String keyValues = "$ID, ";
		keyCounters += "BHOBJECT, ";
		keyValues += "'" + bh.getBhobject() + "', ";
		// Get the rank keys
		keyCounters += createBusyHourRankKeyCountersStatement(bh, reprock);
		keyValues += createBusyHourRankKeysValuesStatement(bh, reprock);
		// Get view name based on if the busy hour is OBJBH or ELEMBH.
		String name;
		if (bh.getBhelement().equals(0)) {
			// OBJBH
			name = bh.getBhlevel() + "_RANKBH_" + bh.getBhobject() + "_"
					+ bh.getBhtype();
		} else {
			// ELEMBH
			final Versioning v = new Versioning(reprock, true);
			v.setVersionid(bh.getTargetversionid());
			final VersioningFactory vF = new VersioningFactory(reprock, v, true);
			final Versioning targetTP = vF.get().elementAt(0);
			name = targetTP.getTechpack_name() + "_ELEMBH_RANKBH_"
					+ bh.getBhobject() + "_" + bh.getBhtype();
		}
		// Get the where clause
		String whereClause = bh.getWhereclause();
		// Add information to the template
		final VelocityContext vctx = new VelocityContext();
		vctx.put("name", name);
		vctx.put("keyColumns", keyCounters);
		vctx.put("keyValues", keyValues);
		vctx.put("from", sources);
		vctx.put("from_COMMA", sources + ", ");
		vctx.put("bhtype", "'" + bh.getBhobject() + "_" + bh.getBhtype() + "'");
		// Add the first table from the sources statement, for example: ''.
		String firstSource;
		if (sources.contains(",")) {
			firstSource = sources.substring(sources.indexOf(" from ") + 6,
					sources.indexOf(",")) + ".";
		} else {
			firstSource = sources.substring(sources.indexOf(" from ") + 6)
					+ ".";
		}
		vctx.put("firsttable", firstSource);
		// Where clause: If the 'where ' is missing from the beginning, it
		// will be added.
		if (whereClause == null) {
			vctx.put("where", "");
			vctx.put("where_ALLWAYS", "where ");
		} else {
			whereClause = whereClause.trim();
			if (whereClause.length() > 0) {
				if (whereClause.contains("where ")) {
					vctx.put("where", whereClause);
					vctx.put("where_ALLWAYS", whereClause);
				} else {
					vctx.put("where", "where " + whereClause);
					vctx.put("where_ALLWAYS", "where " + whereClause + " and ");
				}
			} else {
				vctx.put("where", "");
				vctx.put("where_ALLWAYS", "where ");
			}
		}
		// Add busy hour criteria.
		if (bh.getBhcriteria() == null) {
			vctx.put("bhcriteria", "null");
		} else {
			if (bh.getBhcriteria().trim().length() > 0) {
				vctx.put("bhcriteria", bh.getBhcriteria());
			} else {
				vctx.put("bhcriteria", "null");
			}
		}
		vctx.put("windowsize", bh.getWindowsize());
		vctx.put("offset", bh.getOffset());
		vctx.put("lookback", bh.getLookback());
		vctx.put("pthreshold", bh.getP_threshold());
		vctx.put("nthreshold", bh.getN_threshold());
		String template;
		if (bh.getAggregationtype() != null
				&& bh.getAggregationtype().length() > 0) {
			template = bh.getAggregationtype() + "_View.vm";
		} else {
			template = "RANKBH_View.vm";
		}
		final StringWriter sqlWriter = new StringWriter();
		final boolean isMergeOk = Velocity.mergeTemplate(template,
				Velocity.ENCODING_DEFAULT, vctx, sqlWriter);
		if (!isMergeOk) {
			throw new Exception("Velocity failed");
		}
		return sqlWriter.toString() + "\n";
	}

	public void createCustomBhViewCreates(final Busyhour bh) throws Exception {
		String viewClauses = "";
		// Handle only the busy hours with target versionId being different than
		// the current versionId, since here only custom BH entries are taken
		// care of.
		if (!bh.getVersionid().equals(bh.getTargetversionid())) {
			// Add line feed to the end.
			viewClauses = getCustomBhViewCreates(bh, dwhrepRock);
		}
		// Create the views.
		Statement s = null;
		try {
			if (viewClauses.length() > 0) {
				log.finest("Creating views: " + viewClauses);
				viewClauses = replaceDollarId(viewClauses);
				log.finest("Filtered View Clauses:" + viewClauses);
				s = dcRock.getConnection().createStatement();
				s.execute(viewClauses);
			}
		} catch (Exception e) {
			log.warning("Error while creating views: " + e + " : "
					+ viewClauses);
		} finally {
			cleanUp(s, null);
		}
	}

	/**
	 * Refactored method to handle replacement of the $ID both in product and
	 * custom techpacks.
	 * 
	 * @param sql
	 *            sql string
	 * @return String
	 * @throws Exception .
	 */
	private String replaceDollarId(String sql) throws Exception {
		final String msql = "select id from LOG_BusyhourHistory where sql = '"
				+ sql.replace("'", "''") + "'";
		Statement ss = null;
		ResultSet rSet = null;
		boolean isNewID = false;
		long id = -1;
		try {
			ss = dcRock.getConnection().createStatement();
			rSet = ss.executeQuery(msql);
			if (rSet != null) {
				// noinspection LoopStatementThatDoesntLoop
				while (rSet.next()) {
					// we found a old value
					id = rSet.getLong("id");
					break;
				}
			}
		} catch (Exception e) {
			log.warning("Error while matching sql from LOG_BusyhourHistory: "
					+ e + " : " + msql);
		} finally {
			cleanUp(ss, rSet);
		}
		// if we did not find an old id.
		if (id == -1) {
			// get the next free id value from LOG_BusyhourHistory
			final String ssql = "select max(id) as maxId from LOG_BusyhourHistory";
			try {
				ss = dcRock.getConnection().createStatement();
				rSet = ss.executeQuery(ssql);
				if (rSet != null) {
					// noinspection LoopStatementThatDoesntLoop
					while (rSet.next()) {
						id = rSet.getLong("maxId");
						id++;
						// we have a new id
						isNewID = true;
						break;
					}
				}
				// if we did not found a new id, this must be the first id = 0
				if (id == -1) {
					id = 0;
				}
			} catch (Exception e) {
				log.warning("Error while getting maxid from LOG_BusyhourHistory: "
						+ e + " : " + ssql);
			} finally {
				cleanUp(ss, rSet);
			}
		}
		// if the id id too large we have a problem..
		if (id >= Long.MAX_VALUE) {
			throw new Exception("Id value too large " + id
					+ " for LOG_BusyhourHistory, could not store history data");
		}
		// if we have a new id (means a new sql)
		if (isNewID) {
			// insert the sql (with a id) to LOG_BusyhourHistory
			final String isql = "insert into LOG_BusyhourHistory (id, datetime, sql) values ("
					+ id
					+ ",'"
					+ SDF.format(new Date(System.currentTimeMillis()))
					+ "','"
					+ sql.replace("'", "''") + "');";
			try {
				ss = dcRock.getConnection().createStatement();
				ss.execute(isql);
			} catch (Exception e) {
				log.warning("Error while inserting busyhour history: " + e
						+ " : " + isql);
			} finally {
				cleanUp(ss, null);
			}
		}
		// now we have the id (old or new) so lets put it into the view..
		if (sql.indexOf("$ID") > 0) {
			sql = sql.replace("$ID", "" + id);
		}

		return sql;
	}

	/**
	 * Insert the BH data to the DIM-tables for busy hours for this techpack.
	 * 
	 * @param dwhtechpack
	 *            active techpack
	 */
	public void updateBHCounters(final Dwhtechpacks dwhtechpack) {

		log.log(Level.INFO, "Updating busy hour counters for techpack "
				+ dwhtechpack.getTechpack_name() + ".");

		String inserClauses = "";
		String deleteClauses = "";
		String deleteEmptyDesc = "";

		final Set<String> avoidDuplicateRows = new HashSet<String>();

		try {

			// Measurementtype
			final Measurementtype mt_cond = new Measurementtype(dwhrepRock);
			mt_cond.setVersionid(dwhtechpack.getVersionid());
			final MeasurementtypeFactory mt_condF = new MeasurementtypeFactory(
					dwhrepRock, mt_cond);

			//

			for (Measurementtype mt : mt_condF.get()) {

				final Measurementobjbhsupport mobhs = new Measurementobjbhsupport(
						dwhrepRock);
				mobhs.setTypeid(mt.getTypeid());
				final MeasurementobjbhsupportFactory mobhsF = new MeasurementobjbhsupportFactory(
						dwhrepRock, mobhs);

				if (mobhsF != null) {
					for (Measurementobjbhsupport measObjbhs : mobhsF.get()) {
						// Busyhour
						final Busyhour bh_cond = new Busyhour(dwhrepRock);
						// 20101209, eeeoidiv, HN30938 E11FOA:TR_no backward
						// compatility for reporting historydata in 3G
						// Want BHTYPE's for all previous and not just current
						// version for Reports.
						bh_cond.setBhlevel(mt.getTypename());
						bh_cond.setBhelement(0); // 0 = obj
						final BusyhourFactory vc_condF = new BusyhourFactory(
								dwhrepRock, bh_cond);
						for (Busyhour vc : vc_condF.get()) {
							// replace DC_X_XXX_YYY -> DIM_X_XXX_YYY_BHTYPE
							final String typename = DIM
									+ mt.getTypename().substring(
											mt.getTypename().indexOf("_"))
									+ "_BHTYPE";
							final VelocityContext vctx = new VelocityContext();
							final String desc = vc.getDescription();
							vctx.put("typename", typename);
							vctx.put("bhtype", measObjbhs.getObjbhsupport()
									+ "_" + vc.getBhtype());
							vctx.put("description", vc.getDescription());
							final StringWriter sqlWriter = new StringWriter();
							final boolean isMergeOk = Velocity.mergeTemplate(
									"BHCounter.vm", Velocity.ENCODING_DEFAULT,
									vctx, sqlWriter);
							if (!isMergeOk) {
								throw new Exception("Velocity failed");
							}
							// Insert
							final String insert = sqlWriter.toString() + "\n";
							if (!avoidDuplicateRows.contains(insert)) {
								inserClauses += insert;
								avoidDuplicateRows.add(insert);
							}
							final String delete = "DELETE FROM " + typename
									+ " WHERE BHTYPE='"
									+ measObjbhs.getObjbhsupport() + "_"
									+ vc.getBhtype() + "';\n";
							if (!avoidDuplicateRows.contains(delete)) {
								deleteClauses += delete;
								avoidDuplicateRows.add(delete);
							}
							if ((desc.length() > 0 && !desc
									.equalsIgnoreCase(" "))
									|| desc.equalsIgnoreCase(null)) {

								String del_query = "DELETE FROM " + typename
										+ " WHERE BHTYPE='"
										+ measObjbhs.getObjbhsupport() + "_"
										+ vc.getBhtype()
										+ "' AND DESCRIPTION='';\n";
								log.log(Level.FINEST,
										"Delete Empty Description query is : "
												+ del_query);
								deleteEmptyDesc += del_query;
							}

						}
					}
				}

				// ELEMBH
				if (mt != null && mt.getRankingtable() != null
						&& mt.getElementbhsupport() != null
						&& mt.getElementbhsupport() > 0
						&& mt.getRankingtable() > 0) {

					// Busyhour
					final Busyhour bh_cond = new Busyhour(dwhrepRock);
					// 20101209, eeeoidiv, HN30938 E11FOA:TR_no backward
					// compatility for reporting historydata in 3G
					// Want BHTYPE's for all previous and not just current
					// version for Reports.
					bh_cond.setBhlevel(mt.getTypename()); // Filter
															// Bhlevel=MeasurementType.TypeName
					bh_cond.setBhelement(1); // 1 = element
					final BusyhourFactory vc_condF = new BusyhourFactory(
							dwhrepRock, bh_cond);
					for (Busyhour vc : vc_condF.get()) {
						// replace DC_X_YYY -> DIM_X_YYY_ELEMBH_BHTYPE
						final String typename = DIM
								+ mt.getVendorid().substring(
										mt.getVendorid().indexOf("_"))
								+ "_ELEMBH_BHTYPE";
						final VelocityContext vctx = new VelocityContext();
						vctx.put("typename", typename);
						vctx.put("bhtype",
								vc.getBhobject() + "_" + vc.getBhtype());
						vctx.put("description", vc.getDescription());
						final StringWriter sqlWriter = new StringWriter();
						final boolean isMergeOk = Velocity.mergeTemplate(
								"BHCounter.vm", Velocity.ENCODING_DEFAULT,
								vctx, sqlWriter);
						if (!isMergeOk) {
							throw new Exception("Velocity failed");
						}
						// Insert
						final String insert = sqlWriter.toString() + "\n";
						if (!avoidDuplicateRows.contains(insert)) {
							inserClauses += insert;
							avoidDuplicateRows.add(insert);
						}
						final String delete = "DELETE FROM " + typename
								+ " WHERE BHTYPE='" + vc.getBhobject() + "_"
								+ vc.getBhtype() + "';\n";
						if (!avoidDuplicateRows.contains(delete)) {
							deleteClauses += delete;
							avoidDuplicateRows.add(delete);
						}
					}
				}
				executeSql(deleteClauses);
				executeSql(inserClauses);
				executeSql(deleteEmptyDesc);
				inserClauses = "";
				deleteClauses = "";
				deleteEmptyDesc = "";
				log.finest("Data inserted into BHTYPE and ELEMBH Tables");
			}
		} catch (Exception e) {
			log.warning("Error creating BH reference data: " + e);

		}

	}

	/**
	 * Insert the BH data to the DIM-tables for busy hours for this techpack.
	 * Since the data was not removed from the table in techpack deactivation,
	 * it is deleted before the insert.
	 * 
	 * @param dwhtechpack
	 *            active techpack
	 */
	public void updateBHCountersForCustomTechpack(final Dwhtechpacks dwhtechpack) {
		log.log(Level.INFO, "Updating custom busy hour counters for techpack "
				+ dwhtechpack.getTechpack_name() + ".");

		try {
			// Iterate through all busy hour entries for this techpack.
			final Busyhour obj_bh = new Busyhour(dwhrepRock);
			obj_bh.setVersionid(dwhtechpack.getVersionid());
			final BusyhourFactory vc_condF = new BusyhourFactory(dwhrepRock,
					obj_bh);
			String sqlClauses = "";
			for (Busyhour bh : vc_condF.get()) {
				// Handle only the busy hours with target versionId being
				// different than
				// the current versionId, since here only custom BH entries are
				// taken
				// care of.
				if (!bh.getVersionid().equals(bh.getTargetversionid())) {
					// Get type name based on if the busy hour is OBJBH or
					// ELEMBH.
					String typename;
					if (bh.getBhelement().equals(0)) {
						// OBJBH
						// Replace DC_X_XXX_YYY -> DIM_X_XXX_YYY_BHTYPE.
						typename = DIM
								+ bh.getBhlevel().substring(
										bh.getBhlevel().indexOf("_"))
								+ "_BHTYPE";
					} else {
						// ELEMBH
						// Replace DC_X_YYY -> DIM_X_YYY_ELEMBH_BHTYPE.
						final Versioning v = new Versioning(dwhrepRock, true);
						v.setVersionid(bh.getTargetversionid());
						final VersioningFactory vF = new VersioningFactory(
								dwhrepRock, v, true);
						final Versioning targetTP = vF.get().elementAt(0);
						final String targetName = targetTP.getTechpack_name();
						typename = DIM
								+ targetName.substring(targetName.indexOf("_"))
								+ "_ELEMBH_BHTYPE";
					}
					// Add the data to the template.
					final VelocityContext vctx = new VelocityContext();
					vctx.put("typename", typename);
					vctx.put("bhtype", bh.getBhobject() + "_" + bh.getBhtype());
					vctx.put("description", bh.getDescription());
					// Merge the template.
					final StringWriter sqlWriter = new StringWriter();
					final boolean isMergeOk = Velocity.mergeTemplate(
							"BHCounter.vm", Velocity.ENCODING_DEFAULT, vctx,
							sqlWriter);
					if (!isMergeOk) {
						throw new Exception("Velocity failed");
					}
					// Add the delete statement to the clauses. The row(s) are
					// deleted
					// because they were not removed in deactivation of a custom
					// techpack.
					// For a normal techpack the table was dropped in
					// deactivation, so the
					// data does not exist then.
					sqlClauses += "DELETE FROM " + typename + " WHERE BHTYPE='"
							+ bh.getBhobject() + "_" + bh.getBhtype() + "';\n";
					// Add the insert statement to the clauses.
					sqlClauses += sqlWriter.toString() + "\n";
				}
			}
			// Execute the SQL insert statements
			executeSql(sqlClauses);
		} catch (Exception e) {
			log.warning("Error creating BH reference data");
		}
	}

	/**
	 * Create the "CURRENT" views. For each reference table with dynamic update
	 * policy (2), a table has been created (for example:
	 * DIM_E_GRAN_BSC_CURRENT_DC). This method creates the views for these
	 * tables (for example: DIM_E_GRAN_BSC_CURRENT).
	 * 
	 * @param dwhtechpack
	 *            active techpack
	 */
	public void createCurrentViews(final Dwhtechpacks dwhtechpack) {
		log.log(Level.INFO,
				"Creating '_CURRENT' views for reference tables with dynamic OR timed dynamic update policy for techpack "
						+ dwhtechpack.getTechpack_name() + ".");
		// A vector for storing the reference type names.
		final Vector<String> rtypeNames = new Vector<String>();
		// Get all the reference table names for which the CURRENT views should
		// be created for. These reference tables have "dynamic" update policy.
		try {
			final Referencetable rt_cond = new Referencetable(dwhrepRock);
			rt_cond.setVersionid(dwhtechpack.getVersionid());
			final ReferencetableFactory rt_fact = new ReferencetableFactory(
					dwhrepRock, rt_cond);
			final Vector<Referencetable> rtypes = rt_fact.get();
			for (Referencetable rt : rtypes) {
				// eeoidiv 20091203 : Timed Dynamic topology handling in ENIQ,
				// WI 6.1.2,
				// (284/159 41-FCP 103 8147) Improved WRAN Topology in ENIQ
				// Store the name if the update policy is dynamic(2) or timed
				// dynamic(3)
				// and if name is not already stored.
				// 20110830 EANGUAN :: Adding comparison for policy number 4 for
				// History Dynamic (for SON)
				if ((rt.getUpdate_policy() == 2)
						|| (rt.getUpdate_policy() == 3)
						|| (rt.getUpdate_policy() == 4)) {
					final String typeName = rt.getTypename() + "_CURRENT";
					if (!rtypeNames.contains(typeName)) {
						rtypeNames.add(typeName);
					}
				}
			}
		} catch (Exception e) {
			log.warning("Error collecting reference table data for CURRENT view creation.");
		}

		// The view clauses used for creating all the views.
		String viewClauses = "";
		// Create the view creation clauses
		try {
			for (String name : rtypeNames) {
				// Create the velocity context and add the parameters.
				final VelocityContext vctx = new VelocityContext();
				vctx.put("name", name);
				// Merge the context with the template.
				final StringWriter sqlWriter = new StringWriter();
				final boolean isMergeOk = Velocity.mergeTemplate(
						"CurrentView.vm", Velocity.ENCODING_DEFAULT, vctx,
						sqlWriter);
				// Check if merge was successful
				if (!isMergeOk) {
					throw new Exception("Velocity failed for view: " + name);
				}
				// Add the successfully created clause to the the list of view
				// creation
				// clauses.
				viewClauses += sqlWriter.toString() + "\n";
			}
		} catch (Exception e) {
			log.warning("Error while creating views: " + e);
			viewClauses = "";
		}
		// Create the views from the clauses (if any).
		Statement s = null;
		try {
			if (viewClauses.length() > 0) {
				log.info("Creating current views: " + viewClauses);
				s = dcRock.getConnection().createStatement();
				s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
				s.execute(viewClauses);
			}
		} catch (Exception e) {
			log.warning("Error while creating views: " + e + " : "
					+ viewClauses);
		} finally {
			cleanUp(s, null);
		}
	}

	/**
	 * @param insertClauses
	 *            sql
	 */
	private void executeSql(final String insertClauses) {
		if (!insertClauses.trim().isEmpty()) {
			final String[] sqls = insertClauses.split(";\n");
			int i = 0;
			while (i < sqls.length) {
				String sqlTmp = "";
				int max = i + ROWSPEREXECUTE;
				if (max > sqls.length) {
					max = sqls.length;
				}

				for (int ii = i; ii < max; ii++) {
					if (sqls[ii].length() > 0) {
						sqlTmp += sqls[ii] + ";\n";
					}
				}
				i = max;
				log.finest("SQL: " + sqlTmp);
				if (sqlTmp.length() > 0) {
					Statement s = null;
					try {
						log.finest("Inserting data to DB: " + sqlTmp);
						s = dcRock.getConnection().createStatement();
						s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
						s.execute(sqlTmp);
					} catch (Exception e) {
						log.severe("Error in SQL: " + sqlTmp);
					} finally {
						cleanUp(s, null);
					}
				}
			}
		}
	}

	/**
	 * @param conStr
	 *            db url
	 * @return url split to host port name?
	 */
	private String[] getURL(final String conStr) {
		final int s = conStr.lastIndexOf(":", conStr.lastIndexOf(":") - 1) + 1;
		final int e = conStr.lastIndexOf(":");
		final String s3[] = new String[3];
		s3[0] = conStr.substring(0, s);
		s3[1] = conStr.substring(s, e);
		s3[2] = conStr.substring(e);
		return s3;
	}

	/**
	 * @param techpack
	 *            active techpack
	 * @param reDirectConnections
	 *            .
	 */
	private void executeExternalStatements(final Dwhtechpacks techpack,
			final boolean reDirectConnections) {
		log.log(Level.INFO, "Executing external statements for techpack "
				+ techpack.getTechpack_name() + ".");
		try {
			final Externalstatement es_cond = new Externalstatement(dwhrepRock);
			es_cond.setVersionid(techpack.getVersionid());
			 if(customInstallFlag)
				 es_cond.setDbconnection("dwh");
			final ExternalstatementFactory es_fact = new ExternalstatementFactory(
					dwhrepRock, es_cond);
			final Vector<Externalstatement> ess = es_fact.get();
			if (ess != null && ess.size() > 0) {
				log.info("Techpack has " + ess.size() + " external statements");
			} else {
				log.fine("No external statements");
				return;
			}
			Collections.sort(ess, new Comparator<Externalstatement>() {

				@Override
				public int compare(final Externalstatement e1,
						final Externalstatement e2) {
					return e1.getExecutionorder().compareTo(
							e2.getExecutionorder());
				}

				// public boolean equals(Object o1, Object o2) {
				// final Externalstatement e1 = (Externalstatement) o1;
				// final Externalstatement e2 = (Externalstatement) o2;
				// return e1.getExecutionorder().equals(e2.getExecutionorder());
				// }
			});
			final Externalstatementstatus ess_cond = new Externalstatementstatus(
					dwhrepRock);
			ess_cond.setVersionid(techpack.getVersionid());
			final ExternalstatementstatusFactory ess_fact = new ExternalstatementstatusFactory(
					dwhrepRock, ess_cond);
			final Vector<Externalstatementstatus> esss = ess_fact.get();
			final Map<String, Externalstatementstatus> executed = new HashMap<String, Externalstatementstatus>();
			if (esss != null && esss.size() > 0) {
				for (Externalstatementstatus tess : esss) {
					executed.put(tess.getStatementname(), tess);
				}
			}
			log.fine(executed.size()
					+ " external statements already executed for techpack "
					+ techpack.getTechpack_name());
			final Meta_databases md_cond = new Meta_databases(etlrepRock);
			md_cond.setType_name("USER");
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(
					etlrepRock, md_cond);
			final Vector<Meta_databases> cons = md_fact.get();
			if (cons != null && cons.size() > 0) {
				log.fine("Found " + cons.size() + " database connections");
			} else {
				log.warning("Cannot execute external statements: No db connections found");
			}
			List<String> restrictedExternalStatementNames = new ArrayList<String>();
			restrictedExternalStatementNames.addAll(restrictedTableLevels);
			if(restrictedTableLevels.contains("COUNT")) 
				restrictedExternalStatementNames.add("DELTA");
			for (Externalstatement es : ess) {
				boolean discontinueFlag = false;
				for(String each: restrictedExternalStatementNames){
				if(es.getStatementname().toUpperCase().contains(each.toUpperCase())) {
					discontinueFlag = true;
					break;
				}
			}
			if(discontinueFlag){
				log.info("Ignoring the external statement for the execution as this tablelevel is restricted for custom Installaion.");
				continue;
			}
				Externalstatementstatus xess = executed.get(es
						.getStatementname());
				if (xess != null && "OK".equalsIgnoreCase(xess.getStatus())) {
					log.fine("Statement " + es.getStatementname()
							+ " already successfully executed");
					log.fine("Executing external statements again");
				}
				try {
					log.fine("Trying to execute " + es.getStatementname());
					boolean success = true;
					Connection con = null;
					Statement stmt = null;
					try {
						final String conName = es.getDbconnection();
						log.finest("Connection name " + conName);
						Meta_databases md = null;
						for (Meta_databases xmd : cons) {
							if (xmd.getConnection_name().equalsIgnoreCase(
									conName)) {
								md = xmd;
								break;
							}
						}
						if (md == null) {
							throw new Exception("Connection " + conName
									+ " not found");
						} else {
							log.fine("Connection " + conName + " found");
						}
						RockFactory rook;
						if (reDirectConnections) {
							final String c[] = getURL(md.getConnection_string());
							final String o[] = getURL(dwhrepRock.getDbURL());
							final String n = c[0] + o[1] + c[2];
							log.fine("Re-Directing " + conName + " from "
									+ md.getConnection_string() + " to " + n);
							rook = new RockFactory(n, md.getUsername(),
									md.getPassword(), md.getDriver_name(),
									"DWHMStrTime", false);
						} else {
							rook = new RockFactory(md.getConnection_string(),
									md.getUsername(), md.getPassword(),
									md.getDriver_name(), "DWHMStrTime", false);
						}
						con = rook.getConnection();
						log.fine("Connection to " + conName + " established");
						con.setAutoCommit(false);
						stmt = con.createStatement();
						stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
						stmt.executeUpdate(es.getStatement());
						stmt.close();
						con.commit();
						log.info("Statement " + es.getStatementname()
								+ " successfully executed");
					} catch (Exception e) {
						log.log(Level.WARNING,
								"Statement " + es.getStatementname()
										+ " failed", e);
						if (con != null && stmt != null) {
							try {
								con.rollback();
							} catch (Exception ex) {
								log.log(Level.WARNING, "Rollback failed", e);
							}
						}
						success = false;
					} finally {
						cleanUp(stmt, null);
						try {
							con.close();
						} catch (Exception e) {
							//
						}
					}
					if (xess == null) {
						xess = new Externalstatementstatus(dwhrepRock);
						xess.setTechpack_name(techpack.getTechpack_name());
						xess.setStatementname(es.getStatementname());
						xess.setVersionid(techpack.getVersionid());
						xess.setStatus(success ? "OK" : "ERROR");
						xess.setExectime(new Timestamp(System
								.currentTimeMillis()));
						xess.setExecstatement(es.getStatement());
						xess.saveDB();
					} else {
						xess.setVersionid(techpack.getVersionid());
						xess.setStatus(success ? "OK" : "ERROR");
						xess.setExectime(new Timestamp(System
								.currentTimeMillis()));
						xess.setExecstatement(es.getStatement());
						xess.updateDB();
					}
					log.fine("Status successfully updated");
				} catch (Exception e) {
					log.log(Level.WARNING,
							"Unexpected error during external statement execution",
							e);
				}
			} // foreach externalstatement
		} catch (Exception e) {
			log.log(Level.WARNING,
					"External statement execution failed exceptionally", e);
		}
	}

	/**
	 * Checks that partition exists for simple type if not creates new
	 * partition.
	 * 
	 * @param type
	 *            active measurement type
	 * @throws Exception .
	 */
	private void adjustSimple(final Dwhtype type) throws Exception {
		log.info("Adjusting SIMPLE " + type.getTypename());
		final Dwhpartition dp_cond = new Dwhpartition(dwhrepRock);
		dp_cond.setStorageid(type.getStorageid());
		final DwhpartitionFactory dp_fact = new DwhpartitionFactory(dwhrepRock,
				dp_cond);
		final Vector<Dwhpartition> partitions = dp_fact.get();
		if (partitions.size() < 1) { // Create partition
			log.info("Need to create partition for " + type.getTypename());
			final Dwhcolumn dc_cond = new Dwhcolumn(dwhrepRock);
			dc_cond.setStorageid(type.getStorageid());
			final DwhcolumnFactory dc_fact = new DwhcolumnFactory(dwhrepRock,
					dc_cond);
			final Vector<Dwhcolumn> columns = dc_fact.get();
			sortColumns(columns);
			final Calendar cal = new GregorianCalendar();
			cal.setTime(new Date(0L));
			final Dwhpartition part = new Dwhpartition(dwhrepRock);
			part.setStorageid(type.getStorageid());
			part.setTablename(type.getBasetablename());
			part.setStarttime(new Timestamp(cal.getTimeInMillis()));
			part.setEndtime(null);
			part.setStatus("ACTIVE");
			final boolean partitionStatus = createPartition(type, part, columns);
			if (partitionStatus) {
				log.fine("partitionStatus is " + partitionStatus
						+ " for table: " + type.getBasetablename()
						+ ".Dwhpartition created and stored");
				part.insertDB();
			} else {
				log.info("partitionStatus is " + partitionStatus
						+ " for table: " + type.getBasetablename());
			}
		} else if (partitions.size() > 1) {
			throw new Exception("PANIC: Simple type " + type.getStorageid()
					+ " has multiple partitions");
		} else {
			log.fine("No need to add/remove partitions for type "
					+ type.getTypename());
		}
	}

	/**
	 * Checks that partition exists for unpartitioned type.
	 * 
	 * @param type
	 *            active measurment type
	 * @param partitionPlanType
	 *            volume or time
	 * @return List of partitions to be deleted. Null if no partition alteration
	 *         performed. (No need to update view)
	 * @throws Exception
	 *             in case of failure
	 */
	private List<Dwhpartition> adjustUnpartitioned(final Dwhtype type,
			final short partitionPlanType) throws Exception {
		log.info("Adjusting UNPARTITIONED " + type.getTypename());
		final List<Dwhpartition> tobedeleted = new ArrayList<Dwhpartition>();
		final Dwhpartition dp_cond = new Dwhpartition(dwhrepRock);
		dp_cond.setStorageid(type.getStorageid());
		final DwhpartitionFactory dp_fact = new DwhpartitionFactory(dwhrepRock,
				dp_cond);
		final Vector<Dwhpartition> partitions = dp_fact.get();
		sortPartitions(partitionPlanType, partitions);
		log.fine(type.getType() + " has " + partitions.size() + " partitions.");
		// Unpartitioned type should have only ONE partition
		if (partitions.size() < 1) { // Create partition
			log.info("Need to create partition for " + type.getTypename());
			final Dwhcolumn dc_cond = new Dwhcolumn(dwhrepRock);
			dc_cond.setStorageid(type.getStorageid());
			final DwhcolumnFactory dc_fact = new DwhcolumnFactory(dwhrepRock,
					dc_cond);
			final Vector<Dwhcolumn> columns = dc_fact.get();
			sortColumns(columns);
			final Calendar cal = new GregorianCalendar();
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			final Dwhpartition part = new Dwhpartition(dwhrepRock);
			part.setStorageid(type.getStorageid());
			part.setTablename(type.getBasetablename() + "_00");
			part.setStarttime(new Timestamp(cal.getTimeInMillis()));
			part.setEndtime(null);
			part.setStatus("ACTIVE");
			createPartition(type, part, columns);
			part.insertDB();
			log.fine("Dwhpartition created and stored");
		} else if (partitions.size() > 1) { // Delete partitions
			log.info("Need to delete partition(s) of " + type.getTypename());
			while (partitions.size() > 1) {
				final Dwhpartition delpart = partitions
						.get(partitions.size() - 1);
				delpart.deleteDB();
				tobedeleted.add(delpart);
			}
		} else {
			log.fine("No need to add/remove partitions for type "
					+ type.getTypename());
			return null;
		}
		return tobedeleted;
	}

	/**
	 * Adjusts amount of partitions to reflect desired storage time. "Normal"
	 * exceptions are handled.
	 * 
	 * @param type
	 *            Target DWHType object to use.
	 * @param desired_partitions
	 *            The number of partitions that should be created for this Type
	 * @param usingDefPartitionSize
	 *            informs if the value in type.partitionsize is the
	 *            partitionplans partitionsize value.
	 * @param partitionPlanType
	 *            volume or storage based partitions
	 * @return List of partitions to be deleted. Null if no partition alteration
	 *         performed. (No need to update view)
	 * @throws Exception
	 *             in case of failure
	 */
	private List<Dwhpartition> adjustPartitioned(final Dwhtype type,
			final int desired_partitions, final boolean usingDefPartitionSize,
			final short partitionPlanType) throws Exception {
		log.info("Adjusting PARTITIONED " + type.getStorageid());
		final Long defaultPartitionSize = type.getPartitionsize();
		final List<Dwhpartition> tobedeleted = new ArrayList<Dwhpartition>();

		final Dwhpartition dwp_cond = new Dwhpartition(dwhrepRock);
		dwp_cond.setStorageid(type.getStorageid());
		final DwhpartitionFactory dwp_fact = new DwhpartitionFactory(dwhrepRock, dwp_cond);
		final Vector<Dwhpartition> dwps = dwp_fact.get();
		final Iterator<Dwhpartition> i_dwps = dwps.iterator();
		while (i_dwps.hasNext()) {
			final Dwhpartition dwp = i_dwps.next();
			if (debug) {
				log.fine("DWH partition: " + dwp.getTableName() + "\tStatus:"
						+ dwp.getStatus() + "\tStartTime:" + dwp.getStarttime()
						+ "\tEndTime:" + dwp.getEndtime());
			}
			if ("MANUAL".equalsIgnoreCase(dwp.getStatus())
					|| "INSANE_MA".equalsIgnoreCase(dwp.getStatus())) {
				if (debug) {
					log.fine("Partition removed from list");
				}
				i_dwps.remove();
			} else if ("MIGRATED".equalsIgnoreCase(dwp.getStatus())
					|| "INSANE_MG".equalsIgnoreCase(dwp.getStatus())) {
				if (debug) {
					log.fine("Partition removed from list");
				}
				i_dwps.remove();
				if (dwp.getStarttime() == null && partitionPlanType == TIME_BASED_PARTITION_TYPE) {
					log.info("Found " + dwp.getStatus() + " table to be dropped");
					tobedeleted.add(dwp);
				}
			}
		}
		
		log.finer("Partitions exists " + dwps.size() + " while " + desired_partitions + " needed");
		if (dwps.size() < desired_partitions) { // More partitions
			log.info(type.getStorageid() + ": Increasing amount of partitions");
			final Dwhcolumn dc_cond = new Dwhcolumn(dwhrepRock);
			dc_cond.setStorageid(type.getStorageid());
			final DwhcolumnFactory dc_fact = new DwhcolumnFactory(dwhrepRock, dc_cond);
			final Vector<Dwhcolumn> columns = dc_fact.get();
			sortColumns(columns);
			if (debug) {
				final Iterator<Dwhpartition> it = dwps.iterator();
				int index = 0;
				while (it.hasNext()) {
					final Dwhpartition dwp = it.next();
					if (debug) {
						log.fine("#" + index + ": DWH partition: " + dwp.getTableName() + "\tStatus:"
								+ dwp.getStatus() + "\tStartTime:" + dwp.getStarttime() + "\tEndTime:"
								+ dwp.getEndtime());
					}
					index++;
				}
			}
			
			int nextpartitionnumber = determineNextPartitionNumber(type);
			while (dwps.size() < desired_partitions) {
				final StringBuilder tablename = new StringBuilder(type.getBasetablename()).append("_");
				if (nextpartitionnumber <= 9) {
					tablename.append("0");
				}
				tablename.append(nextpartitionnumber++);
				final Dwhpartition part = new Dwhpartition(dwhrepRock);
				part.setStorageid(type.getStorageid());
				part.setTablename(tablename.toString());
				part.setEndtime(null);

				if (partitionPlanType == TIME_BASED_PARTITION_TYPE) {
					part.setStarttime(null);
					part.setStatus("NEW");
				} else {
					part.setStarttime(new Timestamp(System.currentTimeMillis()));
					part.setStatus("ACTIVE");
				}
				createPartition(type, part, columns);
				if (!debug) {
					part.saveDB();
				}
				dwps.add(part);
				if (debug) {
					log.fine("DWH partition: " + part.getTableName() + "\tStatus:" + part.getStatus() + "\tStartTime:"
							+ part.getStarttime() + "\tEndTime:" + part.getEndtime() + " created and stored");
				} else {
					log.fine("Dwhpartition created and stored");
				}
				type.setPartitioncount((long) dwps.size());
				if (usingDefPartitionSize) {
					type.setPartitionsize((long) -1);
				}
				if (!debug) {
					type.updateDB();
				}
			} // while too few partitions
		} else if (dwps.size() > desired_partitions) { // Less partitions
			log.info(type.getStorageid() + ": Reducing amount of partitions");
			sortPartitions(partitionPlanType, dwps);
			log.finest("Order of partitions while reducing partitions:");
			final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			for (final Dwhpartition currentPart : dwps) {
				// NOPMD : eeipca : ConfusingTernary
				if (currentPart.getStarttime() != null && currentPart.getEndtime() != null) {
					log.finest(currentPart.getTablename() + " Timespan: "
							+ dateFormat.format(currentPart.getStarttime())
							+ " - " + dateFormat.format(currentPart.getEndtime()));
				} else {
					log.finest(currentPart.getTablename() + " Timespan: contains NULL.");
				}
			}
			while (dwps.size() > desired_partitions) {
				final Dwhpartition delpart = dwps.remove(dwps.size() - 1);
				if (!debug) {
					delpart.deleteDB();
				}
				type.setPartitioncount((long) dwps.size());
				if (usingDefPartitionSize) {
					type.setPartitionsize((long) -1);
				}
				if (!debug) {
					type.updateDB();
				}
				tobedeleted.add(delpart);
				if (debug) {
					log.fine("DWH partition: " + delpart.getTableName()
							+ "\tStatus:" + delpart.getStatus()
							+ "\tStartTime:" + delpart.getStarttime()
							+ "\tEndTime:" + delpart.getEndtime()
							+ " scheduled for deletion");
				} else {
					log.finer("Partition " + delpart.getTablename()
							+ " scheduled for deletion");
				}
			} // while too many partitions
		} else {
			log.fine("No need to add/remove partitions for type "
					+ type.getTypename());
		}
		
		if(customInstallFlag){
			final Vector<Dwhpartition> dwhp = dwp_fact.get();
			if (usingDefPartitionSize) {
				type.setPartitionsize(defaultPartitionSize);
			}
			pa.firstPartitioning(type, dwhp);
		
			if (usingDefPartitionSize) { 
				type.setPartitionsize((long) -1);
			}
		}
		return tobedeleted;
	}

	/**
	 * Get the number of partitions that should be created for a particular
	 * dwhType
	 * 
	 * @param type
	 *            The type that the partitions need to be created for
	 * @param desired_storage
	 *            If partition plan is volume base, then this represents the
	 *            number of rows stored in all partitions If partition plan is
	 *            time base, then this represents the number of days stored in
	 *            all partitions
	 * @param partitionPlanType
	 *            The type of partition plan for this this dwhType
	 * @return The number of partitions that should be created for a particular
	 *         dwhType
	 */
	private int getDesiredNumberOfPartitions(final Dwhtype type,
			final long desired_storage, final short partitionPlanType) {
		int desired_partitions;
		final long partition_size = type.getPartitionsize(); // hours or rows
		if (partitionPlanType == VOLUME_BASED_PARTITION_TYPE) {
			// Desired_storage = max number of rows stored across all the
			// partitions.
			// partition_size = number of rows stored per partitions.
			// Dividing these two numbers will give number of partitions to be
			// created.
			// One more partition is added to give an extra buffer
			desired_partitions = (int) Math.ceil((double) desired_storage
					/ (double) partition_size) + 1;
		} else {
			long storagetime_hours = desired_storage * 24; // hours
			log.finer("Storagetime " + storagetime_hours
					+ " hours, while partition size " + partition_size
					+ " hours");
			// Adding 48 h to storage time because partition change happens 48h
			// before
			// last partition is closed. Otherwise there could be change that in
			// case
			// of
			// partition change we could not quarantee storagetime.
			storagetime_hours += 48;
			// storagetime_hours = max number of hours stored across all the
			// partitions.
			// partition_size = number of hours per partitions.
			// Dividing these two numbers will give number of partitions to be
			// created. One more partition is added to give an extra buffer
			desired_partitions = (int) Math.ceil((double) storagetime_hours
					/ (double) partition_size) + 1;
		}
		return desired_partitions;
	}

	/**
	 * Deletes the listed partitions.
	 * 
	 * @param partitions
	 *            partitions to delete
	 * @return List of partitions failed to delete
	 */
	private List<Dwhpartition> deletePartitions(final List<Dwhpartition> partitions) {
		final Iterator<Dwhpartition> it = partitions.iterator();
		String partitionToDelete = "";
		Statement stmt = null;
		ResultSet res = null;
		
		while (it.hasNext()) {
			partitionToDelete += it.next().getTablename() + ",";
		}
		
		if (partitionToDelete.length() > 0) {
			partitionToDelete = partitionToDelete.substring(0, partitionToDelete.length() - 1);
		}
		
		String deleteProcSql = "call dba.forceDropTableList('" + partitionToDelete + "');";
		log.finer("Deleting partition SQL contains " + deleteProcSql);
		
		try {
			stmt = dcRock.getConnection().createStatement();
			res = stmt.executeQuery(deleteProcSql);
			log.info("Partition succesfully deleted : " + partitionToDelete);
		} catch (Exception esc) {
			log.warning("Exception while running store procedure to Delete partition. " + esc);
			esc.getMessage();
		} finally {
			try {
				if (res != null) {
					res.close();
				} 
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				log.log(Level.WARNING, "Error closing statement in deletePartitions ", e.getMessage());
			}
		}
		
		return partitions;
	}

	/**
	 * @param type
	 *            active measurement type
	 * @param partition
	 *            the partition to create
	 * @param columns
	 *            colums on the partition
	 * @throws Exception .
	 */
	private boolean createPartition(final Dwhtype type, final Dwhpartition partition, 
			final Vector<Dwhcolumn> columns) throws Exception {
		// First try to remove the existing partition. Most of the cases it
		// shouldn't exist.
		log.finer("Trying to delete existing partition " + partition.getTablename());

		final Connection con = getConnection(dcRock);
		Statement stmt = null;
		try {
			if (!debug) {
				stmt = con.createStatement();
				stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
				stmt.executeUpdate("DROP TABLE " + partition.getTablename());
			}
			Logger.getLogger("dwhm." + partition.getStorageid() + "." + partition.getTablename()).info(
					"Partition deleted");
			log.fine("Existing partition successfully dropped: " + partition.getTablename());
		} catch (Exception e) {
			log.log(Level.FINE, "No old partition existed for " + partition.getTablename());
			Logger.getLogger("dwhm." + partition.getStorageid() + "." + partition.getTablename()).fine(
					"No old partition existed for " + partition.getTablename());
		} finally {
			cleanUp(stmt, null);
		}

		// Start:HL94878, HL34974, HL71954
		if (columns.isEmpty()) {
			log.log(Level.FINE, "partition is not needed for " + partition.getTablename());
			return false;
		}
		
		final String sql = getPartitionCreateStatement(type, partition, columns);
		log.finest("Create template ready: " + sql);
		if (!debug) {
			executeUpdateForPartition(con, sql);
		}
		Logger.getLogger("dwhm." + type.getStorageid() + "." + partition.getTablename())
				.info("Partition created");
		log.info("Partition succesfully created: " + partition.getTablename());
		// UpdateVectorCounters uvc = new UpdateVectorCounters(reprock, dwhrock,
		// log);
		// uvc.execute(type);
		return true;
	}

	/**
	 * Extracted out for testing purposes
	 * 
	 * @param con
	 *            Execute sql
	 * @param sql
	 *            sql to execute
	 * @throws SQLException .
	 */
	protected void executeUpdateForPartition(final Connection con,
			final String sql) throws SQLException {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			stmt.executeUpdate(sql);
		} finally {
			cleanUp(stmt, null);
		}
	}

	protected Connection getConnection(final RockFactory rock) {
		return rock.getConnection();
	}

	/**
	 * @param type
	 *            active measurement type
	 * @return next number to use
	 * @throws Exception .
	 */
	private int determineNextPartitionNumber(final Dwhtype type)
			throws Exception {
		final Dwhpartition dp_cond = new Dwhpartition(dwhrepRock);
		dp_cond.setStorageid(type.getStorageid());
		final DwhpartitionFactory dp_fact = new DwhpartitionFactory(dwhrepRock,
				dp_cond);
		final Vector<Dwhpartition> partitions = dp_fact.get();
		int no = 0;
		final Iterator<Dwhpartition> it = partitions.iterator();
		while (it.hasNext()) {
			final Dwhpartition p = it.next();
			if (p.getStatus().equalsIgnoreCase("MANUAL")) {
				it.remove();
			} else {
				final String name = p.getTablename();
				final int ix = name.lastIndexOf("_");
				if (ix > 0) {
					try {
						final int cand = Integer.parseInt(name
								.substring(ix + 1));
						if (cand < 1000 && cand > no) {
							no = cand;
						}
					} catch (Exception e) {
						//
					}
				}
			}
		}
		return no + 1;
	}

	/**
	 * @throws Exception .
	 */
	private static void initVelocity() throws Exception {
		Velocity.setProperty("resource.loader", "class,file");
		Velocity.setProperty("class.resource.loader.class",
				"org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
		Velocity.setProperty("file.resource.loader.class",
				"org.apache.velocity.runtime.resource.loader.FileResourceLoader");
		Velocity.setProperty("file.resource.loader.path", StaticProperties
				.getProperty("dwhm.templatePath",
						"/dc/dc5000/conf/dwhm_templates"));
		Velocity.setProperty("file.resource.loader.cache", "true");
		Velocity.setProperty("file.resource.loader.modificationCheckInterval",
				"60");
		Velocity.setProperty( RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,"org.apache.velocity.runtime.log.AvalonLogChute,org.apache.velocity.runtime.log.Log4JLogChute,org.apache.velocity.runtime.log.CommonsLogLogChute,org.apache.velocity.runtime.log.ServletLogChute,org.apache.velocity.runtime.log.JdkLogChute" );
	    Velocity.setProperty("runtime.log.logsystem.log4j.logger","/eniq/home/dcuser/velocity.log"); 
		Velocity.init();
	}

	/**
	 * Sorts the partitions
	 * 
	 * @param partitionPlanType
	 *            volume or time
	 * @param partitions
	 *            list of partitions
	 */
	protected void sortPartitions(final short partitionPlanType,
			final Vector<Dwhpartition> partitions) {
		if (partitionPlanType == TIME_BASED_PARTITION_TYPE) {
			sortTimeBasedPartitions(partitions);
		} else {
			sortVolumeBasedPartitions(partitions);
		}
	}

	/**
	 * Sorts the time-based partitions
	 * 
	 * @param partitions
	 *            list of partitions
	 */
	private void sortTimeBasedPartitions(final Vector<Dwhpartition> partitions) {
		Collections.sort(partitions, new Comparator<Dwhpartition>() {
			@Override
			public int compare(final Dwhpartition o1, final Dwhpartition o2) {
				if (o1.getStarttime() == null && o2.getStarttime() == null) {
					return o1.getTablename().compareTo(o2.getTablename());
				} else if (o1.getStarttime() == null) {
					return 1;
				} else if (o2.getStarttime() == null) {
					return -1;
				} else {
					return o2.getStarttime().compareTo(o1.getStarttime());
				}
			}
		});
	}

	/**
	 * Sorts the volume-based partitions
	 * 
	 * @param partitions
	 *            list of partitions
	 */
	private void sortVolumeBasedPartitions(final Vector<Dwhpartition> partitions) {
		Collections.sort(partitions, new Comparator<Dwhpartition>() {
			@Override
			public int compare(final Dwhpartition o1, final Dwhpartition o2) {
				if (o1.getLoadorder() == null && o2.getLoadorder() == null) {
					return o1.getTablename().compareTo(o2.getTablename());
				} else if (o1.getLoadorder() == null) {
					return 1;
				} else if (o2.getLoadorder() == null) {
					return -1;
				} else {
					return o2.getLoadorder().compareTo(o1.getLoadorder());
				}
			}
		});
	}

	/**
	 * @param columns
	 *            list of columns
	 */
	private void sortColumns(final Vector<Dwhcolumn> columns) {
		Collections.sort(columns, new Comparator<Dwhcolumn>() {

			@Override
			public int compare(final Dwhcolumn o1, final Dwhcolumn o2) {
				return o1.getColnumber().compareTo(o2.getColnumber());
			}

			// public boolean equals(Dwhcolumn o1, Dwhcolumn o2) {
			// return o1.getColnumber().equals(o2.getColnumber());
			// }
		});
	}

	/**
	 * Creates the busy hour sources statement for a busy hour object. For
	 * example: ' from DC_E_CPP_VCLTP_COUNT,DIM_E_RAN_RBS'.
	 * 
	 * @param bh
	 *            busy hour
	 * @param reprock
	 *            repdb.dwhrep
	 * @return source statement
	 * @throws Exception .
	 */
	private static String createBusyHourSourcesStatement(final Busyhour bh,
			final RockFactory reprock) throws Exception {
		String sources = "";
		boolean first = true;
		final Busyhoursource bhs = new Busyhoursource(reprock);
		bhs.setVersionid(bh.getVersionid());
		bhs.setBhlevel(bh.getBhlevel());
		bhs.setBhtype(bh.getBhtype());
		bhs.setTargetversionid(bh.getTargetversionid());
		bhs.setBhobject(bh.getBhobject());
		// eeoidiv 20100505, HL82220,Busyhour views not created if DIM_E_ table
		// returned first. (So ordering result)
		final BusyhoursourceFactory bhsF = new BusyhoursourceFactory(reprock,
				bhs, " ORDER BY TYPENAME ASC ");
		for (Busyhoursource bsorce : bhsF.get()) {
			if (first) {
				sources = " from " + bsorce.getTypename();
				first = false;
			} else {
				sources += ", " + bsorce.getTypename();
			}
		}
		return sources;
	}

	/**
	 * Creates the busy hour rank key names busy hour object. For example:
	 * 'OSS_ID,ELEMENT_NAME'.
	 * 
	 * @param bh
	 *            busy hour
	 * @param reprock
	 *            repdb.dwhrep
	 * @return busy hour rank key names
	 * @throws Exception .
	 */
	private static String createBusyHourRankKeyCountersStatement(
			final Busyhour bh, final RockFactory reprock) throws Exception {
		boolean first = true;
		String keyCounters = "";
		final Busyhourrankkeys bhrk = new Busyhourrankkeys(reprock);
		bhrk.setVersionid(bh.getVersionid());
		bhrk.setBhlevel(bh.getBhlevel());
		bhrk.setBhtype(bh.getBhtype());
		bhrk.setBhobject(bh.getBhobject());
		bhrk.setTargetversionid(bh.getTargetversionid());
		final BusyhourrankkeysFactory bhrkF = new BusyhourrankkeysFactory(
				reprock, bhrk);
		for (Busyhourrankkeys bkey : bhrkF.get()) {
			if (first) {
				keyCounters = bkey.getKeyname();
				first = false;
			} else {
				keyCounters += ", " + bkey.getKeyname();
			}
		}
		return keyCounters;
	}

	/**
	 * Creates the busy hour rank key values for a busy hour object. For
	 * example: 'DC_E_CPP_VCLTP_COUNT.OSS_ID,DC_E_CPP_VCLTP_COUNT.RBS'.
	 * 
	 * @param bh
	 *            busy hour
	 * @param reprock
	 *            repdb.dwhrep
	 * @return busy hour rank key values
	 * @throws Exception .
	 */
	private static String createBusyHourRankKeysValuesStatement(
			final Busyhour bh, final RockFactory reprock) throws Exception {
		boolean first = true;
		String keyValues = "";
		final Busyhourrankkeys bhrk = new Busyhourrankkeys(reprock);
		bhrk.setVersionid(bh.getVersionid());
		bhrk.setBhlevel(bh.getBhlevel());
		bhrk.setBhtype(bh.getBhtype());
		bhrk.setBhobject(bh.getBhobject());
		bhrk.setTargetversionid(bh.getTargetversionid());
		final BusyhourrankkeysFactory bhrkF = new BusyhourrankkeysFactory(
				reprock, bhrk);
		for (Busyhourrankkeys bkey : bhrkF.get()) {
			if (first) {
				keyValues = bkey.getKeyvalue();
				first = false;
			} else {
				keyValues += ", " + bkey.getKeyvalue();
			}
		}
		return keyValues;
	}

	public static void deleteRankbhAggregationsForBusyhour(final Busyhour bh,
			final String versionId, final RockFactory rockFactory)
			throws Exception {
		// Remove all the RANKBH busy hour aggregations and aggregation rules
		// from the DB for given busyhour.

		final String targetTechpack = Utils.replaceNull(bh.getTargetversionid()
				.substring(0, bh.getTargetversionid().indexOf(":")));
		final String ranking = Utils.replaceNull(bh.getBhobject()).trim()
				+ Constants.TYPENAMESEPARATOR
				+ Utils.replaceNull(bh.getBhtype());
		String rankingTable;
		if (Utils.replaceNull(bh.getBhelement()) == 0) {
			rankingTable = bh.getBhlevel();
		} else {
			rankingTable = targetTechpack + Constants.TYPENAMESEPARATOR
					+ "ELEMBH";
		}

		final String dayAggName = rankingTable + "_RANKBH_" + ranking;
		final Aggregation whereDayAggregation = new Aggregation(rockFactory);
		whereDayAggregation.setVersionid(versionId);
		whereDayAggregation.setAggregationtype("RANKBH");
		whereDayAggregation.setAggregation(dayAggName);

		final AggregationFactory dayAggregationFactory = new AggregationFactory(
				rockFactory, whereDayAggregation);
		for (Aggregation aggregation : dayAggregationFactory.get()) {
			final Aggregationrule agr = new Aggregationrule(rockFactory);
			agr.setVersionid(versionId);
			agr.setAggregation(aggregation.getAggregation());
			agr.deleteDB();
			aggregation.deleteDB();
		}

		final String weekAggName = rankingTable + "_WEEKRANKBH_" + ranking;
		final Aggregation whereWeekAggregation = new Aggregation(rockFactory);
		whereWeekAggregation.setVersionid(versionId);
		whereWeekAggregation.setAggregationtype("RANKBH");
		whereWeekAggregation.setAggregation(weekAggName);

		final AggregationFactory weekAggregationFactory = new AggregationFactory(
				rockFactory, whereWeekAggregation);
		for (Aggregation aggregation : weekAggregationFactory.get()) {
			final Aggregationrule agr = new Aggregationrule(rockFactory);
			agr.setVersionid(versionId);
			agr.setAggregation(aggregation.getAggregation());
			agr.deleteDB();
			aggregation.deleteDB();
		}

		final String monthAggName = rankingTable + "_MONTHRANKBH_" + ranking;
		final Aggregation whereMonthAggregation = new Aggregation(rockFactory);
		whereMonthAggregation.setVersionid(versionId);
		whereMonthAggregation.setAggregationtype("RANKBH");
		whereMonthAggregation.setAggregation(monthAggName);

		final AggregationFactory monthAggregationFactory = new AggregationFactory(
				rockFactory, whereMonthAggregation);
		for (Aggregation aggregation : monthAggregationFactory.get()) {
			final Aggregationrule agr = new Aggregationrule(rockFactory);
			agr.setVersionid(versionId);
			agr.setAggregation(aggregation.getAggregation());
			agr.deleteDB();
			aggregation.deleteDB();
		}

	}

	/**
	 * Generates the RANKBH aggregations and rules for the busy hour.
	 * 
	 * @param bh
	 *            busy hour
	 * @param busyhoursourceList
	 *            source list
	 * @param versionId
	 *            version id
	 * @param rockFactory
	 *            repdb.dwhrep
	 * @throws Exception .
	 */
	public static void createRankbhAggregationsForBusyhour(final Busyhour bh,
			final List<Busyhoursource> busyhoursourceList,
			final String versionId, final RockFactory rockFactory)
			throws Exception {
		// Get the target techpack (without the version number).
		// For example: 'DC_X_RADIO'
		final String targetTechpack = Utils.replaceNull(bh.getTargetversionid()
				.substring(0, bh.getTargetversionid().indexOf(":")));
		// Get the ranking value:
		// For example: 'Cell_TCHTRAF'
		final String ranking = Utils.replaceNull(bh.getBhobject()).trim()
				+ Constants.TYPENAMESEPARATOR
				+ Utils.replaceNull(bh.getBhtype());
		String rankingTable;
		// get the first busyhoursource to get the source_type, source_level and
		// source_mtableId for aggregation rule.
		// if we have more than one source we just use the first source.
		// if no sources found we leave all empty.
		String source_typename;
		String source_sourceMTableId = "";
		String source_sourceType = "";
		String source_sourceLevel = "";
		for (Busyhoursource busyhoursource : busyhoursourceList) {
			source_typename = busyhoursource.getTypename().trim();
			if (!source_typename.isEmpty()) {
				source_sourceType = source_typename.substring(0,
						source_typename.lastIndexOf("_"));
				source_sourceLevel = source_typename.substring(source_typename
						.lastIndexOf("_") + 1);
				source_sourceMTableId = busyhoursource.getTargetversionid()
						+ Constants.TYPESEPARATOR + source_sourceType
						+ Constants.TYPESEPARATOR + source_sourceLevel;
				break;
			}
		}
		// Check if this is an object or element busy hour and create the
		// aggregations and rules accordingly.
		if (Utils.replaceNull(bh.getBhelement()) == 0) {
			// Object Busy Hour.
			// Get ranking table value from the bhlevel.
			// For example: DC_X_RADIO_BSS_CELLBH
			rankingTable = bh.getBhlevel();
		} else {
			// Element Busy Hour.
			// Get the rankingTable value.
			// For example: DC_X_RADIO_ELEMBH
			rankingTable = targetTechpack + Constants.TYPENAMESEPARATOR
					+ "ELEMBH";
		}
		// Create the RANKBH aggregations for DAY, WEEK, and MONTH.
		final String dayAggName = rankingTable + "_RANKBH_" + ranking;
		final String weekAggName = rankingTable + "_WEEKRANKBH_" + ranking;
		final String monthAggName = rankingTable + "_MONTHRANKBH_" + ranking;
		createAggregation(rockFactory, dayAggName, versionId,
				Constants.RANKBHLEVEL, Constants.DAYSCOPE);
		createAggregation(rockFactory, weekAggName, versionId,
				Constants.RANKBHLEVEL, Constants.WEEKSCOPE);
		createAggregation(rockFactory, monthAggName, versionId,
				Constants.RANKBHLEVEL, Constants.MONTHSCOPE);
		// Create the aggregation rules for this busy hour.
		//
		// Initialise empty values to the parameter values for aggregation
		// rules.
		// The common values for all rules are set here.
		//
		String aggregationName;
		// RuleId is a running number starting from zero.
		Integer ruleID = 0;
		// Target measurement table id, for example:
		// 'DC_X_RADIO:((PA6)):DC_X_RADIO_BSS_CELLBH:RANKBH'
		final String targetMTableId = bh.getTargetversionid()
				+ Constants.TYPESEPARATOR + bh.getBhlevel()
				+ Constants.TYPESEPARATOR + Constants.RANKBHLEVEL;
		// TargetType, for example: DC_X_RADIO_BSS_CELLBH or DC_X_RADIO_ELEMBH
		final String targetType = rankingTable;
		// Target level: 'RANKBH'
		final String targetLevel = Constants.RANKBHLEVEL;
		// Target table, for example: 'DC_X_RADIO_BSS_CELLBH_RANKBH'
		final String targetTable = rankingTable + Constants.TYPENAMESEPARATOR
				+ Constants.RANKBHLEVEL;
		String sourceMTableId;
		String sourceType;
		String sourceLevel;
		String sourceTable;
		String ruleType;
		String scope;
		// Busy hour is enabled if enable value is 1 or null.
		// Disabled if value is 0.
		Integer enable = 1;
		if (bh.getEnable() != null && bh.getEnable() == 0) {
			enable = 0;
		}
		// Create DAY Aggregation rule
		//
		aggregationName = dayAggName;
		sourceMTableId = source_sourceMTableId;
		sourceType = source_sourceType;
		sourceLevel = source_sourceLevel;
		sourceTable = dayAggName;
		// Rule type: 'RANKBH'
		ruleType = Constants.RANKBHLEVEL;
		// Aggregation scope: 'DAY'
		scope = Constants.DAYSCOPE;
		createAggregationRule(rockFactory, aggregationName, versionId,
				ruleID++, targetMTableId, targetType, targetLevel, targetTable,
				sourceMTableId, sourceType, sourceLevel, sourceTable, ruleType,
				scope, ranking, enable);
		// Create WEEK Aggregation rule
		//
		aggregationName = weekAggName;
		// The sourceMTableId is the RANKBH Table, for example:
		// 'DC_X_RADIO:((PA5)):DC_X_RADIO_BSS_CELLBH:RANKBH' or
		// 'DC_X_RADIO:((PA5)):DC_X_RADIO_ELEMBH:RANKBH'
		sourceMTableId = bh.getTargetversionid() + Constants.TYPESEPARATOR
				+ rankingTable + Constants.TYPESEPARATOR
				+ Constants.RANKBHLEVEL;
		sourceType = rankingTable;
		sourceLevel = Constants.RANKBHLEVEL;
		sourceTable = rankingTable + Constants.TYPENAMESEPARATOR
				+ Constants.RANKBHLEVEL;
		// Rule type: 'RANKBHCLASS'
		ruleType = Constants.RANKBHCLASSLEVEL;
		// Aggregation scope: 'WEEK'
		scope = Constants.WEEKSCOPE;
		createAggregationRule(rockFactory, aggregationName, versionId,
				ruleID++, targetMTableId, targetType, targetLevel, targetTable,
				sourceMTableId, sourceType, sourceLevel, sourceTable, ruleType,
				scope, ranking, enable);
		// Create MONTH Aggregation rule
		//
		aggregationName = monthAggName;
		// The sourceMTableId is the RANKBH Table, for example:
		// 'DC_X_RADIO:((PA5)):DC_X_RADIO_BSS_CELLBH:RANKBH' or
		// 'DC_X_RADIO:((PA5)):DC_X_RADIO_ELEMBH:RANKBH'
		sourceMTableId = bh.getTargetversionid() + Constants.TYPESEPARATOR
				+ rankingTable + Constants.TYPESEPARATOR
				+ Constants.RANKBHLEVEL;
		sourceType = rankingTable;
		sourceLevel = Constants.RANKBHLEVEL;
		// Rule type: 'RANKBHCLASS'
		ruleType = Constants.RANKBHCLASSLEVEL;
		// Aggregation scope: 'MONTH'
		scope = Constants.MONTHSCOPE;
		createAggregationRule(rockFactory, aggregationName, versionId,
				ruleID++, targetMTableId, targetType, targetLevel, targetTable,
				sourceMTableId, sourceType, sourceLevel, sourceTable, ruleType,
				scope, ranking, enable);
	}

	/**
	 * Creates a new aggregation object to the DB.
	 * 
	 * @param rockFactory
	 *            repdb.dwhrep
	 * @param aggregationName
	 *            aggregation name
	 * @param versionId
	 *            versionId
	 * @param type
	 *            type
	 * @param scope
	 *            scope
	 * @throws SQLException .
	 * @throws RockException .
	 */
	private static void createAggregation(final RockFactory rockFactory,
			final String aggregationName, final String versionId,
			final String type, final String scope) throws SQLException,
			RockException {
		final Aggregation agg = new Aggregation(rockFactory, true);
		agg.setAggregation(aggregationName);
		agg.setVersionid(versionId);
		agg.setAggregationtype(type);
		agg.setAggregationscope(scope);
		agg.saveToDB();
	}

	/**
	 * Creates a new aggregation rule object to the DB.
	 * 
	 * @param rockFactory
	 *            rockFactory
	 * @param aggregationName
	 *            aggregationName
	 * @param versionId
	 *            versionId
	 * @param ruleId
	 *            ruleId
	 * @param targetMTableId
	 *            targetMTableId
	 * @param targetType
	 *            targetType
	 * @param TargetLevel
	 *            TargetLevel
	 * @param targetTable
	 *            targetTable
	 * @param sourcetMTableId
	 *            sourcetMTableId
	 * @param sourceType
	 *            sourceType
	 * @param sourceLevel
	 *            sourceLevel
	 * @param sourceTable
	 *            sourceTable
	 * @param ruleType
	 *            ruleType
	 * @param scope
	 *            scope
	 * @param bhType
	 *            bhType
	 * @param enable
	 *            enable
	 * @throws SQLException .
	 * @throws RockException .
	 */
	private static void createAggregationRule(final RockFactory rockFactory,
			final String aggregationName, final String versionId,
			final Integer ruleId, final String targetMTableId,
			final String targetType, final String TargetLevel,
			final String targetTable, final String sourcetMTableId,
			final String sourceType, final String sourceLevel,
			final String sourceTable, final String ruleType,
			final String scope, final String bhType, final Integer enable)
			throws SQLException, RockException {
		final Aggregationrule rankAggregationRule = new Aggregationrule(
				rockFactory, true);
		rankAggregationRule.setAggregation(aggregationName);
		rankAggregationRule.setVersionid(versionId);
		rankAggregationRule.setRuleid(ruleId);
		rankAggregationRule.setTarget_mtableid(targetMTableId);
		rankAggregationRule.setTarget_type(targetType);
		rankAggregationRule.setTarget_level(TargetLevel);
		rankAggregationRule.setTarget_table(targetTable);
		rankAggregationRule.setSource_mtableid(sourcetMTableId);
		rankAggregationRule.setSource_type(sourceType);
		rankAggregationRule.setSource_level(sourceLevel);
		rankAggregationRule.setSource_table(sourceTable);
		rankAggregationRule.setRuletype(ruleType);
		rankAggregationRule.setAggregationscope(scope);
		rankAggregationRule.setBhtype(bhType);
		rankAggregationRule.setEnable(enable);
		rankAggregationRule.saveToDB();
	}

	/**
	 * @param type
	 *            New method to filter the Dwhtype for a specific measurment
	 *            type - Generic SIU
	 */
	private void filterMeasType(final Dwhtype type) {
		if (measurementType != null && measurementType.length() > 0) {
			type.setTypename(measurementType);
		}
	}

	/**
	 * Returns SQL Query to create partition via VelocityContext
	 * 
	 * @param type
	 * @param partition
	 * @param columns
	 * @return
	 * @throws Exception
	 */
	public String getPartitionCreateStatement(final Dwhtype type,
			final Dwhpartition partition, final Vector<Dwhcolumn> columns)
			throws Exception {
		log.finer("Creating partition " + partition.getTablename());
		final VelocityContext vctx = new VelocityContext();
		vctx.put("type", type);
		vctx.put("columns", columns);
		vctx.put("tableName", partition.getTablename());
		log.finest(" values to create SQL query:" + "Table name for type:" + type.getTableName() 
				+ "columns:" + Arrays.toString(columns.toArray()) + "tableName: " + partition.getTablename());
		final StringWriter sqlWriter = new StringWriter();
		final boolean isMergeOk = Velocity.mergeTemplate(
				type.getCreatetemplate(), Velocity.ENCODING_DEFAULT, vctx, sqlWriter);
		if (!isMergeOk) {
			throw new Exception("Velocity failed");
		}
		return sqlWriter.toString();
	}

	/**
	 * Closes ResultSets and Statements. 
	 * 
	 * @param s
	 * @param rs
	 */
	private void cleanUp(Statement s, ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (Exception e) {
			log.log(Level.FINE, "ResultSet close failed", e);
		}
		rs = null;

		try {
			if (s != null) {
				s.close();
			}
		} catch (Exception e) {
			log.log(Level.FINE, "Statement close failed", e);
		}
		s = null;
	}
	
	/**
	 * Checks if techpack has been updated with 
	 * latest vector handling implementation
	 * @param techpackName 
	 */
	private void checkVectorFlag(String techpackName) {
		String vectorConfDir = CONF_DIR + "vectorflags/";
		String vectorConfFlag = vectorConfDir + "New_Vector_" + techpackName;
		
		try {
			File vectorConfFlagDir = new File(vectorConfDir);
			File vectorConfFlagFile = new File(vectorConfFlag);
			if(!vectorConfFlagDir.exists()) {
				this.newVectorFlag = false;
			}
			else {
				if(!vectorConfFlagFile.exists()) {
					this.newVectorFlag = false;
				}
				else {
					this.newVectorFlag = true;
				}
			}
		}
		catch(Exception e) {
			this.newVectorFlag = false;
		}
		
	}
	
	/**
	 * Deletes old vector tables and replaces with views
	 * 
	 * @param techpackName 
	 * @throws Exception 
	 */
	private void handleOldVectors(String techPack) throws Exception {
		// Delete partition and create new views
		final String vecRefTableName = DIM + techPack.substring(techPack.indexOf("_")) + "_VECTOR_REFERENCE";
		final Set<String> vecTables = getVectorReferenceTablesFromDB(vecRefTableName);
		for(String vecTable : vecTables) {
			log.fine("Handling old vectors for " + vecTable);
			List<Dwhpartition> vecToDelete = new ArrayList<>();
			final Dwhpartition dp_cond = new Dwhpartition(dwhrepRock);
			dp_cond.setTablename(vecTable);
			final DwhpartitionFactory dp_fact = new DwhpartitionFactory(dwhrepRock,dp_cond);
			final Vector<Dwhpartition> partitions = dp_fact.get();
			if (partitions.size() < 1) {
				log.fine("Old vector table " + vecTable + " already deleted.");
			}
			else if (partitions.size() > 1) {
				throw new Exception("PANIC: Simple type " + vecTable + ":PLAIN" + " has multiple partitions");
			}
			else {
				log.fine("Need to remove partitions for type " + vecTable);
				final Dwhpartition delpart = partitions.get(0);
				delpart.deleteDB();
				vecToDelete.add(delpart);
			}
			if (vecToDelete != null && vecToDelete.size() > 0) {
					deletePartitions(vecToDelete);
			}
			// All old vector tables are deleted
			
			//Create views
			CreateViewsAction createdView = new CreateViewsAction(this.dbaRock, this.dcRock, this.dwhrepRock, this.clog, vecTable, vecRefTableName);
			log.fine("Created view " + createdView.getViewName());
		}
		
	}

	/**
	 * Queries the vector reference table to get all the vector table names for each vector counter
	 * (Only for defined vector ranges)
	 * 
	 * @param vecTableName
	 * @return Set of Vector tables
	 */
	private Set<String> getVectorReferenceTablesFromDB(String vecTableName) {
		Set<String> vecTables = new HashSet<>();
		final String tabQuery = "SELECT DISTINCT TABLE_COUNTER FROM dc." + vecTableName;
		Statement stmt = null;
		ResultSet vecTableRes = null;
		try {
			stmt = this.dcRock.getConnection().createStatement();
			vecTableRes = stmt.executeQuery(tabQuery);
			if(vecTableRes != null) {
				while(vecTableRes.next()) {
					vecTables.add(vecTableRes.getString(1));
				}
			}
		}
		catch (SQLException sqlEx) {
			log.severe("Database update failed because " + sqlEx.getMessage());
		}
		catch (Exception ex) {
			log.severe("Vector table reference query failed because " + ex.getMessage());
		}
		finally {
			try {
				if (vecTableRes != null) {
					vecTableRes.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			}
			catch (final Exception e) {
				log.warning("SQL Objects cleanup error " + e.getMessage());
			}
		}
		
		return vecTables;
	}
	
	
}
