package com.distocraft.dc5000.dwhm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.common.RemoteExecutor;
import com.ericsson.eniq.repository.ETLCServerProperties;
import ssc.rockfactory.RockFactory;

/**
 * Standalone class to drop all views from DWHDB using DC Connection for a given Techpack.
 * 
 * @author xtouoos
 *
 */
public class ForceDropView {

	public static String TECHPACKNAME = null;

	private RockFactory etlRepRock = null;
	
	private String etlrepURL = "";

	private String etlrepUserName = "";

	private String etlrepPassword = "";

	private String etlrepDriverName = "";
	
	private String dcURL = "";
	
	private String dcUserName = "";
	
	private String dcPassword = "";
	
	private String dcDriverName = "";
	
	private String dbaURL = "";
	
	private String dbaUserName = "";
	
	private String dbaPassword = "";
	
	private String dbaDriverName = "";
	
	private String dwhrepURL = "";
	
	private String dwhrepUserName = "";
	
	private String dwhrepPassword = "";
	
	private String dwhrepDriverName = "";
	
	private final static String APPLICATION_USER = "dcuser";

	private int reTryCount = 0;

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
	
	private Logger log;

	/**
	 * Main method to start. 
	 * 
	 * @param args - TechpackName. 
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("ERROR : No TECHPACK name passed. Exiting. ");
			System.exit(1);
		}

		TECHPACKNAME = args[0];
		ForceDropView fdv = new ForceDropView();
		try {
			fdv.execute();
		} catch (Exception e) {
			System.out.println("ERROR : Not able to remove views. Exiting. ");
			System.exit(2);
		}
	}
	
	/**
	 * Constructor without parameter
	 * 
	 */
	public ForceDropView() {
		log = Logger.getLogger("etlengine.dwh.ForceDropView");
	}
	
	/**
	 * One parameter constructor
	 * 
	 * @param techpackName
	 */
	public ForceDropView(String techpackName) {
		TECHPACKNAME = techpackName;
		log = Logger.getLogger("etlengine.dwh.ForceDropView");
	}
	
	/**
	 * 2 parameter constructor
	 * 
	 * @param techpackName
	 * @param log
	 */
	public ForceDropView(String techpackName, Logger log) {
		TECHPACKNAME = techpackName;
		this.log = log;
	}

	/**
	 * Starting point to drop all views. 
	 * 
	 */
	public void execute() throws Exception {
		log.info("PERFORMANCE_LOG :: DROPVIEW for " +TECHPACKNAME+ " : STARTED at " + sdf.format(new Date(System.currentTimeMillis())));
		
		try {
			// Load Properties
			loadProperties();

			// Create ETLREP DB Connection
			initETLRepConnection();
			
			// Read META_DATABASE for Connection info
			getConnInfo();

			// Get all view list from DWHDB
			List<String> dwhViewList = getAllDwhViewList();
			
			// Get all view list from REPDB
			List<String> repViewList = getAllRepViewList();
			
			// Take backup
			backupViewsDef(dwhViewList, repViewList);
			
			// Process view list
			while (reTryCount < 3) {
				if (dwhViewList != null && dwhViewList.size() > 0) {
					for (String eachView : dwhViewList) {
						removeView(eachView);
					}
					log.info(dwhViewList.size() + " views dropped.");
				}

				// Check all views dropped.
				log.info("Checking DHWDB to verify if any views available.");
				dwhViewList = getAllDwhViewList();
				if (dwhViewList != null && dwhViewList.size() > 0) {
					log.info("Trying again to drop remaining views. View List : " +dwhViewList);
					reTryCount++;
				} else {
					log.info("No views found. Hence proceeding with locking partitions. ");
					break;
				}
			}
			
			if (dwhViewList != null && dwhViewList.size() > 0) {
				log.warning("Still few views are available after 3 attempts. Couldn't able to remove them. Locking all DB Users to remove views. " +
						"User : DCBO, DCPUBLIC, DCNETAN will be locked. Hence, all BO refresh will be impacted. ");
				
				// Lock all users [dcbo, dcpublic, dcnetan]
				lockDBUsers();
				
				// Try removing views 
				for (String eachView : dwhViewList) {
					removeView(eachView);
				}
				
				dwhViewList = getAllDwhViewList();
				if (dwhViewList != null && dwhViewList.size() > 0) {
					log.severe("Not able to remove views : " +dwhViewList);
					unlockDBUsers();
					throw new Exception("Couldn't able to remove view .");
				}
				
				// Unlock all users [dcbo, dcpublic, dcnetan]
				unlockDBUsers();
			}
		} catch (Exception e) {
			log.warning("Exception occurred during ForceDropView : " + e);
			throw e;
		} finally {
			// Close ETLREP Connection
			closeDBConnection(etlRepRock);
		}
		log.info("PERFORMANCE_LOG :: DROPVIEW for " +TECHPACKNAME+ " : ENDED at " + sdf.format(new Date(System.currentTimeMillis())));
	}

	/**
	 * Method to unlock DB Users
	 * 
	 * @throws Exception
	 */
	public void unlockDBUsers() throws Exception {
		log.info("Unlocking DB Users [DCBO, DCPUBLIC, DCNETAN will be released]."); 
		String unlockLogFile = "/eniq/log/sw_log/tp_installer/" + sdf.format(new Date(System.currentTimeMillis())) + "_unlockUser_" + TECHPACKNAME + ".log";
		String runtimeScript = ". /eniq/home/dcuser; . ~/.profile; /eniq/sw/installer/change_db_users_perm.bsh -a unlock -u ALL -l " + unlockLogFile ;
		String output;
		try {
			output = RemoteExecutor.executeComandSshKey(APPLICATION_USER, "dwhdb", runtimeScript);
			if (output != null && output != "") {
				if (!output.contains("BUILD SUCCESSFUL")){
					log.severe("Failed to unlock DB users : " +output); 
					throw new Exception(output);
				}
			} 
		} catch (Exception e) {
			log.log(Level.WARNING, "Exception occurred during unlockDBUsers ", e);
		} 
		log.info("DB Users [DCBO, DCPUBLIC, DCNETAN] released.");
	}

	/**
	 * Method to lock DB Users
	 * 
	 * @throws Exception
	 */
	public void lockDBUsers() throws Exception {
		log.info("Locking DB Users [DCBO, DCPUBLIC, DCNETAN will be locked]."); 
		String lockLogFile = "/eniq/log/sw_log/tp_installer/" + sdf.format(new Date(System.currentTimeMillis())) + "_lockUser_" + TECHPACKNAME + ".log";
		String runtimeScript = ". /eniq/home/dcuser; . ~/.profile; /eniq/sw/installer/change_db_users_perm.bsh -a lock -u ALL -l " + lockLogFile ;
		String output;
		try {
			output = RemoteExecutor.executeComandSshKey(APPLICATION_USER, "dwhdb", runtimeScript);
			if (output != null && output != "") {
				if (!output.contains("BUILD SUCCESSFUL")){
					log.severe("Failed to lock DB users : " +output); 
					throw new Exception(output);
				}
			} 
		} catch (Exception e) {
			log.log(Level.WARNING, "Exception occurred during lockDBUsers ", e);
		} 
		log.info("DB Users [DCBO, DCPUBLIC, DCNETAN] locked."); 
	}

	/**
	 * Reads/Loads static.properties file and ETLCServer.Properties file.
	 * 
	 */
	private void loadProperties() {
		log.info("Starting to load properties.");
		
		try {
			StaticProperties.reload();
		} catch (IOException e1) {
			log.warning("IOException when loading StaticProperties" + e1);
		}
		String sysPropDC5000 = System.getProperty("dc5000.config.directory");
		Properties appProps = null;

		if (sysPropDC5000 == null) {
			log.warning("System property dc5000.config.directory not defined");
			System.exit(3);
		}

		if (!sysPropDC5000.endsWith(File.separator)) {
			sysPropDC5000 += File.separator;
		}

		try {
			appProps = new ETLCServerProperties(sysPropDC5000 + "ETLCServer.properties");
		} catch (IOException e) {
			log.warning("Failed to read ETLCServer.Properties file." + e);
		}

		etlrepURL = appProps.getProperty("ENGINE_DB_URL");
		etlrepUserName = appProps.getProperty("ENGINE_DB_USERNAME");
		etlrepPassword = appProps.getProperty("ENGINE_DB_PASSWORD");
		etlrepDriverName = appProps.getProperty("ENGINE_DB_DRIVERNAME");

		log.info("Loading of properties successfully completed. ");
	}

	/**
	 * Initialize ETLREP DB Connection 
	 * 
	 */
	private void initETLRepConnection() {
		etlRepRock = null;
		while (etlRepRock == null) {
			try {
				etlRepRock = new RockFactory(etlrepURL, etlrepUserName,
						etlrepPassword, etlrepDriverName, "dropViewETLConn", true);
			} catch (Exception e) {
				log.warning("Database connection failed. check ETLREP" + e);
			}
		}
		
		log.fine("Successfully created REPDB connection. ");
	}
	
	/**
	 * Get DC and DWHREP Connection Info from METADATABASES
	 * 
	 */
	private void getConnInfo() {
		try {
			final Meta_databases mDB = new Meta_databases(etlRepRock);
			Meta_databasesFactory mDBF = new Meta_databasesFactory(etlRepRock, mDB);
			final Vector<Meta_databases> listDB = mDBF.get();
			for (int i = 0; i < listDB.size(); i++) {
				final Meta_databases eachDB = listDB.get(i);
				if (eachDB.getConnection_name().equalsIgnoreCase("dwh")
						&& eachDB.getType_name().equalsIgnoreCase("USER")) {
					dcURL = eachDB.getConnection_string();
					dcUserName = eachDB.getUsername();
					dcPassword = eachDB.getPassword();
					dcDriverName = eachDB.getDriver_name();
				} else if (eachDB.getConnection_name().equalsIgnoreCase("dwhrep")
						&& eachDB.getType_name().equalsIgnoreCase("USER")) {
					dwhrepURL = eachDB.getConnection_string();
					dwhrepUserName = eachDB.getUsername();
					dwhrepPassword = eachDB.getPassword();
					dwhrepDriverName = eachDB.getDriver_name();
				} else if (eachDB.getConnection_name().equalsIgnoreCase("dwh")
						&& eachDB.getType_name().equalsIgnoreCase("DBA")) {
					dbaURL = eachDB.getConnection_string();
					dbaUserName = eachDB.getUsername();
					dbaPassword = eachDB.getPassword();
					dbaDriverName = eachDB.getDriver_name();
				}
			}
		} catch (Exception e) {
			log.warning("Exception while querying repdb database for connection info. Reason: " + e.getMessage());
		}
		
	}

	/**
	 * Returns DC Connection
	 * 
	 * @return
	 */
	private RockFactory getDcConnection() {
		RockFactory dcConn = null;

		try {
			if (dcURL != null){
				dcConn = new RockFactory(dcURL, dcUserName, dcPassword, dcDriverName, "dropViewDcConn", true);
			} else {
				log.warning("DC Connection info not found. ");
			}
		} catch (Exception e) {
			log.warning("Exception while creating DC Conneciton. Reason: " + e.getMessage());
		}
		return dcConn;
	}
	
	/**
	 * Returns DWHREP Connection
	 * 
	 * @return
	 */
	private RockFactory getDwhrepConnection() {
		RockFactory dwhrepConn = null;

		try {
			if (dcURL != null){
				dwhrepConn = new RockFactory(dwhrepURL, dwhrepUserName, dwhrepPassword, dwhrepDriverName, "dropViewDwhrepConn", true);
			} else {
				log.warning("DWHREP Connection info not found. ");
			}
		} catch (Exception e) {
			log.warning("Exception while creating DWHREP Conneciton. Reason: " + e.getMessage());
		}
		return dwhrepConn;
	}
	
	/**
	 * Returns DBA Connection
	 * 
	 * @return
	 */
	private RockFactory getDbaConnection() {
		RockFactory dbaConn = null;

		try {
			if (dbaURL != null){
				dbaConn = new RockFactory(dbaURL, dbaUserName, dbaPassword, dbaDriverName, "dropViewDBAConn", true);
			} else {
				log.warning("DBA Connection info not found. ");
			}
		} catch (Exception e) {
			log.warning("Exception while creating DBA Conneciton. Reason: " + e.getMessage());
		}
		return dbaConn;
	}

	/**
	 * Provides all views belonging to given TP from DWHDB database. 
	 * 
	 * @return
	 * @throws Exception
	 */
	private List<String> getAllDwhViewList() throws Exception {
		List<String> listOfViews = new ArrayList<String>();
		RockFactory dcRock = getDcConnection();
		ResultSet res = null;
		
		try {
			Statement stmnt = dcRock.getConnection().createStatement();
			String viewSQL = "select viewname from sysviews where vcreator = 'dc' and viewtext like '%" + TECHPACKNAME + "|_%' escape '|'";
			
			if (TECHPACKNAME.equalsIgnoreCase("DC_E_IMS")){
				viewSQL += " and viewtext not like '%DC_E_IMS_IPW|_%' escape '|'";
			} else if (TECHPACKNAME.equalsIgnoreCase("DC_E_CMN_STS")) {
				viewSQL += " and viewtext not like '%DC_E_CMN_STS_PC|_%' escape '|'";
			} else if (TECHPACKNAME.equalsIgnoreCase("DC_E_SASN")) {
				viewSQL += " and viewtext not like '%DC_E_SASN_SARA|_%' escape '|'";
			}
			
			try {
				res = stmnt.executeQuery(viewSQL);
				if (res != null) {
					while (res.next()){
						String viewName = res.getString("viewname").trim();
						listOfViews.add(viewName);
					}
				}
				if (TECHPACKNAME.equalsIgnoreCase("DC_E_ERBSG2")){
					String erbsCombinedSQL = "select viewname from sysviews where vcreator = 'dc' and viewtext like '%DC_E_ERBS%DC_E_ERBSG2%'";
					res = stmnt.executeQuery(erbsCombinedSQL);
					if (res != null) {
						while (res.next()){
							String erbsCombinedViewName = res.getString("viewname").trim();
							if (!listOfViews.contains(erbsCombinedViewName)){
								listOfViews.add(erbsCombinedViewName);
							}
						}
					}
				} else if (TECHPACKNAME.equalsIgnoreCase("DC_E_RBSG2")) {
					String rbsCombinedSQL = "select viewname from sysviews where vcreator = 'dc' and viewtext like '%DC_E_RBS%DC_E_RBSG2%'";
					res = stmnt.executeQuery(rbsCombinedSQL);
					if (res != null) {
						while (res.next()) {
							String rbsCombinedViewName = res.getString("viewname").trim();
							if (!listOfViews.contains(rbsCombinedViewName)){
								listOfViews.add(rbsCombinedViewName);
							}
						}
					}
				}
			} catch (SQLException e) {
				log.warning("Exception while getting DWH views " + e);
			} finally {
				try {
					// Close ResultSet and Statement
					if (res != null) {
						res.close();
					}
					if (stmnt != null) {
						stmnt.close();
					}
				} catch (Exception e) {
					log.warning("Error closing statement in getAllViewList " + e.getMessage());
				}
			}
		} catch (Exception e) {
			log.warning("Error during getAllViewList " + e);
			closeDBConnection(dcRock);
			throw e;
		} finally {
			closeDBConnection(dcRock);
		}
		
		if (listOfViews != null && listOfViews.size() > 0) {
			log.info("In DWHDB, " + TECHPACKNAME + " has " + listOfViews.size() + " views and the list are : " +listOfViews);
		}
		
		return listOfViews;
	}
	
	/**
	 * Provides all views belonging to given TP from REPDB database. 
	 * 
	 * @return
	 * @throws Exception
	 */
	private List<String> getAllRepViewList() throws Exception {
		List<String> listOfViews = new ArrayList<String>();
		RockFactory dwhrepRock = getDwhrepConnection();
		Statement s = null;
		ResultSet rs = null;
		String dwhrepViewSQL = "select TYPENAME, TABLELEVEL from dwhrep.TypeActivation where TECHPACK_NAME = '" 
					+ TECHPACKNAME + "' and TABLELEVEL <> 'PLAIN'";
		
		try {
			s = dwhrepRock.getConnection().createStatement();
			s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			rs = s.executeQuery(dwhrepViewSQL);
			if (rs != null){
				while (rs.next()){
					String typeName = rs.getString("TYPENAME");
					String tableLevel = rs.getString("TABLELEVEL");
					
					String viewName = typeName.trim() + "_" + tableLevel.trim(); 
							
					if (!listOfViews.contains(viewName)){
						listOfViews.add(viewName);
					} else {
						log.warning("Duplicate view. " + viewName);
					}
				}
			}
		} catch (SQLException e) {
			log.warning("Exception while getting DWHREP views " + e);
			closeDBConnection(dwhrepRock);
			throw e;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if ( s != null){
					s.close();
				}
				closeDBConnection(dwhrepRock);
			} catch (SQLException e) {
				log.warning("Not able to close statement " + e);
			}
		}

		RockFactory dcRock = getDcConnection();
		try {
			s = dcRock.getConnection().createStatement();
			if (TECHPACKNAME.equalsIgnoreCase("DC_E_ERBSG2")){
				String erbsCombinedSQL = "select viewname from sysviews where viewname like 'DC_E_ERBS|_%' escape '|' " +
						"and vcreator = 'dc' and viewtext like '%DC_E_ERBS%DC_E_ERBSG2%'";
				rs = s.executeQuery(erbsCombinedSQL);
				if (rs != null) {
					while (rs.next()){
						String erbsCombinedViewName = rs.getString("viewname");
						listOfViews.add(erbsCombinedViewName.trim());
					}
				}
			} else if (TECHPACKNAME.equalsIgnoreCase("DC_E_RBSG2")) {
				String rbsCombinedSQL = "select viewname from sysviews where viewname like 'DC_E_RBS|_%' escape '|' " +
						"and vcreator = 'dc' and viewtext like '%DC_E_RBS%DC_E_RBSG2%'";
				rs = s.executeQuery(rbsCombinedSQL);
				if (rs != null) {
					while (rs.next()) {
						String rbsCombinedViewName = rs.getString("viewname");
						listOfViews.add(rbsCombinedViewName.trim());
					}
				}
			}
		} catch (Exception e) {
			log.warning("Exception while getting DWHREP combined views " + e);
			closeDBConnection(dcRock);
			throw e;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if ( s != null){
					s.close();
				}
				closeDBConnection(dcRock);
			} catch (SQLException e) {
				log.warning("Not able to close statement in combined view generation. " + e);
			}
		}
		
		if (listOfViews != null && listOfViews.size() > 0) {
			log.info("In REPDB, " +TECHPACKNAME+ " has " +listOfViews.size() + " views and the list are : " +listOfViews);
		}
		
		return listOfViews;
	}
	
	/**
	 * This method removes the given view. It uses forceDropView store procedure. 
	 * 
	 * @param eachView
	 */
	private void removeView(String eachView) {
		// Get DC Connection
		RockFactory dcRock = getDcConnection();

		try {
			Statement stmnt = dcRock.getConnection().createStatement();

			String dropViewSql = "call dba.forceDropView('" + eachView + "');";
			ResultSet res = null;
			try {
				res = stmnt.executeQuery(dropViewSql);
				log.info("View " +eachView+ " successfully dropped. ");
			} catch (SQLException esc) {
				log.warning("Exception while running store procedure to drop views " + esc);
			} finally {
				try {
					if (res != null) {
						res.close();
					}
					if (stmnt != null) {
						stmnt.close();
					}
				} catch (Exception e) {
					log.warning("Error closing statement" + e.getMessage());
				}
			}
		} catch (Exception e) {
			log.warning("Error during removing all views. " + e);
		} finally {
			closeDBConnection(dcRock);
		}
	}
	
	/**
	 * Method to take backup of custom views 
	 * 
	 * @param dwhViewList
	 * @param repViewList
	 * @throws Exception
	 */
	private void backupViewsDef(List<String> dwhViewList, List<String> repViewList) throws Exception {
		List<String> diffViewList = new ArrayList<String>();
		List<String> knownViewList = new ArrayList<String>();
		diffViewList.addAll(dwhViewList);
		diffViewList.removeAll(repViewList);

		int viewCount = 0;
		for (String eachView : diffViewList) {
			// Remove known view like _DELTA, _DATES, etc. 
			if (eachView.startsWith(TECHPACKNAME) && eachView.endsWith("_DELTA")) {
				continue;
			} else if (eachView.startsWith(TECHPACKNAME) && eachView.endsWith("_DISTINCT_DATES")){
				continue;
			}

			// Remove known view like _PPx, _CPx [ x is number] 
			if (Pattern.matches(TECHPACKNAME+"_[a-zA-Z0-9].*_PP[0-9]*", eachView) || Pattern.matches(TECHPACKNAME+"_[a-zA-Z0-9].*_CP[0-9]*", eachView)){
				continue;
			}
			
			knownViewList.add(eachView);
			viewCount++;
		}
		
		if (viewCount > 0) {
			// handle custom view
			log.info("Found " + viewCount + " views to take backup before dropping them. Taking backup now for views : " + knownViewList);
			
			try {
				long startTime = System.currentTimeMillis();
				takeCustomView(knownViewList);
				log.info("Total Time taken to backup " + viewCount + " custom views are : " + (System.currentTimeMillis() - startTime) + " ms. ");
			} catch (Exception ex) {
				log.severe("General Exception during backupViewsDef " + ex);
				log.severe("Please refer logs at /eniq/backup/customViewStorage/logs directory for more details. ");
				throw ex;
			}
		} else {
			log.info("No Custom view found for " +TECHPACKNAME);
		}
	}
	
	/**
	 * Method to take backup of custom view and write into file segregated with TP version
	 * 
	 * @param diffViewList
	 * @throws Exception
	 */
	private void takeCustomView(List<String> diffViewList) throws Exception {
		String tpVersionSQL = "select TECHPACK_VERSION, substr(v.VERSIONID, (CHARINDEX(':', v.VERSIONID)+3) , ((CHARINDEX(')', v.VERSIONID)-1) - " +
				"(CHARINDEX('(', v.VERSIONID)+1))) as VERSIONID from dwhrep.Versioning v, dwhrep.TPActivation t where " +
				"t.TECHPACK_NAME = '"+ TECHPACKNAME +"' and v.VERSIONID = t.VERSIONID";
		RockFactory dwhrepRock = getDwhrepConnection();
		Statement s = null;
		ResultSet rs = null;
		String TPVersion = null;
		String techpackDir = "/eniq/backup/customViewStorage/";
		
		try {
			s = dwhrepRock.getConnection().createStatement();
			s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			rs = s.executeQuery(tpVersionSQL);
			if (rs != null){
				while (rs.next()){
					String tpVer = rs.getString("TECHPACK_VERSION");
					String verId = rs.getString("VERSIONID");
					TPVersion = tpVer.trim() + "b" + verId.trim(); 
				}
			}
		} catch (SQLException e) {
			log.warning("Exception while getting version in takeCustomView " + e);
			closeDBConnection(dwhrepRock);
			throw e;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if ( s != null){
					s.close();
				}
				closeDBConnection(dwhrepRock);
			} catch (SQLException e) {
				log.warning("Not able to close statement in takeCustomView" + e);
			}
		}
		
		if (TPVersion != null){
			techpackDir += TECHPACKNAME + "_" + TPVersion + File.separator;
		} else {
			techpackDir += TECHPACKNAME + File.separator;
		}
		
		File customViewDir = new File(techpackDir);
		if (!customViewDir.exists()){
			customViewDir.mkdirs();
		}
		
		for(String eachView : diffViewList){
			writeIntoBackupFile(eachView, techpackDir);
		}
	}

	/**
	 * Method to write into view backup file. 
	 * 
	 * @param eachView
	 * @param techpackDir
	 * @throws Exception
	 */
	private void writeIntoBackupFile(String eachView, String techpackDir) throws Exception {
		RockFactory dcRock = getDcConnection();
		RockFactory dbaRock = getDbaConnection();
		Statement s = null;
		ResultSet rs = null;
		String viewFilename = techpackDir + eachView + ".sql"; 
		String dumpViewSQL = "select viewtext from sysviews where vcreator = 'dc' and viewname = '" + eachView + "'";
		String grantViewSQL = "SELECT 'GRANT SELECT ON '+USER_NAME(creator)+'.'+table_name+' TO '+USER_NAME(grantee)+' " +
				"FROM '+USER_NAME(grantor)+';' as viewPermission FROM systable, systableperm, sysuser " +
				"WHERE stable_id = systable.table_id AND systable.creator=user_id AND grantee IN (USER_ID('dcbo'), " +
				"USER_ID('dcpublic')) AND table_name in ('"+eachView+"')";
		
		try {
			log.info("Taking backup of viewtext for " +eachView);
			s = dcRock.getConnection().createStatement();
			s.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			rs = s.executeQuery(dumpViewSQL);
			if (rs != null){
				while (rs.next()){
					String viewText = rs.getString("viewtext");
					
					try (BufferedWriter bw = new BufferedWriter(new FileWriter(viewFilename))) {
						bw.write(viewText + ";");
					} catch (IOException ie ){
						log.severe("IOException occurred during view backup " +ie);
						throw ie;
					}
				}
			}
			
			log.info("Taking backup of view grants for " +eachView);
			s = dbaRock.getConnection().createStatement();
			rs = s.executeQuery(grantViewSQL);
			if (rs != null){
				while (rs.next()){
					String viewGrants = rs.getString("viewPermission");
					
					try (BufferedWriter bw = new BufferedWriter(new FileWriter(viewFilename, true))) {
						bw.write(viewGrants);
					} catch (IOException ie ){
						log.severe("IOException occurred during view grants " +ie);
						throw ie;
					}
				}
			}
			log.info("Successfully taken backup of " +eachView);
		} catch (SQLException esc) {
			log.warning("Exception while saving views in writeIntoBackupFile " + esc);
			throw esc;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (s != null) {
					s.close();
				}
				closeDBConnection(dcRock);
				closeDBConnection(dbaRock);
			} catch (Exception e) {
				log.warning("Error closing statement" + e.getMessage());
			}
		}
		
	}
	

	/**
	 * Method to close DB Connection
	 * 
	 * @param dbConn
	 */
	private void closeDBConnection(RockFactory dbConn){
		if (dbConn != null) {
			try {
				dbConn.getConnection().close();
			} catch (SQLException e) {
				log.warning("Not able to close Connection " + e);
			}
		}
	}
}
