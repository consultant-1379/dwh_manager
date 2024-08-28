package com.distocraft.dc5000.dwhm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import java.util.ArrayList;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;

import ssc.rockfactory.RockFactory;

/**
 * Locks all the partitions of the Techpack.
 *  
 * @author xtouoos
 *
 */
public class LockTable {

	final private Logger log;

	private final RockFactory dwhrock;

	private final RockFactory dwhRepRock;
	
	private static final String ACTIVE = "ACTIVE";

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
	
	private static List<String> listOfViews = new ArrayList<String>();
	
	private int retryCount = 0;
	
	private boolean isRetry = false;

	public LockTable(final RockFactory reprock, final RockFactory dwhrock, final Logger clog) throws SQLException {
		this.dwhRepRock = reprock;
		this.dwhrock = dwhrock;
		log = Logger.getLogger(clog.getName() + ".dwhm.LockTable");
	}
	
	/**
	 * Method to lock all partition using dba.forceLockTableList procedure. 
	 * 
	 * @param techPack
	 * @throws Exception
	 */
	public void lockPartition(String techPack) throws Exception {
		log.info("Locking Partitions started at " + sdf.format(new Date(System.currentTimeMillis())));

		final List<Dwhpartition> partitionList = getDWHPartitions(techPack);
		log.finest("There are " + partitionList.size() + " available for " + techPack);

		final Iterator<Dwhpartition> iter = partitionList.iterator();
		String tableNameList = "";
		
		// Prepare to execute store procedure
		while (iter.hasNext()) {
			tableNameList += iter.next().getTablename() + ",";
		}
		
		if (tableNameList.length() > 0) {
			tableNameList = tableNameList.substring(0, tableNameList.length() - 1);
		} else {
			// No Partition to lock. Return.
			log.info("No Partition available to acquire lock for " + techPack);
			return;
		}
		
		log.info("Need to acquire exclusive lock on " + partitionList.size() + " partitions of " + techPack);
		String lockProcSql = "call dba.forceLockTableList('" + tableNameList + "');";
		log.fine("Store procedure list are : " +lockProcSql);

		while (retryCount < 2) {
			try (Statement stmnt = dwhrock.getConnection().createStatement()) {
				stmnt.execute(lockProcSql);
				log.info("Acquired exclusive lock on " + partitionList.size() + " partitions of " + techPack);
				isRetry = false;
			} catch (SQLException esc) {
				isRetry = true;
				log.warning("Exception while running store procedure to Lock the table. " + esc);
			}  
			
			if (isRetry) {
				log.info("Not able to acquire exclusive lock. Retrying again...");
				retryCount++;
			} else {
				isRetry = false;
				break;
			}
		}
		
		if (isRetry) {
			ForceDropView fdv = new ForceDropView(techPack, log);
			fdv.lockDBUsers(); 
			try (Statement stmnt = dwhrock.getConnection().createStatement()){
				stmnt.execute(lockProcSql);
				log.info("Acquired exclusive lock on " + partitionList.size() + " partitions of " + techPack);
			} catch (SQLException esc) {
				log.warning("Exception to Lock the table. Even after DB User lock. " + esc);
			} 
			fdv.unlockDBUsers();
		}
		
		log.info("Locked all partitions at " + sdf.format(new Date(System.currentTimeMillis())));
	}

	/**
	 * Provides the list of partitions for the given Techpack
	 * 
	 * @param techPack
	 * @return
	 * @throws Exception
	 */
	public List<Dwhpartition> getDWHPartitions(String techPack) throws Exception {
		final List<Dwhpartition> result = new ArrayList<Dwhpartition>();
		try {
			// Tpactivation
			final Tpactivation tpActivation = new Tpactivation(dwhRepRock);
			tpActivation.setStatus(ACTIVE);
			tpActivation.setTechpack_name(techPack);

			final TpactivationFactory tpActivationFactory = new TpactivationFactory(dwhRepRock, tpActivation);

			if (tpActivationFactory != null) {
				for (Tpactivation tpaftmp : tpActivationFactory.get()) {
					try {
						// Dwhtype
						final Dwhtype dtp = new Dwhtype(dwhRepRock);
						dtp.setTechpack_name(tpaftmp.getTechpack_name());
						final DwhtypeFactory dwhTypeFactory = new DwhtypeFactory(dwhRepRock, dtp);

						if (dwhTypeFactory != null) {
							Iterator<Dwhtype> inneriter;
							inneriter = dwhTypeFactory.get().iterator();

							while (inneriter.hasNext()) {
								try {
									final Dwhtype dwhType = inneriter.next();
									// DWHPartitions
									final Dwhpartition dwhPartition = new Dwhpartition(dwhRepRock);
									dwhPartition.setStorageid(dwhType.getStorageid());
									final DwhpartitionFactory dwhPartitionFactory = new DwhpartitionFactory(
											dwhRepRock, dwhPartition);

									if (dwhPartitionFactory != null) {
										// loop DWHPartitions
										for (Dwhpartition dwhPArtitionTmp : dwhPartitionFactory.get()) {
											try {
												// collect datanames
												result.add(dwhPArtitionTmp);
											} catch (Exception e) {
												log.log(Level.WARNING, "Error while iterating Dwhpartition", e);
												throw (e);
											}
										}
									}
								} catch (Exception e) {
									log.log(Level.WARNING, "Error while iterating Dwhtype", e);
									throw (e);
								}
							}
						}
					} catch (Exception e) {
						log.log(Level.WARNING,
								"Error while iterating Tpactivation table", e);
						throw (e);
					}
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Error while retrieving Versioning table", e);
			throw (e);
		}

		return result;
	}
	
	/**
	 * Get all the views from DWHDB. 
	 * 
	 * @param techpackname
	 * @return
	 */
	public List<String> getAllView(final String techpackname) {
		ResultSet res = null;
		listOfViews.clear();
		
		try {
			Statement stmnt = dwhrock.getConnection().createStatement();
			String viewSQL = "select viewname from sysviews where viewname like '"
					+ techpackname + "|_%' escape '|' and vcreator = 'dc'";
			
			if (techpackname.equalsIgnoreCase("DC_E_IMS")){
				viewSQL = "select viewname from sysviews where viewname like 'DC_E_IMS|_%' escape '|' " +
						"and viewname not like 'DC_E_IMS_IPW|_%' escape '|' and vcreator = 'dc'";
			} else if (techpackname.equalsIgnoreCase("DC_E_CMN_STS")) {
				viewSQL = "select viewname from sysviews where viewname like 'DC_E_CMN_STS|_%' escape '|' " +
						"and viewname not like 'DC_E_CMN_STS_PC|_%' escape '|' and vcreator = 'dc'";
			} else if (techpackname.equalsIgnoreCase("DC_E_SASN")) {
				viewSQL = "select viewname from sysviews where viewname like 'DC_E_SASN|_%' escape '|' " +
						"and viewname not like 'DC_E_SASN_SARA|_%' escape '|' and vcreator = 'dc'";
			}
			
			try {
				res = stmnt.executeQuery(viewSQL);
				if (res != null) {
					while (res.next()){
						String viewName = res.getString("viewname");
						listOfViews.add(viewName.trim());
					}
				} 
			} catch (SQLException e) {
				log.warning("Exception while getting views " + e);
				e.getMessage();
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
			e.printStackTrace();
		} 
		
		return listOfViews;
	}
}
