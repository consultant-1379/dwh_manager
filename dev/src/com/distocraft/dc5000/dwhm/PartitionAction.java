package com.distocraft.dc5000.dwhm;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Partitionplan;
import com.distocraft.dc5000.repository.dwhrep.PartitionplanFactory;
import com.ericsson.eniq.common.Constants;

public class PartitionAction {

	private static final int UNKNOWN_PARTITIONSIZE = -1;

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	private static final Hashtable partitionBalancers = new Hashtable();

	private RockFactory reprock = null;

	private RockFactory dwhrock = null;

	private Logger log = null;

	private Logger performanceLog = null;

	private boolean currTypeUsesDefPartPlan = false;

	private boolean isCalledByNVU = false;

	private boolean customInstallFlag = false;

	final File eniq_status_file = new File(Constants.TP_INSTALL_FILE);

	public PartitionAction(final Logger clog, final boolean customInstallFag) {
		this.log = clog;
		this.customInstallFlag = customInstallFag;
	}

	/**
	 * @param reprock
	 * @param dwhrock
	 * @param techPack
	 * @param clog
	 * @param nvu
	 *            The boolean value set as true when called for NVU (Generic
	 *            SIU), afj_manager
	 * @throws Exception
	 *             This constructor is only called from NVU (Generic SIU).
	 */
	public PartitionAction(final RockFactory reprock, final RockFactory dwhrock, final String techPack,
			final Logger clog, final boolean isCalledByNVU) throws Exception {
		this.isCalledByNVU = isCalledByNVU;
		executeAction(reprock, dwhrock, techPack, clog);
	}

	/**
	 * @param reprock
	 * @param dwhrock
	 * @param techPack
	 * @param clog
	 * @throws Exception
	 *             Original constructor
	 */
	public PartitionAction(final RockFactory reprock, final RockFactory dwhrock, final String techPack,
			final Logger clog) throws Exception {
		executeAction(reprock, dwhrock, techPack, clog);
	}

	/**
	 * @param reprock
	 * @param dwhrock
	 * @param techPack
	 * @param clog
	 * @throws Exception
	 *             Common execution logic method for both constructors
	 */
	public void executeAction(final RockFactory reprock, final RockFactory dwhrock, final String techPack,
			final Logger clog) throws Exception {
		this.reprock = reprock;
		this.dwhrock = dwhrock;

		partitionAction(techPack, clog);

	}

	protected void partitionAction(final String techPack, final Logger clog) throws SQLException, RockException {
		log = Logger.getLogger(clog.getName() + ".dwhm.PartitionAction");
		performanceLog = Logger.getLogger("performance" + log.getName().substring(log.getName().indexOf(".")));

		if (techPack == null) {
			log.severe("Techpack not defined");
			return;
		}

		final ActivationCache ac = getActivationCache();

		if (!ac.isActive(techPack)) {
			log.fine("Techpack " + techPack + " is not active");
			return;
		}

		final Dwhtechpacks dtp_cond = new Dwhtechpacks(reprock);
		dtp_cond.setTechpack_name(techPack);
		final DwhtechpacksFactory dtp_fact = new DwhtechpacksFactory(reprock, dtp_cond);

		final Vector tps = dtp_fact.get();

		if (tps.size() > 1) {
			log.severe("Panic: Found multiple techpacks with name " + techPack);
			return;
		} else if (tps.size() < 1) {
			log.severe("Techpack " + techPack + " not found from dwhrep");
			return;
		}

		final Dwhtype dt_cond = new Dwhtype(reprock);
		dt_cond.setTechpack_name(techPack);
		final DwhtypeFactory dt_fact = new DwhtypeFactory(reprock, dt_cond);

		final Vector types = dt_fact.get();

		log.fine("Found " + types.size() + " types for techpack");

		final long now = System.currentTimeMillis();

		final Enumeration enu = types.elements();

		while (enu.hasMoreElements()) {
			final Dwhtype type = (Dwhtype) enu.nextElement();

			if (!ac.isActive(type.getTechpack_name(), type.getTypename(), type.getTablelevel())) {
				log.fine("Type " + type.getTypename() + " (" + type.getTablelevel() + ") is not active");
				continue;
			}

			try {

				// Initially set to false
				currTypeUsesDefPartPlan = false;

				final Dwhpartition dp_cond = new Dwhpartition(reprock);
				dp_cond.setStorageid(type.getStorageid());
				final DwhpartitionFactory dp_fact = new DwhpartitionFactory(reprock, dp_cond);

				final Vector<Dwhpartition> partitions = dp_fact.get();

				if (partitions.size() <= 0) {
					log.info("Type " + type.getStorageid() + ": No partitions available");
					continue;
				} else {
					log.info("Type " + type.getStorageid() + ": " + partitions.size() + " partitions available");
				}

				if ("partitioned".equalsIgnoreCase(type.getType())) {
					final Partitionplan partitionPlan = getPartitionPlan(type);

					if (type.getPartitionsize() == UNKNOWN_PARTITIONSIZE) {
						// Get the default value from table PARTITIONPLAN.
						final Long defaultPartitionSize = partitionPlan.getDefaultpartitionsize();
						this.log.info("Type " + type.getTypename() + " uses defaultpartitionsize with value "
								+ defaultPartitionSize);
						type.setPartitionsize(defaultPartitionSize);
						currTypeUsesDefPartPlan = true;
					}

					log.finer("Partitioned type " + type.getTypename() + " (" + type.getTablelevel() + ")");

					if (partitionPlan.getPartitiontype() == StorageTimeAction.TIME_BASED_PARTITION_TYPE) {
						partitioned(now, type, partitions);
					}

					log.fine("Partitioning performed");

				} else if ("unpartitioned".equalsIgnoreCase(type.getType())) {

					log.finer("Unpartitioned type " + type.getTypename() + " (" + type.getTablelevel() + ")");

					unpartitioned(ac, now, type, partitions);

				} else if ("simple".equalsIgnoreCase(type.getType())) {

					log.finer("Simple type " + type.getTypename() + " no partitioning");

				} else {
					log.warning("Unknown (" + type.getType() + ") type " + type.getStorageid());
				}
			} catch (Exception e) {
				log.log(Level.WARNING, "Partitioning failed for " + type.getStorageid(), e);
			}

		} // for each type

		// Fix for TR - HN66650. Ignore the PhysicalTableCache revalidation if
		// called from NVU (Generic SIU implementation).
		if (!isCalledByNVU) {
			doRevalidation();
		}

	}

	/**
	 * Extracted out for testing purposes
	 */
	protected PhysicalTableCache getPhysicalTableCache() {
		return PhysicalTableCache.getCache();
	}

	/**
	 * Extracted out for testing purposes
	 *
	 * @return
	 */
	protected ActivationCache getActivationCache() {
		return ActivationCache.getCache();
	}

	/**
	 * Extracted out for testing purposes
	 *
	 * @return
	 */
	protected void doRevalidation() {
		try {
			getPhysicalTableCache().revalidate();
		} catch (Exception e) {
			log.log(Level.WARNING, "Cache revalidation failed", e);
		}

	}

	private void unpartitioned(final ActivationCache ac, final long now, final Dwhtype type, final Vector partitions)
			throws Exception, SQLException, RockException {

		final long storage = ac.getStorageTime(type.getTechpack_name(), type.getTypename(), type.getTablelevel());

		if (storage > 0 && type.getDatadatecolumn() != null) {

			log.info("Housekeep for unpartitioned type. Storage time " + storage);

			final Calendar cal = new GregorianCalendar();
			final int oldestStorageTime = (int) (-1 * storage);
			cal.add(Calendar.DATE, oldestStorageTime);
			final long old = cal.getTimeInMillis();

			log.fine("StorageTime for type " + type.getStorageid() + " is [" + sdf.format(new Date(old)) + " ... "
					+ sdf.format(new Date(now)) + "]");

			final Dwhpartition partition = (Dwhpartition) partitions.get(0);

			deleteOlderData(partition, old, type.getDatadatecolumn());

			partition.setStarttime(new Timestamp(now));
			partition.updateDB();

			log.fine("Housekeep performed");

		} else {

			log.finer("no housekeep");

		}

	}

	private void partitioned(final long now, final Dwhtype type, final Vector partitions)
			throws Exception, SQLException, RockException {

		Dwhpartition latest = getLatest(partitions);

		if (latest == null) {

			log.fine("First partitioning for type");

			firstPartitioning(type, partitions);

			latest = getLatest(partitions);

		}

		// We have missed partitionings -> perform them
		while (latest.getEndtime().getTime() < now) {

			log.info("Missed partitioning: now " + sdf.format(new Date(now)) + " last partition "
					+ latest.getTablename() + " [" + sdf.format(new Date(latest.getStarttime().getTime())) + " - "
					+ sdf.format(new Date(latest.getEndtime().getTime())));
				latest = step(latest, partitions, type);
		}

		// Partitionings done up to this moment of time
		final long limit = now + (48 * 60 * 60 * 1000);

		log.finest("Checking partitioning need: Limit = " + sdf.format(new Date(limit)) + " last partition ends "
				+ sdf.format(new Date(latest.getEndtime().getTime())));

		// Perform next partitioning
		if (latest.getEndtime().getTime() > limit) {
			log.finer("Time for partition change is not yet");
		} else {
			// If partition size is only 1 day, then, we will need to step more
			// than 1 partition. This is the reason for the while loop.
			// If partition size is only 1 day, then, we could run into issues
			// in different time zones with no partition available.
			while (latest.getEndtime().getTime() <= limit) {
					latest = step(latest, partitions, type);
			}
		}

		log.fine("Checking MIGRATED partition");

		final ActivationCache ac = ActivationCache.getCache();

		final Iterator pi = partitions.iterator();

		while (pi.hasNext()) {
			final Dwhpartition dp = (Dwhpartition) pi.next();

			if (("MIGRATED".equalsIgnoreCase(dp.getStatus()) || "INSANE_MG".equalsIgnoreCase(dp.getStatus()))
					&& dp.getStarttime() != null) {

				log.finer("Migrated partition " + dp.getTablename() + " " + dp.getStarttime() + " ... "
						+ dp.getEndtime());

				final long storage = ac.getStorageTime(type.getTechpack_name(), type.getType(), type.getTablelevel());

				final long storagestart = now - (storage * 24 * 60 * 60 * 1000);

				if (dp.getEndtime().before(new Date(storagestart))) {
					log.fine("Partition has entirely passed storagetime");

					Statement stmt = null;

					try {
						stmt = reprock.getConnection().createStatement();
						if (System.getProperty("dwhm.test") == null) {
							stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
						}

						truncateTableAndRebuildIndex(dp.getTablename(), stmt);

					} finally {
						try {
							stmt.close();
						} catch (Exception e) {
						}
					}

					log.fine("Partition cleared successfully");

					dp.setStarttime(null);
					dp.setEndtime(null);

					dp.updateDB();

					log.fine("Partition status stored successfully");

				} else {
					log.finer("Partition still contains active data");
				}

			}

		}

	}

	/**
	 * Truncate the rebuild the index JIRA
	 * http://jira-oss.lmera.ericsson.se/browse/EQEV-21012
	 * 
	 * @throws Exception
	 */
	private void truncateTableAndRebuildIndex(final String tablename, final Statement stmt) throws Exception {
		try {
			if (eniq_status_file.exists()) {
				stmt.executeUpdate("truncate table " + tablename);
				stmt.executeUpdate("rebuild_idx 'dc." + tablename + "'");
				log.info("Table " + tablename + " truncated and index rebuilt");
			} else {
				log.info("Initial Install detected. rebuild index skipped");
			}
		} catch (SQLException e) {
			log.log(Level.WARNING, "Truncation and Index rebuild failed for " + tablename, e);
			throw e;
		}
		
	}

	/**
	 * Partitions specified type one step forward
	 * 
	 * @return Dwhpartition - latest partition after the step
	 */
	private Dwhpartition step(Dwhpartition latest, final Vector partitions, final Dwhtype type) throws Exception {

		log.info("Stepping " + type.getStorageid() + " forward");

		final Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(latest.getEndtime().getTime());

		latest = getReused(partitions);

		Calendar unroundedCal = cal;
		boolean roundPerformed = roundToMidnight(cal);

		if (roundPerformed) {
			this.log.fine("Rounded starttime from " + sdf.format(unroundedCal.getTime()) + " to "
					+ sdf.format(cal.getTime()) + ".");
		}

		latest.setStarttime(new Timestamp(cal.getTimeInMillis()));

		final int days = (int) (type.getPartitionsize() / 24);
		cal.add(Calendar.DATE, days);

		unroundedCal = cal;
		// NOTE: Maybe the rounding of endtime is not needed, because only full
		// days
		// are added to cal?
		roundPerformed = roundToMidnight(cal);

		if (roundPerformed) {
			this.log.fine("Rounded endtime from " + sdf.format(unroundedCal.getTime()) + " to "
					+ sdf.format(cal.getTime()) + ".");
		}

		latest.setEndtime(new Timestamp(cal.getTimeInMillis()));

		log.finer("Step: latest partition " + latest.getTablename() + " ["
				+ sdf.format(new Date(latest.getStarttime().getTime())) + " - "
				+ sdf.format(new Date(latest.getEndtime().getTime())) + "]");

		latest.setStatus("ACTIVE");

		clearPartition(latest);

		latest.updateDB();

		type.setNextpartitiontime(latest.getEndtime());

		Long defPartPlanValue = new Long(-1);

		// Set the -1 as PartitionSize before updating value to database.
		// This needs to be done so that the defaultpartitionsize does not save
		// as
		// excplicit value to table DWHType.
		if (this.currTypeUsesDefPartPlan) {
			defPartPlanValue = type.getPartitionsize();
			type.setPartitionsize(new Long(-1));
		}

		type.updateDB();

		if (this.currTypeUsesDefPartPlan) {
			type.setPartitionsize(defPartPlanValue);
		}

		return latest;

	}

	/**
	 * Performs first partitioning of partitioned type. Partitions are assigned
	 * a storagetime back from current day.
	 */
	public void firstPartitioning(final Dwhtype type, final Vector partitions) throws SQLException, RockException {

		int startDateOffset = -1;

		// Start time of current day. (Balance this)
		final Calendar cal = new GregorianCalendar();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);

		final Long partSizeInDays = new Long(type.getPartitionsize().longValue() / 24);

		if (customInstallFlag && partitions.size() == 1) {
			startDateOffset = -3;
		}

		PartitionBalancer b = (PartitionBalancer) partitionBalancers.get(partSizeInDays);

		if (b == null) {
			b = new PartitionBalancer();
			b.setSize(partSizeInDays.intValue());
			partitionBalancers.put(partSizeInDays, b);
		}

		try {
			// changed from -1 to -3 so that the partition will contain for last
			// n-3 days.
			if (!customInstallFlag) {
				cal.add(Calendar.DATE, startDateOffset * b.getNextValue());
			} else {
				cal.add(Calendar.DATE, startDateOffset);
			}

		} catch (Exception e) {
			this.log.severe("PartitionBalancer function getNextValue failed. Using default value of 1.");
			cal.add(Calendar.DATE, startDateOffset);
		}

		type.setNextpartitiontime(
				new Timestamp(cal.getTimeInMillis() + (type.getPartitionsize().longValue() * 60 * 60 * 1000)));

		final Iterator it_parts = partitions.iterator();

		while (it_parts.hasNext()) {
			final Dwhpartition part = (Dwhpartition) it_parts.next();

			Calendar unroundedCal = cal;
			boolean roundPerformed = roundToMidnight(cal);

			if (roundPerformed) {
				this.log.fine("Rounded starttime from " + sdf.format(unroundedCal.getTime()) + " to "
						+ sdf.format(cal.getTime()) + ".");
			}

			part.setStarttime(new Timestamp(cal.getTimeInMillis()));

			final int days = type.getPartitionsize().intValue() / 24;
			// This is done because daylite saving might otherwise break
			// partition
			// limits
			cal.add(Calendar.DATE, days);

			unroundedCal = cal;
			// NOTE: Maybe the rounding of endtime is not needed, because only
			// full
			// days
			// are added to cal?
			roundPerformed = roundToMidnight(cal);

			if (roundPerformed) {
				this.log.fine("Rounded endtime from " + sdf.format(unroundedCal.getTime()) + " to "
						+ sdf.format(cal.getTime()) + ".");
			}

			part.setEndtime(new Timestamp(cal.getTimeInMillis()));
			cal.add(Calendar.DATE, -1 * days);

			part.setStatus("ACTIVE");

			part.updateDB();

			cal.add(Calendar.HOUR, -1 * type.getPartitionsize().intValue());

		}

		Long defPartPlanValue = new Long(-1);

		// Set the -1 as PartitionSize before updating value to database.
		// This needs to be done so that the defaultpartitionsize does not save
		// as
		// excplicit value to table DWHType.
		if (this.currTypeUsesDefPartPlan) {
			defPartPlanValue = type.getPartitionsize();
			type.setPartitionsize(new Long(-1));
		}

		type.updateDB();

		if (this.currTypeUsesDefPartPlan) {
			type.setPartitionsize(defPartPlanValue);
		}
	}

	/**
	 * Drops all data from specified partition
	 * 
	 * @throws Exception
	 */
	protected void clearPartition(final Dwhpartition partition) throws Exception {
		Statement stmt = null;
		final String tablename = partition.getTablename();
		try {
			stmt = dwhrock.getConnection().createStatement();
			if (System.getProperty("dwhm.test") == null) {
				stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			}
			log.fine("Reuse partition " + tablename + ". Executing truncate.");

			truncateTableAndRebuildIndex(tablename, stmt);

			Logger.getLogger("dwhm." + partition.getStorageid() + "." + tablename).info("Table truncated for reuse");
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				log.log(Level.FINE, "Cleanup failed", e);
			}
		}
	}

	/**
	 * Returns the latest partition used. If no partitions used returns null.
	 */
	private Dwhpartition getLatest(final Vector partitions) {

		Dwhpartition ret = (Dwhpartition) partitions.get(0);

		Iterator i = partitions.iterator();
		while (i.hasNext()) {
			final Dwhpartition p = (Dwhpartition) i.next();

			if ("MANUAL".equals(p.getStatus()) || "INSANE_MA".equalsIgnoreCase(p.getStatus())
					|| "MIGRATED".equalsIgnoreCase(p.getStatus()) || "INSANE_MG".equalsIgnoreCase(p.getStatus())) {
				i.remove();
			}
		}

		i = partitions.iterator();

		while (i.hasNext()) {
			final Dwhpartition c = (Dwhpartition) i.next();

			final Timestamp tr = ret.getStarttime();
			final Timestamp tc = c.getStarttime();

			if (tr == null && tc == null) {
				continue;
			}

			if (tr == null && tc != null) {
				ret = c;
			} else if (tc == null && tr != null) {
				continue;
			} else if (tc.getTime() > tr.getTime()) {
				ret = c;
			}
		}

		if (ret.getStarttime() != null) {
			return ret;
		} else {
			return null;
		}
	}

	/**
	 * Return oldest partition for reuse. If no partitions used returns first.
	 */
	private Dwhpartition getReused(final Vector partitions) {

		log.fine("Total partitions " + partitions.size());

		Dwhpartition ret = (Dwhpartition) partitions.get(0);

		Iterator i = partitions.iterator();
		while (i.hasNext()) {
			final Dwhpartition p = (Dwhpartition) i.next();

			if ("MANUAL".equals(p.getStatus()) || "INSANE_MA".equalsIgnoreCase(p.getStatus())
					|| "MIGRATED".equalsIgnoreCase(p.getStatus()) || "INSANE_MG".equalsIgnoreCase(p.getStatus())) {
				i.remove();

			}
		}

		log.fine("Reusable partitions " + partitions.size());

		i = partitions.iterator();
		while (i.hasNext()) {
			final Dwhpartition c = (Dwhpartition) i.next();

			final Timestamp tr = ret.getStarttime();
			final Timestamp tc = c.getStarttime();

			if (tr == null && tc == null) {
				continue;
			} else if (tr == null && tc != null) {
				continue;
			} else if (tc == null && tr != null) {
				ret = c;
			} else if (tc.getTime() < tr.getTime()) {
				ret = c;
			}
		}

		return ret;
	}

	/**
	 * Deletes too old data from a partition.
	 * 
	 * @throws Exception
	 *             in case of failure
	 */
	protected void deleteOlderData(final Dwhpartition partition, final long oldest_time, final String datecolumn)
			throws Exception {

		final long start = System.currentTimeMillis();

		final StringBuffer sql = new StringBuffer("DELETE FROM ");
		sql.append(partition.getTablename()).append(" WHERE ");
		sql.append(datecolumn).append(" < ?");

		PreparedStatement ps = null;
		try {
			ps = dwhrock.getConnection().prepareStatement(sql.toString());
			if (System.getProperty("dwhm.test") == null) {
				ps.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			}
			ps.setTimestamp(1, new Timestamp(oldest_time));
			final int amount = ps.executeUpdate();

			Logger.getLogger("dwhm." + partition.getStorageid() + "." + partition.getTablename().toString())
					.info("Housekeep deleted " + amount + " rows");

		} finally {
			try {

				if (ps != null) {
					ps.close();
				}

			} catch (Exception e) {
			}
		}

		performanceLog.fine(
				"Delete from " + partition.getTablename() + " took " + (System.currentTimeMillis() - start) + " ms");

	}

	/**
	 * This function returns the partitionplan to be used for the DWHType
	 * 
	 * @param type
	 *            Dwhtype object containing values of the target Dwhtype.
	 * @return Returns the value of PartitionSize to use.
	 * @throws Exception
	 */
	protected Partitionplan getPartitionPlan(final Dwhtype type) throws SQLException, RockException, Exception {

		final String partitionPlan = type.getPartitionplan();

		if (partitionPlan == null || partitionPlan.equalsIgnoreCase("")) {
			this.log.severe("Partitionplan for type " + type.getTypename() + " is undefined.");
			throw new Exception("Partitionplan for type " + type.getTypename() + " is undefined.");
		}

		final Partitionplan wherePartitionPlan = new Partitionplan(this.reprock);
		wherePartitionPlan.setPartitionplan(partitionPlan);
		final PartitionplanFactory partitionPlanFactory = getPartitionPlanFactory(wherePartitionPlan);
		final Vector partitionPlanVector = partitionPlanFactory.get();

		if (partitionPlanVector.size() == 0) {
			this.log.severe("No partitionplan named " + partitionPlan + " found for type " + type.getTypename() + ".");
			throw new Exception(
					"No partitionplan named " + partitionPlan + " found for type " + type.getTypename() + ".");
		}

		final Partitionplan targetPartitionPlan = (Partitionplan) partitionPlanVector.get(0);
		return targetPartitionPlan;
	}

	/**
	 * Extracted out for testing purposes
	 * 
	 * @param wherePartitionPlan
	 * @return
	 * @throws SQLException
	 * @throws RockException
	 */
	protected PartitionplanFactory getPartitionPlanFactory(final Partitionplan wherePartitionPlan)
			throws SQLException, RockException {
		return new PartitionplanFactory(this.reprock, wherePartitionPlan);
	}

	/**
	 * This function rouns the time of calendar to the closest midnight.
	 * 
	 * @param cal
	 *            Java Calendar object to round.
	 * @return Returns true if rounding is needed (when hours != 00). Returns
	 *         false if no rounding is performed.
	 */
	private boolean roundToMidnight(final Calendar cal) {

		if (cal.get(Calendar.HOUR_OF_DAY) != 0) {

			if (cal.get(Calendar.HOUR_OF_DAY) >= 12) {
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.add(Calendar.DATE, 1);
			} else {
				cal.set(Calendar.HOUR_OF_DAY, 0);
			}

			return true;
		} else {
			return false;
		}
	}

}
