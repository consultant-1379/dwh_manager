package com.distocraft.dc5000.dwhm;

import com.ericsson.eniq.common.TechPackType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
//import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Partitionplan;
import com.distocraft.dc5000.repository.dwhrep.PartitionplanFactory;

public class SanityChecker {

  private static final String EVENT_E = "EVENT_E";

  private static final String PARTITION_STATUS_ACTIVE          = "ACTIVE";
  private static final String PARTITION_STATUS_INSANE_ACTIVE   = "INSANE_AC";
  private static final String PARTITION_STATUS_INSANE          = "INSANE";
  private static final String PARTITION_STATUS_MANUAL          = "MANUAL";
  private static final String PARTITION_STATUS_INSANE_MANUAL   = "INSANE_MA";
  private static final String PARTITION_STATUS_MIGRATED        = "MIGRATED";
  private static final String PARTITION_STATUS_INSANE_MIGRATED = "INSANE_MG";
  private static final String PARTITION_STATUS_READONLY        = "READONLY";
  private static final String PARTITION_STATUS_INSANE_READONLY = "INSANE_RO";
  private static final String PARTITION_STATUS_NEW             = "NEW";
  private static final String PARTITION_STATUS_INSANE_NEW      = "INSANE_NE";
  
  private static final String PARTITION_TYPE_UNPARTITIONED = "UNPARTITIONED";
  private static final String PARTITION_TYPE_PARTITIONED   = "PARTITIONED";
  private static final String PARTITION_TYPE_SIMPLE        = "SIMPLE";
	  
  private static final String S0002 = "S0002";

  //http://dcx.sybase.com/1100/en/saerrors_en11/errm141.html
  private static final int TABLE_NOT_FOUND_ERROR_CODE = 2706;
  
  private static final String SELECT_COUNT_FROM = "SELECT count(*) FROM ";

  final private RockFactory reprock;

  final private RockFactory dwhrock;
  
//  final private RockFactory dbarock;

  final private Logger log;

  final private Logger performanceLog;  

  private boolean isCalledByNVU = false;
  
//  private final String DC_SCHEMA = "dc.";

  public SanityChecker(final RockFactory reprock, final RockFactory dwhrock, final Logger clog) {
    this(reprock, dwhrock, false, clog);

  }
  /**
  * @param reprock
  * @param dwhrock
  * @param isCalledByNVU
  * The boolean value set as true when called for NVU (Generic SIU), afj_manager -> StorageTimeAction -> SanityChecker
  * @param clog
  */
 public SanityChecker(RockFactory reprock, RockFactory dwhrock, boolean isCalledByNVU, Logger clog) {
     this.isCalledByNVU = isCalledByNVU;
     this.reprock = reprock;
     this.dwhrock = dwhrock;
//     this.dbarock = dbarock;
     log = Logger.getLogger(clog.getName() + ".dwhm.SanityChecker");
     performanceLog = Logger.getLogger("performance" + log.getName().substring(log.getName().indexOf(".")));

   if(isCalledByNVU){
     log.info("New constructor call for Node Version Update");
   }

   }

/**
   * Checks for specified techpack that repository is consistent. Errorneous
   * partitions are marked with INSANE status.
   * 
   * @return true if all partitions were fine false otwerwise
   */
  public boolean sanityCheck(final Dwhtechpacks tp) {
    if (tp == null) {
      return true;
    }

    final long startstamp = System.currentTimeMillis();

    boolean result = true;

    try {

      final Dwhtype dt_cond = new Dwhtype(reprock);
      dt_cond.setTechpack_name(tp.getTechpack_name());
      final DwhtypeFactory dt_fact = new DwhtypeFactory(reprock, dt_cond);

      final Vector<Dwhtype> types = dt_fact.get();

      final Enumeration<Dwhtype> en = types.elements();
      
      Dwhtype type = null;
      
      while (en.hasMoreElements()) {
        type = en.nextElement();

        if (!sanityCheck(type)) {
          result = false;
        }

      }

    } catch (Exception e) {
      log.log(Level.WARNING, "Sanity check error", e);

      result = false;
    }

    final long total = (System.currentTimeMillis() - startstamp) / 60L;
    performanceLog.info("Sanity check for " + tp.getTechpack_name() + " in " + total + " seconds");

    return result;

  }

  /**
   * Checks for specified type that repository is consistent. Errorneous
   * partitions are marked with INSANE status.
   * 
   * @return true if all partitions were fine false otwerwise
   */
  public boolean sanityCheck(final Dwhtype type) {

    log.fine("SanityCheck for type " + type.getStorageid());

    final long startstamp = System.currentTimeMillis();

    //EEIKBE: bug fix. This Method never returns true if the partition is OK.
    //boolean result = false;
    boolean result = true;
    Connection con = null;

    try {

      final Dwhpartition dp_cond = new Dwhpartition(reprock);
      dp_cond.setStorageid(type.getStorageid());
      final DwhpartitionFactory dp_fact = new DwhpartitionFactory(reprock, dp_cond);

      final Vector<Dwhpartition> partitions = dp_fact.get();

      if (partitions.size() <= 0) {
        log.finest("No partitions found.");
        return true;
      } else {
        log.fine(partitions.size() + " partitions found");
      }

      if (type.getPartitioncount() != partitions.size()) {
        log.fine("Fixing partition count for type " + type.getStorageid());
        type.setPartitioncount(new Long(partitions.size()));
        type.updateDB();
      }

      if (partitions.size() > 1 && type.getType().equalsIgnoreCase(PARTITION_TYPE_UNPARTITIONED)) {
        throw new Exception("Unpartitioned type " + type.getType() + " has multiple partitions!");
      }

      if (partitions.size() > 1 && type.getType().equalsIgnoreCase(PARTITION_TYPE_SIMPLE)) {
        throw new Exception("Simple type " + type.getType() + " has multiple partitions!");
      }

      final Dwhcolumn dc_cond = new Dwhcolumn(reprock);
      dc_cond.setStorageid(type.getStorageid());
      final DwhcolumnFactory dc_fact = new DwhcolumnFactory(reprock, dc_cond);

      final Vector<Dwhcolumn> columns = dc_fact.get();

      sortColumns(columns, type.getBasetablename());

      final Enumeration<Dwhpartition> e_partitions = partitions.elements();
      
      Dwhpartition partition = null;
      
      while (e_partitions.hasMoreElements()) {
        partition = e_partitions.nextElement();

        log.fine("Checking partition " + partition.getTablename());

         con = dwhrock.getConnection();

        try { // main try

          final short partitionType = getPartitionType(type);

          if (partitionType == StorageTimeAction.TIME_BASED_PARTITION_TYPE) {
            existenceCheckForTimeBasedPartitions(type, partition, con);
          } else {
            existenceCheckForVolumeBasedPartitions(partition, con);
          }

          log.finer("Existence & hygiene: OK");

          if (partition.existsDB()) {
            columnCheck(columns, partition, con);
          }

          log.finer("Columns: OK");

          if (partition.getStatus().startsWith(PARTITION_STATUS_INSANE)) {
            setStatusOfINSANEPartition(partition);
          }

          log.fine("Partition: " + partition.getTablename() + " is OK");

        } catch (SanityCheckException e) { // Partition WAS faulty
          log.log(Level.SEVERE, "Partition: " + partition.getTablename() + " is INSANE", e);

          Logger.getLogger("dwhm." + partition.getStorageid() + "." + partition.getTablename()).severe("Gone INSANE");

          setStatusOfPartition(partition);

          result = false;

        }

      } // for each Dwhpartition

    } catch (Exception e){//(Throwable t) { 
      log.log(Level.WARNING, "Sanity check error", e.getMessage());

      // Mark whole type ERRORNEOUS??

      result = false;
    }

    if(isCalledByNVU) {
      try {
        PhysicalTableCache.getCache().revalidate();
      } catch (Exception e) {
        log.log(Level.WARNING, "Cache revalidation failed", e);
      }
    }
    final long total = System.currentTimeMillis() - startstamp;
    performanceLog.info("Sanity check for type " + type.getStorageid() + " in " + total + " ms");

    return result;

  }
/**
 * This method sets the status of the Partition.
 * @param partition
 * @throws SQLException
 * @throws RockException
 */
private void setStatusOfPartition(final Dwhpartition partition) throws SQLException,
		RockException {
	if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_ACTIVE)) {
	    partition.setStatus(PARTITION_STATUS_INSANE_ACTIVE);
	  } else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_MANUAL)) {
	    partition.setStatus(PARTITION_STATUS_INSANE_MANUAL);
	  } else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_MIGRATED)) {
	    partition.setStatus(PARTITION_STATUS_INSANE_MIGRATED);
	  } else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_READONLY)) {
	    partition.setStatus(PARTITION_STATUS_INSANE_READONLY);
	  } else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_NEW)) {
	    partition.setStatus(PARTITION_STATUS_INSANE_NEW);
	  } else if (partition.getStatus().startsWith(PARTITION_STATUS_INSANE)) {
	    log.warning("Status already INSANE, status not changed from ("+partition.getStatus()+")");
	  } else {
	    log.warning("Undefined status " + partition.getStatus() + " on table " + partition.getTablename());
	    partition.setStatus(PARTITION_STATUS_INSANE);            
	  }

	  if (!partition.gimmeModifiedColumns().isEmpty()) {
	    partition.updateDB();
	  }
}

/**
 * This method sets the status of an INSANE partition.
 * @param partition
 * @throws SQLException
 * @throws RockException
 */
private void setStatusOfINSANEPartition(final Dwhpartition partition)
		throws SQLException, RockException {
	if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_ACTIVE)) {
	  partition.setStatus(PARTITION_STATUS_ACTIVE);
	} else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_MANUAL)) {
	  partition.setStatus(PARTITION_STATUS_MANUAL);
	} else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_MIGRATED)) {
	  partition.setStatus(PARTITION_STATUS_MIGRATED);
	} else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_READONLY)) {
	  partition.setStatus(PARTITION_STATUS_READONLY);
	} else if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_NEW)) {
	  partition.setStatus(PARTITION_STATUS_NEW);
	} else {
	  log.warning("Undefined status " + partition.getStatus() + " restoring to READONLY");
	  partition.setStatus(PARTITION_STATUS_READONLY);
	}
	partition.updateDB();

	Logger.getLogger("dwhm." + partition.getStorageid() + "." + partition.getTablename()).info(
	    "Sanity restored. Status restored to " + partition.getStatus());
}

  /**
   * Sorts columns differently depending on if the techpack is ENIQ stats or ENIQ Events
   * 
   * @param columns
   * @param baseTableName
   */
  protected void sortColumns(final List<Dwhcolumn> columns, final String baseTableName) {
    if (baseTableName.startsWith(EVENT_E)) {
      sortColumnsForEvents(columns);
    } else {
      sortColumnsForStats(columns);
    }
  }

  /**
   * Sorts colums for ENIQ Stats techpacks. Columns are sorted by column number
   * 
   * @param columns
   */
  private void sortColumnsForStats(final List<Dwhcolumn> columns) {
    Collections.sort(columns, new Comparator<Dwhcolumn>() {
      @Override
      public int compare(final Dwhcolumn o1, final Dwhcolumn o2) {
        return o1.getColnumber().compareTo(o2.getColnumber());
      }
    });
  }

  /**
   * Sorts colums for ENIQ Events techpacks. Columns are sorted by column name
   * 
   * @param columns
   */
  private void sortColumnsForEvents(final List<Dwhcolumn> columns) {
    Collections.sort(columns, new Comparator<Dwhcolumn>() {

      @Override
      public int compare(final Dwhcolumn o1, final Dwhcolumn o2) {
        // Need to remove "_" as sql "order by" does not order them correctly
        // with "_"
        final String columnName1 = o1.getDataname().replaceAll("_", "");
        final String columnName2 = o2.getDataname().replaceAll("_", "");

        return columnName1.compareTo(columnName2);
      }
    });
  }

  /**
   * This returns the partition type.
   * @param type
   * @return
   * @throws SQLException
   * @throws RockException
   */
  private short getPartitionType(final Dwhtype type) throws SQLException, RockException {
    final Partitionplan partitionPlanCondition = new Partitionplan(reprock);
    partitionPlanCondition.setPartitionplan(type.getPartitionplan());
    final PartitionplanFactory partitionplanFactory = new PartitionplanFactory(reprock, partitionPlanCondition);

    final Partitionplan partitionPlan = partitionplanFactory.get().get(0);
    return partitionPlan.getPartitiontype();
  }

  /**
   * This method checks for Time based partitions. 
   * @param type
   * @param partition
   * @param con
   * @throws CheckForTimeBasedPartitionsException
   */
  private void existenceCheckForTimeBasedPartitions(final Dwhtype type, final Dwhpartition partition, final Connection con) throws CheckForTimeBasedPartitionsException {

    Statement stmt = null;
    ResultSet rs = null;

    try { // existence check try

      stmt = con.createStatement();

      if (PARTITION_TYPE_UNPARTITIONED.equalsIgnoreCase(type.getType())) {

        log.finer("Partion of unpartitioned type. Status is " + partition.getStatus());

        if (partition.getStarttime() == null || partition.getEndtime() != null) {
          throw new CheckForTimeBasedPartitionsException("Partition of UNPARTITIONED type should have only starttime defined (and null endtime).");
        }

        final String datecolumn = type.getDatadatecolumn();

        if (datecolumn != null
            && (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_ACTIVE) || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_ACTIVE))) {

          log.finer("Datatime housekeeped type");

          final StringBuffer query = new StringBuffer("SELECT min(");
          query.append(datecolumn).append("),max(").append(datecolumn);
          query.append(") FROM ").append(partition.getTablename());

          // Exception if table does not exists
          rs = stmt.executeQuery(query.toString());

          if (rs.next()) {

            final Timestamp earliest = rs.getTimestamp(1);

            if (!rs.wasNull()) {
              if (earliest.getTime() < partition.getStarttime().getTime()) {
                log.warning("UNPARTITIONED table " + partition.getTablename() + " has data that is too early for this " +
                  "partition. Data starttime:"+earliest.getTime() + " but partition starttime:" +
                  partition.getStarttime().getTime());
              }
            }

            final Timestamp latest = rs.getTimestamp(2);

            if (!rs.wasNull()) {
              if (latest.getTime() > System.currentTimeMillis()) {
                log.warning("UNPARTITIONED table " + partition.getTablename() + " has data from future. Data endtime:"
                  + latest.getTime()+ " but current time:"+System.currentTimeMillis());
              }
            }

          }

        } else if (datecolumn == null
            && (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_ACTIVE) || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_ACTIVE))) {

          log.finer("Not datatime housekeeped type (datecolumn is null)");

          final StringBuffer query = new StringBuffer(SELECT_COUNT_FROM);
          query.append(partition.getTablename());

          // Exception is thrown if table does not exists
          rs = stmt.executeQuery(query.toString());

          if (rs.next()) {
            log.finest(rs.getInt(1) + " rows of data");
          }

        } else {
          throw new CheckForTimeBasedPartitionsException("Illegal partition status " + partition.getStatus() + " for UNPARTITIONED type");
        }

      } else if (PARTITION_TYPE_PARTITIONED.equalsIgnoreCase(type.getType())) {

        log.finer("Partion of partitioned type");

        if (!partition.getStatus().startsWith(PARTITION_STATUS_INSANE) && !partition.getStatus().equals(PARTITION_STATUS_NEW)
            && (partition.getStarttime() == null || partition.getEndtime() == null)) {
          throw new CheckForTimeBasedPartitionsException("Partition of PARTITIONED type should have starttime and endtime defined");
        }

        if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_NEW) || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_NEW)
            || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_MANUAL) || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_MANUAL)
            || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_MIGRATED)
            || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_MIGRATED)) {

          log.finer("Not datatime housekeeped status " + partition.getStatus());

          final StringBuffer query = new StringBuffer(SELECT_COUNT_FROM);
          query.append(partition.getTablename());

          log.finest("Existence query: \"" + query.toString() + "\"");

          // Exception is thrown if table does not exists
          rs = stmt.executeQuery(query.toString());

          if (rs.next()) {

            if (rs.getInt(1) > 0
                && (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_NEW) || partition.getStatus()
                    .equalsIgnoreCase(PARTITION_STATUS_INSANE_NEW))) {
              throw new CheckForTimeBasedPartitionsException("New partition contains data");
            }

          } // else no data

        } else {

          log.finer("Datatime housekeeped status");

          final StringBuffer query = new StringBuffer("SELECT ");
          if ("RAW".equalsIgnoreCase(type.getTablelevel())) {
            query.append("min(DATETIME_ID),max(DATETIME_ID) ");
          } else {
            query.append("min(DATE_ID),max(DATE_ID) ");
          }
          query.append("FROM ").append(partition.getTablename());

          log.finest("Existence query: \"" + query.toString() + "\"");

          // Exception if table does not exists
          rs = stmt.executeQuery(query.toString());

          if (rs.next()) {

            final Timestamp first = rs.getTimestamp(1);

            if (!rs.wasNull()) {

              final Timestamp last = rs.getTimestamp(2);

              if (!rs.wasNull()) {

                if (first.getTime() < partition.getStarttime().getTime()) {
                  log.warning("PARTITIONED table:"+ partition.getTablename()+" has data that is too early for this " +
                    "partition. Data startTime:"+first.getTime() + " but partition starttime:"+
                    partition.getStarttime().getTime());
                }

                final Date end = partition.getEndtime();

                if (end != null && last.getTime() > end.getTime()) {
                  log.warning("PARTITIONED table:"+ partition.getTablename()+" has data that is too late for this " +
                    "partition. Data endTime:" + last.getTime()+"  but partition endTime:"+end.getTime());
                }

              }

            }

          } // else rows

        }

      } else if (PARTITION_TYPE_SIMPLE.equalsIgnoreCase(type.getType())) {
        log.finer("Partion of simple type");

        if (partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_ACTIVE) || partition.getStatus().equalsIgnoreCase(PARTITION_STATUS_INSANE_ACTIVE)) {

          if (partition.getStarttime() == null || partition.getEndtime() != null) {
            throw new CheckForTimeBasedPartitionsException("Partition of SIMPLE type should have only starttime defined (and null endtime).");
          }
          
          final StringBuffer query = new StringBuffer(SELECT_COUNT_FROM);
          query.append(partition.getTablename());

          log.finest("Existence query: \"" + query.toString() + "\"");

          // Exception is thrown if table does not exists
          rs = stmt.executeQuery(query.toString());

          if (rs.next()) {

            final int count = rs.getInt(1);

            log.finest(count + " rows of data");

          } // else no data

        } else {
          throw new CheckForTimeBasedPartitionsException("Illegal partition status " + partition.getStatus() + " for SIMPLE type");
        }

      } else {
        throw new CheckForTimeBasedPartitionsException("Unknown type: " + type.getType());
      }

    } catch (SQLException e) {

      final String msg = e.getMessage();

      // Special handle for table not found
        if (msg != null && (e.getErrorCode() == TABLE_NOT_FOUND_ERROR_CODE || e.getSQLState().equals(S0002))) {

        try {
          log.warning("Partition " + partition.getTablename() + " does not exist in DWH. Deleting DWHPartition.");

          partition.deleteDB();

          log.info("Partition " + partition.getTablename() + " removed from DWHPartition.");

        } catch (Exception dex) {
          log.log(Level.WARNING, "DWHPartition removal failed", dex);
        }

      } else {
        throw new CheckForTimeBasedPartitionsException("Existence check, exceptional error", e);
      }

    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (Exception e) {
      }
      rs = null;
      try {
        if (stmt != null) {
          while (stmt.getMoreResults()) {
            stmt.getResultSet().close();
          }
          stmt.close();
        }
      } catch (Exception e) {
      }
      stmt = null;

    }
  }

  /**
   * This method checks for Volume based partitions.
   * @param partition
   * @param con
   * @throws CheckForVolumeBasedPartitionsException
   */
  private void existenceCheckForVolumeBasedPartitions(final Dwhpartition partition, final Connection con)
      throws CheckForVolumeBasedPartitionsException {

    Statement stmt = null;
    ResultSet rs = null;

    try { // existence check try

      stmt = con.createStatement();

      final StringBuffer query = new StringBuffer(SELECT_COUNT_FROM);
      query.append(partition.getTablename());

      // Exception is thrown if table does not exists
      rs = stmt.executeQuery(query.toString());

      if (rs.next()) {
        log.finest(rs.getInt(1) + " rows of data");
      }

    } catch (SQLException e) {

      final String msg = e.getMessage();

      // Special handle for table not found
      if (msg != null && (e.getErrorCode() == TABLE_NOT_FOUND_ERROR_CODE || e.getSQLState().equals(S0002))) {

        try {
          log.warning("Partition " + partition.getTablename() + " does not exist in DWH. Deleting DWHPartition.");

          partition.deleteDB();

          log.info("Partition " + partition.getTablename() + " removed from DWHPartition.");

        } catch (Exception dex) {
          log.log(Level.WARNING, "DWHPartition removal failed", dex);
        }

      } else {
        throw new CheckForVolumeBasedPartitionsException("Existence check, exceptional error", e);
      }

    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (Exception e) {
      }
      rs = null;
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (Exception e) {
      }
      stmt = null;

    }
  }

  /**
   * This method performs a column check based on a list of columns against the partition. If they are not the 
   * same an exception is thrown
   * @param columns
   * @param partition
   * @param con
   * @throws ColumnCheckException
   */
  private void columnCheck(final List<Dwhcolumn> columns, final Dwhpartition partition, final Connection con) throws ColumnCheckException {

    Statement stmt = null;
    ResultSet rs = null;

    try { // column check try

      stmt = con.createStatement();

      final String query = getSqlForColumnCheck(partition.getTablename());
      final long start = System.currentTimeMillis();
      rs = stmt.executeQuery(query);
      final long end = System.currentTimeMillis();
      performanceLog.info("columnCheck Query:"+ query+ " completed in " + (end - start) + " ms");

      int ix = 0;

      while (rs.next()) {

        final Dwhcolumn dc = columns.get(ix); // Dwhpartition ?!?!

        final String colna = rs.getString("cname").trim();
        
        if (!colna.equalsIgnoreCase(dc.getDataname())) {
          throw new ColumnCheckException("Column error " + ix + ": Name \"" + colna + "\" != \"" + dc.getDataname() + "\"");
        }

        final String sys_type = rs.getString("coltype").trim();
        final String dwh_type = dc.getDatatype();

        if (sys_type.equalsIgnoreCase("timestamp") && dwh_type.equalsIgnoreCase("datetime")) {
          // This is ok because timestamp and datetime are synonyms
        } else if (sys_type.equalsIgnoreCase("integer") && dwh_type.equalsIgnoreCase("int")) {
          // Synonyms again
        } else if (sys_type.equalsIgnoreCase("double") && dwh_type.equalsIgnoreCase("float")) {
          // Synonyms again
        } else if (sys_type.equalsIgnoreCase("real") && dwh_type.equalsIgnoreCase("float")) {
          // Synonyms again
        } else if (!sys_type.equalsIgnoreCase(dc.getDatatype())) {
          throw new ColumnCheckException("Column error " + ix + " " + colna + ": Datatype does not match");
        }

        final int datsiz = rs.getInt("length");
        final int idatsi = dc.getDatasize().intValue();

        if (idatsi > 0 && datsiz < dc.getDatasize().intValue()) {
          throw new ColumnCheckException("Column error " + ix + " " + colna + ": Datasize does not match " + datsiz + " != "
              + idatsi);
        }

        final String nullable = rs.getString("nulls").trim();

        final int inullable = dc.getNullable().intValue();

        if ((inullable == 0 && nullable.equalsIgnoreCase("Y")) || (inullable == 1 && nullable.equalsIgnoreCase("N"))) {
          log.info("Partition " + partition.getTablename() + " Column " + ix + " " + colna
              + ": Allow nulls rule changed on dwhrep definition");
        }

        ix++;

      } // For each column

      if (columns.size() > ix) {
        throw new ColumnCheckException("Columns missing from partition");
      }

    } catch (SQLException e) {
      throw new ColumnCheckException("Column check, exceptional error", e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (Exception e) {
      }
      rs = null;
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (Exception e) {
      }
      stmt = null;

    }
  }

  /**
   * SQL query should order by colno for STATS and by cname for EVENTS
   * 
   * @param tableName
   * @return
   */
  protected String getSqlForColumnCheck(final String tableName) {
    final StringBuffer query = new StringBuffer("SELECT cname, coltype, length, nulls FROM SYS.SYSCOLUMNS WHERE tname='");
    query.append(tableName).append("' AND creator='dc' ORDER BY ");

    if (tableName.startsWith(EVENT_E)) {
      // Need to remove "_" as sql "order by" does
      // not order them correctly with "_"
      query.append("(REPLACE ( cname, '_', '' ))");
    } else {
      query.append("colno");
    }
    return query.toString();
  }
  
  private void closeConn(Connection con) {
		try {
			con.close();
		} catch (Exception e) {
			log.log(Level.WARNING, "Exception", e.getMessage());
		}
	}
}
