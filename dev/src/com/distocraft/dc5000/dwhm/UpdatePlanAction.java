package com.distocraft.dc5000.dwhm;

import java.io.File;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.repository.dwhrep.Configuration;
import com.distocraft.dc5000.repository.dwhrep.ConfigurationFactory;
import com.distocraft.dc5000.repository.dwhrep.Partitionplan;
import com.distocraft.dc5000.repository.dwhrep.PartitionplanFactory;
import com.ericsson.eniq.common.INIGet;

/**
 * This action calculates and updates all of the defaultpartitionsize values in
 * table PartitionPlan. Also updates value of maxstoragetime. Parameters are
 * loaded from table Configuration and from file ${CONF_DIR}/niq.ini.
 * 
 * @author ejannbe
 */
public class UpdatePlanAction extends TransferActionBase {

  private Logger log;

  private RockFactory dwhrepRock;

  public UpdatePlanAction(RockFactory reprock, Logger clog) {
    this.log = Logger.getLogger(clog.getName() + ".DWHMUpdatePlan");
    this.dwhrepRock = reprock;
  }

  public void execute() throws EngineException {

    Partitionplan wherePartitionPlan = new Partitionplan(this.dwhrepRock);

    PartitionplanFactory partitionPlanFactory = null;

    try {
      partitionPlanFactory = new PartitionplanFactory(this.dwhrepRock, wherePartitionPlan);
    } catch (Exception e) {
      throw new EngineException("Initializing PartitionplanFactory failed.", new String[] { new String("") }, e, this,
          this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
    }

    Vector partitionPlans = partitionPlanFactory.get();
    Iterator partitionPlansIterator = partitionPlans.iterator();

    Float numOfCells = getNumOfCellsValue();

    if (numOfCells == null) {
      throw new EngineException("No value defined for numOfCells Aborting execution of UpdatePlanAction.",
          new String[] { new String("") }, new Exception(""), this, this.getClass().getName(),
          EngineConstants.ERR_TYPE_EXECUTION);
    }

    this.log.finest("numOfCells = " + numOfCells.floatValue());

    // Iterate through all PartitionPlans.
    while (partitionPlansIterator.hasNext()) {
      Partitionplan currentPartitionPlan = (Partitionplan) partitionPlansIterator.next();
      String partitionPlanName = currentPartitionPlan.getPartitionplan();
      Float storageTime = new Float(currentPartitionPlan.getDefaultstoragetime().floatValue());

      Float rowsPerTable = getConfigurationValue("dwhm." + partitionPlanName + ".rowsPerTable");

      if (rowsPerTable == null || rowsPerTable.floatValue() == 0) {
        throw new EngineException("No value defined for dwhm." + partitionPlanName
            + ".rowsPerTable or value is 0. Aborting execution of UpdatePlanAction.", new String[] { new String("") },
            new Exception(""), this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
      }

      this.log.finest("dwhm." + partitionPlanName + ".rowsPerTable = " + rowsPerTable.floatValue());

      // Load up all the parameters to use with calculations.
      Float rowsPerCellPerROP = getConfigurationValue("dwhm." + partitionPlanName + ".rowsPerCellPerROP");

      if (rowsPerCellPerROP == null) {
        throw new EngineException("No value defined for dwhm." + partitionPlanName
            + ".rowsPerCellPerROP. Aborting execution of UpdatePlanAction.", new String[] { new String("") },
            new Exception(""), this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
      }

      Float defaultROP = getConfigurationValue("dwhm." + partitionPlanName + ".defaultROP");

      if (defaultROP == null || defaultROP.floatValue() == 0) {
        throw new EngineException(
            "No value defined for defaultROP or value is 0. Aborting execution of UpdatePlanAction.",
            new String[] { new String("") }, new Exception(""), this, this.getClass().getName(),
            EngineConstants.ERR_TYPE_EXECUTION);
      }

      Float maxTables = getConfigurationValue("dwhm." + partitionPlanName + ".maxTables");

      if (maxTables == null) {
        throw new EngineException("No value defined for maxTables. Aborting execution of UpdatePlanAction.",
            new String[] { new String("") }, new Exception(""), this, this.getClass().getName(),
            EngineConstants.ERR_TYPE_EXECUTION);
      }

      Float minTables = getConfigurationValue("dwhm." + partitionPlanName + ".minTables");

      if (minTables == null) {
        throw new EngineException("No value defined for minTables. Aborting execution of UpdatePlanAction.",
            new String[] { new String("") }, new Exception(""), this, this.getClass().getName(),
            EngineConstants.ERR_TYPE_EXECUTION);
      }

      this.log.finest("dwhm." + partitionPlanName + ".rowsPerCellPerROP = " + rowsPerCellPerROP.floatValue());
      this.log.finest("dwhm." + partitionPlanName + ".defaultROP = " + defaultROP.floatValue());
      this.log.finest("dwhm." + partitionPlanName + ".maxTables = " + maxTables.floatValue());
      this.log.finest("dwhm." + partitionPlanName + ".minTables = " + minTables.floatValue());

      Float numOfPartitions = new Float((1440 * storageTime.floatValue() * rowsPerCellPerROP.floatValue() * numOfCells
          .floatValue())
          / (defaultROP.floatValue() * rowsPerTable.floatValue()));

      if (numOfPartitions.floatValue() > maxTables.floatValue()) {
        this.log.fine("Calculated number of partitions " + numOfPartitions.toString() + " is more than maxTables "
            + maxTables.toString() + ". Using maxTables value " + maxTables.toString() + ".");
        numOfPartitions = maxTables;
      }

      if (numOfPartitions.floatValue() < minTables.floatValue()) {
        this.log.fine("Calculated number of partitions " + numOfPartitions.toString() + " is less than minTables "
            + minTables.toString() + ". Using minTables value " + minTables.toString() + ".");
        numOfPartitions = minTables;
      }

      Float exactNewPartitionSize = new Float((24 * storageTime.floatValue()) / numOfPartitions.floatValue());

      this.log.info("New exact calculated partitionsize for partition " + currentPartitionPlan.getPartitionplan()
          + " is " + exactNewPartitionSize.toString() + " hours.");

      Double roundedNewPartitionSize = new Double(Math.ceil(exactNewPartitionSize.doubleValue()));
      Long newPartitionSize = new Long(roundedNewPartitionSize.longValue());
      this.log.info("Rounded partitionsize for partition " + currentPartitionPlan.getPartitionplan() + " is "
          + newPartitionSize.toString() + " hours.");

      // Make sure the partitionsize is dividible with 24.
      if (newPartitionSize.longValue() % 24 != 0) {

        while (newPartitionSize.longValue() % 24 != 0) {
          newPartitionSize = new Long(newPartitionSize.longValue() + 1);
        }

        this.log.info("Increased partitionsize to accomodate 24 hour periods for partition "
            + currentPartitionPlan.getPartitionplan() + ". Final value for partitionsize is "
            + newPartitionSize.toString() + " hours.");
      }

      currentPartitionPlan.setDefaultpartitionsize(new Long(newPartitionSize.longValue()));

      Long maxStorageTime = new Long(newPartitionSize.longValue() * maxTables.longValue() / 24);

      this.log.finest("Calculated maxstoragetime is " + maxStorageTime.toString());

      if (currentPartitionPlan.getMaxstoragetime() == null) {
        currentPartitionPlan.setMaxstoragetime(new Long(-1));
        this.log
            .fine("Previous maxstoragetime was undefined. Using value -1 for comparing to calculated maxstoragetime.");
      }

      if (maxStorageTime.floatValue() != currentPartitionPlan.getMaxstoragetime().floatValue()) {
        this.log.finest("PartitionPlan " + currentPartitionPlan.getPartitionplan() + " maxstoragetime updated from "
            + currentPartitionPlan.getMaxstoragetime().toString() + " to " + maxStorageTime.toString() + ".");
        currentPartitionPlan.setMaxstoragetime(new Long(maxStorageTime.longValue()));
      }

      try {
        currentPartitionPlan.updateDB();
      } catch (Exception e) {
        log.log(Level.SEVERE, "Updating values of partition plan " + currentPartitionPlan.getPartitionplan()
            + " failed.", e);
      }
    }
    log.info("Successfully finished DWHMUpdatePlanAction.");
  }

  /**
   * This function returns the value of the parameter from table Configuration.
   * 
   * @param paramName
   *          Name of the parameter.
   * @return Returns the value of the paramater specified by the parameter
   *         paramName. In case of error returns null.
   */
  private Float getConfigurationValue(String paramName) {

    try {
      Configuration whereConfiguration = new Configuration(this.dwhrepRock);
      whereConfiguration.setParamname(paramName);

      ConfigurationFactory configurationFactory = new ConfigurationFactory(this.dwhrepRock, whereConfiguration);
      Vector configurations = configurationFactory.get();

      if (configurations.size() == 0) {
        this.log.warning("Parameter named " + paramName + " not found from Configuration table.");
        return null;
      } else {
        Configuration targetConfiguration = (Configuration) configurations.get(0);
        return new Float(targetConfiguration.getParamvalue());
      }

    } catch (Exception e) {
      this.log.log(Level.SEVERE, "Getting values from table Configuration failed.", e);
      return null;
    }
  }

  /**
   * This function loads the value of numOfCellsValue used in calculating
   * partitionsize. The data is found in file ${CONF_DIR}/niq.ini and the
   * parameter name is ManagedNodes.
   * 
   * @return Returns the value for numOfCellsValue. In case of error returns
   *         null.
   */
  private Float getNumOfCellsValue() {

    try {
      INIGet iniGet = new INIGet();
      iniGet.setFile(System.getProperty("CONF_DIR") + File.separator + "niq.ini");
      iniGet.setSection("ENIQ_NET_INFO");

      iniGet.setParameter("ManagedNodesCORE");
      iniGet.execute(this.log);
      Float numOfCellsCore = new Float(iniGet.getParameterValue());

      iniGet.setParameter("ManagedNodesGRAN");
      iniGet.execute(this.log);
      Float numOfCellsGran = new Float(iniGet.getParameterValue());

      iniGet.setParameter("ManagedNodesWRAN");
      iniGet.execute(this.log);
      Float numOfCellsWran = new Float(iniGet.getParameterValue());

      Float TotalNumOfCells = numOfCellsCore + numOfCellsGran + numOfCellsWran;

      if (TotalNumOfCells == null) {
        this.log.warning("Getting parameter NumOfCellsValue failed. INIGet returned null.");
        return null;
      } else {
        this.log.finest("getNumOfCellsValue returned " + TotalNumOfCells);
        return TotalNumOfCells;
      }
    } catch (NumberFormatException nfe) {
      this.log.severe("Failed to convert ManagedNodes parameter to Float.");
      return null;
    } catch (Exception e) {
      this.log.severe("Exception occurred while getting ManagedNodes parameters.");
      return null;
    }
  }
}
