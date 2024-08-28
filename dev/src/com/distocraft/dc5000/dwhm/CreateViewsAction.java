package com.distocraft.dc5000.dwhm;

//import com.distocraft.dc5000.common.Utils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementcounter;
import com.distocraft.dc5000.repository.dwhrep.MeasurementcounterFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementkey;
import com.distocraft.dc5000.repository.dwhrep.MeasurementkeyFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.TechPackType;




/**
 * Creates views for specified type. The view creation may fail if there are
 * locks on the database. However this action retries view recreation for
 * specified amount of times, three times by default, before giving up.
 * 
 * @author lemminkainen
 * 
 */
public class CreateViewsAction extends AbstractCreateViewsAction {

  /**
   * Creates views for specified type
   * 
   * @param dbaConnectionToDwhdb
   *          This allows a user to connection to the DWHDB database as user DBA
   * @param dcConnectiontoDwhdb
   *          This allows a user to connection to the DWHDB database as user dc
   * @param dwhrepConnectiontoRepdb
   *          This allows a user to connection to the REPDB database as user
   *          dwhrep
   * @param dwhType
   * @param loggerForClass
   *          Allows the logging of messages for this class
   * @throws Exception
   */
	
  private static final String COUNT_TABLELEVEL = "COUNT"; // Added after merge.. 
	
  public CreateViewsAction(final RockFactory dbaConnectionToDwhdb, final RockFactory dcConnectiontoDwhdb,
      final RockFactory dwhrepConnectiontoRepdb, final Dwhtype dwhType, final Logger loggerForClass,
      final TechPackType techPackType) throws Exception {
    super(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb, loggerForClass);
    ResultSet resultset = null;
    viewName = getViewName(dwhType);
    adjustViews(dwhType,techPackType);
    //Start code changes for TR HP46511
		if((dwhType.getTechpack_name().contains("DC_E_WLE")) && (dwhType.getTypename().contains("DIM_E_WLE_pmHwCePoolEul")))
		{
		Statement stmt1 =  dbadwhrock.getConnection().createStatement();
		String sql1 ="select count(*) from sys.sysview where view_def like '%dcpublic.DIM_E_WLE_pmHwCePoolEul%';";
		resultset = stmt1.executeQuery(sql1);
		if(resultset.next()){
			if(resultset.getString(1).equals("1")){
				final String sql = "drop view dcpublic.DIM_E_WLE_pmHwCePoolEul;"; 
				stmt1.executeUpdate(sql.toString());
			
			}
		   }
		}
		else
			{
			adjustPublicViews(dwhType,techPackType);
			}
		// End code changes for TR HP46511

    if ((techPackType == TechPackType.EVENTS) && (dwhType.getTablelevel().equals(Constants.RAW))) {
      adjustTimeRangeViews(dwhType, techPackType);
    }
  }
  
  /**
   * Constructor for creating new vector DC views
   * 
   * @param dbaConnectionToDwhdb
   * @param dcConnectiontoDwhdb
   * @param dwhrepConnectiontoRepdb
   * @param loggerForClass
   * @param dwhType
   * @throws Exception
   */
  public CreateViewsAction(final RockFactory dbaConnectionToDwhdb, final RockFactory dcConnectiontoDwhdb,
		  final RockFactory dwhrepConnectiontoRepdb, final Logger loggerForClass, final String vecTable, final String VecRefTable) throws Exception {
	  super(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb, loggerForClass);
	  boolean quant = false;
	  viewName = vecTable;
	  final String infoStringForLogging = "vector viewtemplate";
	  final String viewTemplate = "createvectorview.vm";
	  log.info("Creating " + infoStringForLogging + " for type " + vecTable + ":PLAIN");
	  log.finest("Using " + infoStringForLogging + viewTemplate);
	  
	  if(viewName.contains("pmRes")) {
		  quant = true;
	  }
	  final String dcVec = viewName.substring(viewName.lastIndexOf('_') + 1) + "_DCVECTOR";
	  final String val = viewName.substring(viewName.lastIndexOf('_') + 1) + "_VALUE";
	  
	  final VelocityContext velocityContext = new VelocityContext();
	  
	  velocityContext.put("baseTableName", viewName);
	  velocityContext.put("vectorRefTable", VecRefTable);
	  velocityContext.put("dcVec", dcVec);
	  velocityContext.put("val", val);
	  velocityContext.put("quant", String.valueOf(quant));
	  
	  mergeVelocityTemplateAndCreateView(viewTemplate, dbadwhrock, infoStringForLogging, velocityContext);
  }

  /**
   * Tries to create views. If view creation fails too many times exception is
   * thrown.
   */
  private void adjustViews(final Dwhtype type, final TechPackType tpType) throws Exception {
    final String infoStringForLogging = "viewtemplate";
    final String viewTemplate = type.getViewtemplate();
    createView(type, viewTemplate, dwhrock, infoStringForLogging, viewName,tpType);
  }

  /**
   * Tries to create views. If view creation fails too many times exception is
   * thrown.
   */
  private void adjustPublicViews(final Dwhtype type,final TechPackType tpType) throws Exception {
    final String infoStringForLogging = "public viewtemplate";
    final String publictemplate = type.getPublicviewtemplate();
    createView(type, publictemplate, dbadwhrock, infoStringForLogging, viewName,tpType);
  }

  /**
   * Tries to time range views. If view creation fails too many times exception is thrown.
   */
   private void adjustTimeRangeViews(final Dwhtype type, final TechPackType tpType) throws Exception {
    final String infoStringForLogging = "time range viewtemplate";
    final String timeRangeViewName = viewName + "_TIMERANGE";
    createView(type, RAW_TIME_RANGE_VIEW_TEMPLATE_FOR_DC, dwhrock, infoStringForLogging, timeRangeViewName,tpType);
  }

  private void createView(final Dwhtype type, final String template, final RockFactory dbForCreatingView,
      final String infoStringForLogging, final String vName, final TechPackType tType) throws SQLException, RockException, Exception {
    if (template == null || template.length() <= 0) {
      log.info("No " + infoStringForLogging + " defined for type " + type.getStorageid() + " ignoring...");
      return;
    }

    log.info("Creating " + infoStringForLogging + " for type " + type.getStorageid());
    log.finest("Using " + infoStringForLogging + template);

    final VelocityContext velocityContext = getVelocityContext(type, vName, template,tType);
    mergeVelocityTemplateAndCreateView(template, dbForCreatingView, infoStringForLogging, velocityContext);
  }

  private VelocityContext getVelocityContext(final Dwhtype type, final String vName, final String template,final TechPackType techPackType ) throws SQLException, RockException {
    final Vector<Dwhcolumn> columns = getDwhColumns(type);
    final Vector<Dwhpartition> partitions = getDwhPartitions(type);
    final VelocityContext velocityContext = new VelocityContext();
    
    velocityContext.put("techPackType",techPackType.name());
    velocityContext.put("baseTableName", vName);
    velocityContext.put("type", type);
    velocityContext.put("partitions", partitions);
    velocityContext.put("columns", columns);
    
    //System.out.println("techPackType = "+ techPackType);
    //System.out.println("baseTableName-"+vName);
    //System.out.println("template-"+template);
    /**
     * 
     * Added newly for merge
     * Only for STATS 
     */
    //Get template name here as this is reqd only for normal view
    //Changing here
    
  //Code changes for TR HQ22916 ,for creating the CUSTOM views, CUSTOM is added in below condition. 
    
 if((techPackType == TechPackType.STATS || techPackType == TechPackType.CUSTOM) && (template.equalsIgnoreCase(CREATE_VIEW_TEMPLATE))){
    	//System.out.println("getViewtemplate()):"+template);
    	final boolean hasCount = doesHaveCountTableLevel(type);
		Vector measCounters = new Vector(); 
		Vector measKeys = new Vector();
		// do this only for count table level
		if(COUNT_TABLELEVEL.equalsIgnoreCase(type.getTablelevel())) {
			measCounters = getMeasurementCounters(type);
			measKeys = getMeasurementKeys(type);

		}
		velocityContext.put("hasCount", String.valueOf(hasCount));
		velocityContext.put("measCounters", measCounters);
		velocityContext.put("measKeys", measKeys);
    }
    
    
    
    return velocityContext;
  }

  private Vector<Dwhpartition> getDwhPartitions(final Dwhtype type) throws SQLException, RockException {
    final Vector<Dwhpartition> partitions = getAllPartitions(type.getStorageid());
    final Iterator<Dwhpartition> it = partitions.iterator();

    while (it.hasNext()) {
      final Dwhpartition dwhPartition = it.next();

      if (dwhPartition.getStatus().startsWith("INSANE")) {
        it.remove();
        log.warning("Ignoring insane partition " + dwhPartition.getTablename());
      }
    }

    if (partitions.size() <= 0) {
      log.warning("No (sane) partitions for type " + type.getTypename() + " (" + type.getTablelevel()
          + ") view will be dropped only");
    } else {
      log.fine(partitions.size() + " sane partitions found");
    }
    return partitions;
  }

  /**
   * View name is got from the base table name stored in DwhType. The RAW view
   * should be a combination of all the RAW and RAW_LEV2 tables. No view should
   * exist for RAW_LEV2 tables. E.g. If the tables were,
   * EVENT_E_TEST_RAW_LEV2_01, EVENT_E_TEST_RAW_LEV2_02,
   * EVENT_E_TEST_RAW_LEV2_02, EVENT_E_TEST_RAW_01, EVENT_E_TEST_RAW_02,
   * EVENT_E_TEST_RAW_03. Then the viewName for these tables would be
   * EVENT_E_TEST_RAW. Therefore, if the baseTableName ends with RAW_LEV2, the
   * name of the view should end with RAW instead
   * 
   * @param type
   * @return
   */
  private String getViewName(final Dwhtype type) {
    String nameOfView = type.getBasetablename();
    //System.out.println("nameOfView "+nameOfView);
    if (nameOfView.endsWith(Constants.RAW_LEV2)) {
      nameOfView = nameOfView.replace(Constants.RAW_LEV2, Constants.RAW);
    }
    return nameOfView;
  }

  /**
   * Gets all the partitions for this storage Id so that a view can be created
   * over all these partitions.
   * 
   * @param storageId
   * @return
   * @throws SQLException
   * @throws RockException
   */
  private Vector<Dwhpartition> getAllPartitions(final String storageId) throws SQLException, RockException {
    final Vector<Dwhpartition> partitions = getPartitions(storageId);

    if (storageId.endsWith(Constants.RAW)) {
      // If the partitions end with RAW, then we also want to get all the
      // partitions for RAW_LEV2 so that the view being created will have
      // the RAW and RAW_LEV2 partitions
      partitions.addAll(getPartitions(storageId + Constants.LEV2));
    } else if (storageId.endsWith(Constants.RAW_LEV2)) {
      // If the partitions end with RAW_LEV2, then we also want to get all the
      // partitions for RAW so that the view includes but the RAW and RAW_LEV2
      // partitions.
      partitions.addAll(getPartitions(storageId.replace(Constants.RAW_LEV2, Constants.RAW)));
    }
    return partitions;
  }
  
	private boolean doesHaveCountTableLevel(final Dwhtype type) {
		boolean returnValue;
		
		// set DWHType condition to check does the type have COUNT table level
		final Dwhtype dwhTypeCond = new Dwhtype(reprock);
		dwhTypeCond.setTypename(type.getTypename());
		dwhTypeCond.setTablelevel(COUNT_TABLELEVEL);
		
		DwhtypeFactory dwhTypeResult = null;
		
		try {
			// get the result by DWHType condition
			dwhTypeResult = new DwhtypeFactory(reprock, dwhTypeCond);
		} catch (Exception e) {
			log.warning("Error occured when querying DWHType with typename and COUNT tablelevel. " + e.toString());
		}

		if (null == dwhTypeResult) {
			// when error happened querying the DWHType information 
			returnValue = false;
		} else if (dwhTypeResult.get().isEmpty()) {
			// when measurement type does not have COUNT table level
			returnValue = false;
		} else {
			// when measurement type does have COUNT table level
			returnValue = true;
		}
		
		return returnValue;
	}  
	
  private Vector getMeasurementCounters(final Dwhtype type) {
		/*
		With this below query the below rockfactory logic could be replaced
		
		select mcou.* 
		from dwhrep.DWHType type 
		join dwhrep.DWHTechpacks tp on (type.techpack_name = tp.techpack_name)
		join dwhrep.MeasurementType mtyp on (tp.versionid = mtyp.versionid and type.typename = mtyp.typename)
		join dwhrep.MeasurementCounter mcou on (mtyp.typeid = mcou.typeid)
		where type.typename = '?' and type.tablelevel = '?'
		*/
		
    final Vector result = new Vector();
		final Measurementtype measurementType = getMeasurementType(type);

		if (null != measurementType) {
			// set condition 
			final Measurementcounter counterCond = new Measurementcounter(reprock);
			counterCond.setTypeid(measurementType.getTypeid());
			
			try {
				// get the results ordered by colnumber column
				final MeasurementcounterFactory counterResult = new MeasurementcounterFactory(reprock, counterCond, " ORDER BY COLNUMBER ");
				
        final Vector counters = counterResult.get();
        final Iterator iter = counters.iterator();
				while(iter.hasNext()){
					final Measurementcounter mc = (Measurementcounter) iter.next();
					result.add(mc);
				}
			} catch (Exception e) {
				log.warning("Error occured when querying MeasurementKey with typeid. " + e.toString());
			}
		}
		return result;	
	}

  private Vector getMeasurementKeys(final Dwhtype type) {
		/*
		With this below query the below Rockfactory logic could be replaced
		
		select mkey.* 
		from dwhrep.DWHType type 
		join dwhrep.DWHTechpacks tp on (type.techpack_name = tp.techpack_name)
		join dwhrep.MeasurementType mtyp on (tp.versionid = mtyp.versionid and type.typename = mtyp.typename)
		join dwhrep.MeasurementKey mkey on (mtyp.typeid = mkey.typeid)
		where type.typename = '?' and type.tablelevel = '?'
		*/
		
    final Vector result = new Vector();
		final Measurementtype measurementType = getMeasurementType(type);

		if (null != measurementType) {
			// set condition 
			final Measurementkey keyCond = new Measurementkey(reprock);
			keyCond.setTypeid(measurementType.getTypeid());

			try {
				// get the results ordered by colnumber column
				final MeasurementkeyFactory keyResult = new MeasurementkeyFactory(reprock, keyCond, " ORDER BY COLNUMBER ");
				
        final Vector keys = keyResult.get();
        final Iterator iter = keys.iterator();
				while(iter.hasNext()){
          final Measurementkey mk = (Measurementkey) iter.next();
					result.add(mk);
				}
			} catch (Exception e) {
				log.warning("Error occured when querying MeasurementKey with typeid. " + e.toString());
			}
		}
		return result;	
	}

	private Measurementtype getMeasurementType(final Dwhtype type) {
		Measurementtype returnObject = null;
		
		// set condition 
    final Dwhtechpacks dwhTPCond = new Dwhtechpacks(reprock);
		dwhTPCond.setTechpack_name(type.getTechpack_name());

		try {
			// get the result
      final DwhtechpacksFactory dwhTPResult = new DwhtechpacksFactory(reprock, dwhTPCond);
			
      final Vector dwhTechpacks = dwhTPResult.get();
	
			Dwhtechpacks tpResult = null;
      final Iterator it = dwhTechpacks.iterator();
			if (it.hasNext()) {
				tpResult = (Dwhtechpacks) it.next();
			}
			
			if (null != tpResult) {
				// set condition 
        final Measurementtype measTypeCond = new Measurementtype(reprock);
				measTypeCond.setVersionid(tpResult.getVersionid());
				measTypeCond.setTypename(type.getTypename());
	
				try {
					// get the result
          final MeasurementtypeFactory measTypeResult = new MeasurementtypeFactory(reprock, measTypeCond);
					
          final Vector measurementTypes = measTypeResult.get();
		
					final Iterator ite = measurementTypes.iterator();
					if (ite.hasNext()) {
						returnObject = (Measurementtype) ite.next();
					}
				} catch (Exception e) {
					log.warning("Error occured when querying MeasurementType with versionid and typename. " + e.toString());
				}
			}
		} catch (Exception e) {
			log.warning("Error occured when querying DWHTechpacks with techpack_name. " + e.toString());
		}
		
		return returnObject;
	}
	
}
