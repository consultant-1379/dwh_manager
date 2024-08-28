package com.distocraft.dc5000.dwhm;

import java.io.File;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.common.ServicenamesHelperFactory;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.repository.cache.GroupTypeDef;
import com.distocraft.dc5000.repository.cache.GroupTypeKeyDef;
import com.distocraft.dc5000.repository.cache.GroupTypesCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementcolumn;
import com.distocraft.dc5000.repository.dwhrep.MeasurementcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementcounter;
import com.distocraft.dc5000.repository.dwhrep.MeasurementcounterFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementobjbhsupport;
import com.distocraft.dc5000.repository.dwhrep.MeasurementobjbhsupportFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtableFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementvector;
import com.distocraft.dc5000.repository.dwhrep.MeasurementvectorFactory;
import com.distocraft.dc5000.repository.dwhrep.Referencecolumn;
import com.distocraft.dc5000.repository.dwhrep.ReferencecolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Referencetable;
import com.distocraft.dc5000.repository.dwhrep.ReferencetableFactory;
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;
import com.distocraft.dc5000.repository.dwhrep.Typeactivation;
import com.distocraft.dc5000.repository.dwhrep.TypeactivationFactory;
import com.distocraft.dc5000.repository.dwhrep.Versioning;
import com.distocraft.dc5000.repository.dwhrep.VersioningFactory;
import com.ericsson.eniq.common.INIGet;
import com.ericsson.eniq.common.TechPackType;
import com.ericsson.eniq.common.Utils;
import com.ericsson.eniq.licensing.cache.DefaultLicensingCache;
import com.ericsson.eniq.repository.ETLCServerProperties;

/**
 * VersionUpdateAction is responsible to alter partitions after verifying Versions and Types. 
 * 
 * @author xtouoos
 *
 */
@SuppressWarnings({ "PMD.ConfusingTernary" })
// eeipca : No changing the order of statements...
public class VersionUpdateAction {
	// maxColsPerStatement replaces a previously hard coded value of 100 -
	// default to this value where the property is not set in static.properties
	private final int MAX_COLS_PER_STATEMENT = Integer.parseInt(StaticProperties.getProperty(
					"VersionUpdateAction.maxColsPerStatement", "100"));

	private static final Long DEFAULT_PARTITION_COUNT = 0L;

	private static final String CREATEEVENTSCALCVIEW_VM = "createeventscalcview.vm";

	private static final String CREATEPUBLICEVENTSCALCVIEW_VM = "createpubliceventscalcview.vm";

	private static final String DC = "dc";

	private static final String DATE_ID = "DATE_ID";

	private static final String PARTITIONED = "PARTITIONED";

	private static final String CREATEPUBLICVIEW_VM = "createpublicview.vm";

	private static final String CREATEPARTITION_VM = "createpartition.vm";

	private static final String CREATEVIEW_VM = "createview.vm";

	final private Logger log;

	final private Logger sqlLog;

	final private Logger performanceLog;

	final private RockFactory dwhRepRock;

	private final RockFactory dcRock;

	private RockFactory etlRepRock = null;

	private final String techpackname;

	private String oldVersionID = null;

	private String newVersionID = null;

	private boolean forceCheck = false;

	private boolean ebs = false;

	private static final String ACTIVE = "ACTIVE";

	private static final String DWHTYPE_STATUS_ENABLED = "ENABLED";
	
	private static final String ENABLED = DWHTYPE_STATUS_ENABLED;

	private static final String OBSOLETE = "OBSOLETE";
	
	private static final String NEWVECTOR = "NEWVECTOR";

	protected String storageIDMask = ".+?:.+?:(.+)";
	
	private static final String DWHTYPE_TABLE_LEVEL_PLAIN = "PLAIN";
	
	private static final String DWHTYPE_OWNER_DC = "DC";
	
	private static final String DWHTYPE_TYPE_SIMPLE = "SIMPLE";
	
	private static final String CREATEPUBLICVIEWFORSIMPLE_VM = "createpublicviewforsimple.vm";

	private final String dcUser;

	// New field for Node Version Update feature
	private String measurementType;

	private TechPackType techPackType = TechPackType.UNKNOWN;

	// conf dir property name, defaults to CONF_DIR_DEFAULT
	private static final String CONF_DIR = "CONF_DIR";
	
	//Default value for ${CONF_DIR}
	private static final String CONF_DIR_DEFAULT = "/eniq/sw/conf";

	private static final String DWH_INI_FILENAME = "dwh.ini";

	private static final String NIQ_INI_FILENAME = "niq.ini";

	private RockFactory dbConnection = null;
	
	//For keeping new code dormant for old techpacks
	private boolean newVectorFlag = false;

	/**
	 * Constructor.
	 * 
	 * @param dwhreprock
	 * @param dwhrock
	 * @param tpName
	 * @param clog
	 * @throws Exception 
	 */
	public VersionUpdateAction(final RockFactory dwhreprock, final RockFactory dwhrock, 
			final String tpName, final Logger clog) throws Exception {
		this.dwhRepRock = dwhreprock;
		this.dcRock = dwhrock;
		this.techpackname = tpName;

		log = Logger.getLogger(clog.getName() + ".dwhm.VersionUpdate");
		sqlLog = Logger.getLogger("sql" + log.getName().substring(log.getName().indexOf(".")));
		final int fix = log.getName().indexOf(".");
		final int eix = log.getName().indexOf(".", fix + 1);
		performanceLog = Logger.getLogger("performance."
						+ log.getName().substring(fix + 1, eix) + ".dwhm.VersionUpdate");
		this.etlRepRock = connectEtlrep();
		this.dcUser = getDCUser();
	}

	// This is only used for Test
	@Deprecated()
	public VersionUpdateAction() {
		this.dwhRepRock = null;
		this.dcRock = null;
		this.techpackname = null;

		log = null;
		sqlLog = null;
		performanceLog = Logger.getLogger("VersionUpdate.performanceLog");
		this.dcUser = null;
	}

	/**
	 * @param typeName
	 *            Refers to the measuretype for which versionupdate needs to be
	 *            run
	 * @throws Exception
	 *             New method for Node Version Update. Filters out the execution
	 *             of version update only to given measurement type.
	 */
	public void execute(final String typeName) throws Exception {
		measurementType = typeName;
		this.forceCheck = true;
		execute();
	}

	/**
	 * @param ebs
	 * @throws Exception 
	 */
	public void execute(final boolean ebs) throws Exception {
		this.forceCheck = ebs;
		this.ebs = ebs;
		execute();
	}

	/**
	 * @throws Exception .
	 */
	public void execute() throws Exception {
		log.info("Executing VersionUpdateAction for techpack " + techpackname);
		
		checkVectorFlag();
		try {
			if (verifyVersions()) {
				final long start = System.currentTimeMillis();
				long pre;

				try {
					// Update and validate DWHType
					verifyTypes();

					pre = System.currentTimeMillis();
					performanceLog.info("Type verification completed in " + (pre - start) + " ms");

					// Update and validate (ReferenceColumn + MeasurementColumn) ->DWHColumn

					final Map<String, Vector<Dwhcolumn>> activeDWHColumns = getActiveDWHColumns();
					final Map<String, Vector<Measurementcolumn>> measurementMap = getMeasurementColumns();
					final Map<String, Vector<Referencecolumn>> referenceMap = getReferenceColumns();
					if(newVectorFlag) {
						referenceMap.putAll(getVectorCounterColumnsNew());
					}
					else {
						referenceMap.putAll(getVectorCounterColumns());
					}
					referenceMap.putAll(getBHColumns());
					
					if (TechPackType.EVENTS == techPackType) {
						referenceMap.putAll(getETLDubCheckColumns());
					}

					verifyColumns(activeDWHColumns, measurementMap, referenceMap);

					performanceLog.info("Column verification completed in "
							+ (System.currentTimeMillis() - pre) + " ms");
					log.finer("Column verification completed in "
							+ (System.currentTimeMillis() - pre) + " ms");

					pre = System.currentTimeMillis();
				} catch (Exception e) {
					throw (e);
				}

				// Check & modify existing partitions sys.SYSCOLUMNS vs DWHColumn
				alterPartitions();

				performanceLog.info("Partition alteration done in "
						+ (System.currentTimeMillis() - pre) + " ms");

				log.finest("Updating DWHTechPack");

				final Dwhtechpacks dtp_cond = new Dwhtechpacks(dwhRepRock);
				dtp_cond.setTechpack_name(techpackname);
				final DwhtechpacksFactory dtp_fact = new DwhtechpacksFactory(dwhRepRock, dtp_cond);

				final Dwhtechpacks dtp = dtp_fact.get().get(0);
				dtp.setVersionid(newVersionID);
				dtp.updateDB();

				log.info("Successfully performed. New version is " + newVersionID);
				performanceLog.info("Version update performed in "
						+ (System.currentTimeMillis() - start) + " ms");
			}
		} finally {
			if (etlRepRock != null) {
				try {
					etlRepRock.getConnection().close();
				} catch (SQLException e) {
				}
			}
		}
		log.info("End of VersionUpdateAction.");
	}

	/**
	 * Returns true if execution is needed false otherwise. Creates a
	 * DWHTechPacks object if does not exist.
	 * 
	 * @return ok
	 * @throws Exception .
	 */
	private boolean verifyVersions() throws Exception {
		final Tpactivation tpa_cond = new Tpactivation(dwhRepRock);
		tpa_cond.setTechpack_name(techpackname);
		final TpactivationFactory tpa_fact = new TpactivationFactory(dwhRepRock, tpa_cond);
		final Vector<Tpactivation> tpas = tpa_fact.get();

		if (tpas.size() < 1) {
			throw new Exception("Techpack " + techpackname + " is not active (not in TPActivation)");
		}

		final Tpactivation tpa = tpas.get(0);
		if (!"ACTIVE".equalsIgnoreCase(tpa.getStatus())) {
			throw new Exception("Techpack " + techpackname + " is not ACTIVE");
		}

		newVersionID = tpa.getVersionid();
		log.fine("Activated TP version is " + newVersionID);

		// Ensuring that version can be found from dwhrep
		final Versioning v_cond = new Versioning(dwhRepRock);
		v_cond.setVersionid(tpa.getVersionid());
		final VersioningFactory v_fact = new VersioningFactory(dwhRepRock, v_cond);
		final Vector<Versioning> vs = v_fact.get();

		if (vs.size() != 1) {
			throw new Exception("Activated techpack " + tpa.getVersionid()
					+ " not found");
		}
		
		final Versioning new_v = vs.get(0);
		techPackType = Utils.getTechPackType(new_v);

		final Dwhtechpacks dtp_cond = new Dwhtechpacks(dwhRepRock);
		dtp_cond.setTechpack_name(techpackname);
		final DwhtechpacksFactory dtp_fact = new DwhtechpacksFactory(dwhRepRock, dtp_cond);
		final Vector<Dwhtechpacks> dtps = dtp_fact.get();

		if (dtps.size() > 1) { 
			throw new Exception("PANIC: Multiple techpacks in DWHTechPacks with name " + techpackname);
		} else if (dtps.size() < 1) {
			// First run. We create it because others have foreign key relation
			// with Dwhtechpacks table.

			final Dwhtechpacks dwhtechpack = new Dwhtechpacks(dwhRepRock);
			dwhtechpack.setTechpack_name(new_v.getTechpack_name());
			// N/A because not ready version at this stage of time
			dwhtechpack.setVersionid("N/A");
			dwhtechpack.setCreationdate(new Timestamp(System.currentTimeMillis()));
			dwhtechpack.saveDB();

			log.fine("A new DWHTechPacks object created");
		} else {
			final Dwhtechpacks dwhtechpack = dtps.get(0);
			oldVersionID = dwhtechpack.getVersionid();
		}

		if (oldVersionID == null) {
			log.info("First execution for TP creating " + newVersionID);
			return true;
		} else if (!oldVersionID.equals(newVersionID)) {
			log.info("Updating " + oldVersionID + " -> " + newVersionID);
			return true;
		} else if (forceCheck) {
			log.info("Force checking TP " + techpackname);
			return true;
		} else {
			log.info("Execution is not needed");
			// return false;
			log.info("Still i will exexute it. ");
			return true;
		}
	}

	/**
	 * Creates missing types and marks obsolete types
	 * 
	 * @throws Exception .
	 */
	private void verifyTypes() throws Exception {
		//For new vector implementation
		boolean isVector = false;
		Vector<Dwhtype> oldVectors = new Vector<>();
		Set<String> vectorDimTables = new HashSet<String>();
		if(newVectorFlag) {
			oldVectors = OldVectorTypes();
			log.info("Deprecated vector types: " + oldVectors.size());
		}
		
		final Dwhtype dt_cond = new Dwhtype(dwhRepRock);
		dt_cond.setTechpack_name(techpackname);
		final DwhtypeFactory dt_fact = new DwhtypeFactory(dwhRepRock, dt_cond);

		Vector<Dwhtype> existing_types = dt_fact.get();
		log.info("Before: " + existing_types.size());
		if (this.measurementType != null) {
			// Implies the constructor used was for Generic SIU. Filter the meas types.
			existing_types = filterExistingTypes(existing_types);
		}
		log.info("After: " + existing_types.size());
		
		if(newVectorFlag) {
			// Need to filter out old vector dwhtypes
			log.info("Before new Vector implementation: " + existing_types.size());
			if (!oldVectors.isEmpty()) {
				existing_types = filterOldVector(existing_types);
			}
			log.info("After new Vector implementation: " + existing_types.size());
		}
		
		final Vector<Dwhtype> deleted_types = new Vector<Dwhtype>();

		log.fine(existing_types.size() + " types already exists");

		final Measurementtype mt_cond = new Measurementtype(dwhRepRock);
		mt_cond.setVersionid(newVersionID);
		// For Node Version Update - Filter out the exact meas type.
		filterMeasType(mt_cond);
		final MeasurementtypeFactory mt_fact = new MeasurementtypeFactory(dwhRepRock, mt_cond);
		final Vector<Measurementtype> mtypes = mt_fact.get();
		final Vector<Dwhtype> created_types = new Vector<Dwhtype>();

		log.fine(mtypes.size() + " measurement types defined");

		final Referencetable rt_cond = new Referencetable(dwhRepRock);
		rt_cond.setVersionid(newVersionID);
		// For Node Version Update - Filter out the exact meas type.
		filterMeasType(rt_cond);
		final ReferencetableFactory rt_fact = new ReferencetableFactory(dwhRepRock, rt_cond);
		final Vector<Referencetable> rtypes = rt_fact.get();
		final Vector<Referencetable> rtypes_dc = new Vector<Referencetable>();
		final Vector<String> rtypeNames = new Vector<String>();

		// collect all reference type names to a list
		for (Referencetable rt : rtypes) {
			rtypeNames.add(rt.getTypename());
		}

		log.fine(rtypes.size() + " reference types defined");

		for (Referencetable rt : rtypes) {
			// create current_dc tables
			// eeoidiv 20091203 : Timed Dynamic topology handling in ENIQ, WI
			// 6.1.2, (284/159 41-FCP 103 8147) Improved WRAN Topology in ENIQ
			// if reference tables update policy is dynamic(2) or timed
			// dynamic(3)
			// and the table does not already contain such a row
			// 20110830 EANGUAN :: Adding comparison for policy number 4 for
			// History Dynamic (SON)
			if ((rt.getUpdate_policy() == 2) || (rt.getUpdate_policy() == 3)
					|| (rt.getUpdate_policy() == 4)) {

				// clone the existing row
				final Referencetable tmprt = (Referencetable) rt.clone();

				// add _CURRENT_DC to the name
				tmprt.setTypename(rt.getTypename() + "_CURRENT_DC");

				// update policy is static (0)
				tmprt.setUpdate_policy(0l);

				// data format support is off
				tmprt.setDataformatsupport(0);

				if (!rtypeNames.contains(tmprt.getTypename())) {
					rtypes_dc.add(tmprt);
					rtypeNames.add(tmprt.getTypename());
				}

				// eeoidiv,20110926:Automatically create _CALC table for update
				// policy=4=HistoryDynamic (like _CURRENT_DC).
				if (rt.getUpdate_policy() == 4) {
					// clone the existing row
					final Referencetable tmprtCalc = (Referencetable) rt.clone();
					// add _CALC to the name
					tmprtCalc.setTypename(rt.getTypename() + "_CALC");
					// update policy is static (0)
					tmprtCalc.setUpdate_policy(0l);
					// data format support is off
					tmprtCalc.setDataformatsupport(0);
					if (!rtypeNames.contains(tmprtCalc.getTypename())) {
						rtypes_dc.add(tmprtCalc);
						rtypeNames.add(tmprtCalc.getTypename());
					}
					final Referencetable tmprtHist = (Referencetable) rt.clone();
					// update policy is static (0)
					tmprtHist.setUpdate_policy(0l);
					// data format support is off
					tmprtHist.setDataformatsupport(0);
					// add _HIST_RAW to the name
					tmprtHist.setTypename(rt.getTypename() + "_HIST_RAW");
					if (!rtypeNames.contains(tmprtHist.getTypename())) {
						rtypes_dc.add(tmprtHist);
						rtypeNames.add(tmprtHist.getTypename());
					}
				}
			}
		}

		log.fine(rtypes_dc.size() + " current_dc reference types defined");

		final Vector<Referencetable> allRtypes = new Vector<Referencetable>();
		allRtypes.addAll(rtypes);
		allRtypes.addAll(rtypes_dc);

		final Iterator<Referencetable> rti = allRtypes.iterator();

		log.finer("Verifying reference types");

		while (rti.hasNext()) {
			final Referencetable rt = rti.next();

			log.finer("Checking reference type " + rt.getTypename());

			final Iterator<Dwhtype> e = existing_types.iterator();

			Dwhtype found = null;
			while (e.hasNext()) {
				final Dwhtype dt = e.next();

				if (dt.getTypename().equalsIgnoreCase(rt.getTypename())) {
					found = dt;
					break;
				}
			} 

			if (found == null) {
				// The type does not exist yet. If it is not a view, then create the type.
				if (!(rt.getTable_type() != null && rt.getTable_type().equalsIgnoreCase("VIEW"))) {
					// Reference type is not found and the type is not a "view" -> create.
					log.finest("Creating new SIMPLE type");

					final Dwhtype newtype = new Dwhtype(dwhRepRock);

					newtype.setTechpack_name(techpackname);
					newtype.setTypename(rt.getTypename());
					newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
					newtype.setStorageid(rt.getTypename() + ":PLAIN");
					newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
					newtype.setStatus(DWHTYPE_STATUS_ENABLED);
					newtype.setOwner(DC);
					newtype.setViewtemplate("");
					newtype.setCreatetemplate(CREATEPARTITION_VM);
					newtype.setNextpartitiontime(null);
					newtype.setBasetablename(rt.getTypename());
					newtype.setDatadatecolumn(null);
					newtype.setPartitionsize(-1L);
					newtype.setType(DWHTYPE_TYPE_SIMPLE);
					newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
					newtype.saveDB();
					created_types.add(newtype);
					log.fine("A new DWHType created " + newtype.getStorageid());
				}
			} else {
				// The type already exists.
				log.finest("Type already exists");
				existing_types.remove(found);

				// If the table type is view, then remove the type from DWH.
				// This is done, because only tables are to be included in the DwhTypes.
				// The special AGGLEVEL reference type might still exist in the
				// types even though it is a "view only type". If the existing type is not
				// a view, then mark it as enabled.
				if (rt.getTable_type() != null
						&& rt.getTable_type().equalsIgnoreCase("VIEW")) {
					// Existing reference type is a "view". Remove from DwhTypes.
					log.info("Checking entry in refrence table DWHPartition for Type "
							+ found.getStorageid());
					final Dwhpartition dwhPartition = new Dwhpartition(dwhRepRock);
					dwhPartition.setStorageid(found.getStorageid());
					final DwhpartitionFactory dwhpartitionFactory = new DwhpartitionFactory(
							dwhRepRock, dwhPartition);
					final Vector<Dwhpartition> dwhm = dwhpartitionFactory.get();
					
					if (dwhm.size() > 0) {
						final Dwhpartition part = dwhm.get(0);
						part.deleteDB();
						log.info("Entry is found in refrence table DWHPartition for Type "
								+ found.getStorageid() + " .Now Entry is REMOVED, because it is a view only.");
					} else {
						log.info("Entry is not found in refrence table DWHPartition for Type "
								+ found.getStorageid() + " .Now removing entry from DWHType table.");
					}
					
					found.deleteDB();
					log.info("Type " + found.getStorageid() + " is REMOVED, because it is a view only.");
				} else {
					// Existing type is not a view. Mark as enabled.
					deleted_types.add(found);

					if (!found.getStatus().equalsIgnoreCase(ENABLED)) {
						log.info("Reference type " + found.getTypename()
								+ " is again active in techpack. Marking with " + ENABLED);
						found.setStatus(ENABLED);
						found.updateDB();
						log.finest("Enabled status written");
					}
				}
			}
		}

		// Reference types checked and missing types added

		final Iterator<Measurementtype> mte = mtypes.iterator();

		log.finer("Verifying measurement types");

		while (mte.hasNext()) {
			final Measurementtype mt = mte.next();

			log.finest("Checking measurement type " + mt.getTypename());

			final Measurementtable mta_cond = new Measurementtable(dwhRepRock);
			mta_cond.setTypeid(mt.getTypeid());
			final MeasurementtableFactory mta_fact = new MeasurementtableFactory(dwhRepRock, mta_cond);
			final Vector<Measurementtable> mtables = mta_fact.get();
			final Enumeration<Measurementtable> mtae = mtables.elements();

			while (mtae.hasMoreElements()) {
				final Measurementtable mtab = mtae.nextElement();

				log.finest("Checking measurement type " + mt.getTypename()
						+ " level " + mtab.getTablelevel());

				final Iterator<Dwhtype> e = existing_types.iterator();
				Dwhtype found = null;
				while (e.hasNext()) {
					final Dwhtype dt = e.next();

					if (dt.getTypename().equalsIgnoreCase(mt.getTypename())
							&& dt.getTablelevel().equalsIgnoreCase(mtab.getTablelevel())) {
						found = dt;
						break;
					}
				}

				if (found == null) {
					final Dwhtype newType = createDwhTypeForMeasType(mt, mtab);
					created_types.add(newType);
				} else {
					log.finest("Type already exists");

					found.setPartitionplan(mtab.getPartitionplan());
					found.updateDB();
					log.finest("Updated partitionplan.");

					existing_types.remove(found);
					deleted_types.add(found);
				}
			}

			// Create simple type for _PREV tables
			if (mt.getJoinable() != null && mt.getJoinable().length() > 0) {
				final String typename = mt.getTypename() + "_PREV";
				final Iterator<Dwhtype> e = existing_types.iterator();
				Dwhtype erase = null;
				while (e.hasNext()) {
					final Dwhtype dt = e.next();
					if (dt.getTypename().equalsIgnoreCase(typename)) {
						erase = dt;
						break;
					}
				}

				if (erase != null) {
					log.finest("Type already exists " + typename);
					existing_types.remove(erase);
					deleted_types.add(erase);
				} else {
					final Dwhtype newtype = new Dwhtype(dwhRepRock);

					newtype.setTechpack_name(techpackname);
					newtype.setTypename(typename);
					newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
					newtype.setStorageid(typename + ":PLAIN");
					newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
					newtype.setStatus(DWHTYPE_STATUS_ENABLED);
					newtype.setOwner(DC);
					newtype.setViewtemplate("");
					newtype.setCreatetemplate(CREATEPARTITION_VM);
					newtype.setNextpartitiontime(null);
					newtype.setBasetablename(typename);
					newtype.setDatadatecolumn(null);
					newtype.setPartitionsize(-1L);
					newtype.setType(DWHTYPE_TYPE_SIMPLE);
					newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
					newtype.saveDB();
					created_types.add(newtype);
					log.fine("A new PREV DWHType " + newtype.getStorageid());
				}
			}
			
			// New vector handling
			if(newVectorFlag) {
				if(mt.getVectorsupport() != null && mt.getVectorsupport() == 1) {
					final Measurementcounter mc_cond = new Measurementcounter(dwhRepRock);
					mc_cond.setTypeid(mt.getTypeid());
					final MeasurementcounterFactory mc_condF = new MeasurementcounterFactory(dwhRepRock, 
							mc_cond);
					for (Measurementcounter mc : mc_condF.get()) {
						// replace DC with DIM in DC_X_YYY_ZZZ
						final String vecType = "DIM" + mt.getTypename().substring(mt.getTypename().indexOf("_")) 
								+ "_" + mc.getDataname();
						vectorDimTables.add(vecType);
					}
					
					if(!isVector) {
						final String vectorTypename = "DIM" + techpackname.substring(techpackname.indexOf("_")) 
						+ "_VECTOR_REFERENCE";
						final Vector<Dwhtype> tmp = new Vector<Dwhtype>();
						tmp.addAll(existing_types);
						tmp.addAll(created_types);
						tmp.addAll(deleted_types);

						final Iterator<Dwhtype> e = tmp.iterator();
						Dwhtype erase = null;
						while (e.hasNext()) {
							final Dwhtype dt = e.next();
							if (dt.getTypename().equalsIgnoreCase(vectorTypename)) {
								erase = dt;
								break;
							}
						}
						if (erase != null) {
							log.finest("Vector counter type already exists " + vectorTypename);
							existing_types.remove(erase);
							deleted_types.add(erase);
							isVector = true;
						} else {
							final Dwhtype newtype = new Dwhtype(dwhRepRock);
							newtype.setTechpack_name(techpackname);
							newtype.setTypename(vectorTypename);
							newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
							newtype.setStorageid(vectorTypename + ":PLAIN");
							newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
							newtype.setStatus(DWHTYPE_STATUS_ENABLED);
							newtype.setOwner(DC);
							newtype.setViewtemplate("");
							newtype.setCreatetemplate(CREATEPARTITION_VM);
							newtype.setNextpartitiontime(null);
							newtype.setBasetablename(vectorTypename);
							newtype.setDatadatecolumn(null);
							newtype.setPartitionsize(-1L);
							newtype.setType(DWHTYPE_TYPE_SIMPLE);
							newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
							created_types.add(newtype);
							newtype.saveDB();
							isVector = true;
							log.fine("A new Vector counter DWHType created " + newtype.getStorageid());
						}
					}
				}
			}
			else {
				// OLD vector handling
				if (mt.getVectorsupport() != null && mt.getVectorsupport() == 1) {
					// Create vectorCounter DIM tables
					final Measurementcounter mc_cond = new Measurementcounter(dwhRepRock);
					mc_cond.setTypeid(mt.getTypeid());
					final MeasurementcounterFactory mc_condF = new MeasurementcounterFactory(
							dwhRepRock, mc_cond);

					for (Measurementcounter mc : mc_condF.get()) {
						// replace DC with DIM in DC_X_YYY_ZZZ
						final String typename = "DIM" + mt.getTypename().substring(
										mt.getTypename().indexOf("_")) + "_" + mc.getDataname();

						final Vector<Dwhtype> tmp = new Vector<Dwhtype>();
						tmp.addAll(existing_types);
						tmp.addAll(created_types);
						tmp.addAll(deleted_types);

						final Iterator<Dwhtype> e = tmp.iterator();
						Dwhtype erase = null;
						while (e.hasNext()) {
							final Dwhtype dt = e.next();
							if (dt.getTypename().equalsIgnoreCase(typename)) {
								erase = dt;
								break;
							}
						}

						if (erase != null) {
							log.finest("Vector counter type already exists " + typename);
							existing_types.remove(erase);
							deleted_types.add(erase);
						} else {
							final Dwhtype newtype = new Dwhtype(dwhRepRock);

							newtype.setTechpack_name(techpackname);
							newtype.setTypename(typename);
							newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
							newtype.setStorageid(typename + ":PLAIN");
							newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
							newtype.setStatus(DWHTYPE_STATUS_ENABLED);
							newtype.setOwner(DC);
							newtype.setViewtemplate("");
							newtype.setCreatetemplate(CREATEPARTITION_VM);
							newtype.setNextpartitiontime(null);
							newtype.setBasetablename(typename);
							newtype.setDatadatecolumn(null);
							newtype.setPartitionsize(-1L);
							newtype.setType(DWHTYPE_TYPE_SIMPLE);
							newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
							created_types.add(newtype);
							newtype.saveDB();
							log.fine("A new Vector counter DWHType created " + newtype.getStorageid());
						}
					}
				}
			}
			
			final Measurementobjbhsupport mobhs = new Measurementobjbhsupport(dwhRepRock);
			mobhs.setTypeid(mt.getTypeid());
			final MeasurementobjbhsupportFactory mobhsF = new MeasurementobjbhsupportFactory(
					dwhRepRock, mobhs);

			// ELEMBH
			if ((mt.getElementbhsupport() != null && mt.getRankingtable() != null 
					&& mt.getElementbhsupport() == 1) && mt.getRankingtable() > 0) {

				// Create ELEMBH DIM table
				// replace DC_E_XXX with DIM_E_XXX_ELEMBH_BHTYPE
				final String typename = "DIM" + mt.getVendorid().substring(
								mt.getVendorid().indexOf("_")) + "_ELEMBH_BHTYPE";

				final Vector<Dwhtype> tmp = new Vector<Dwhtype>();
				tmp.addAll(existing_types);
				tmp.addAll(created_types);
				tmp.addAll(deleted_types);

				final Iterator<Dwhtype> e = tmp.iterator();
				Dwhtype erase = null;
				while (e.hasNext()) {
					final Dwhtype dt = e.next();
					if (dt.getTypename().equalsIgnoreCase(typename)) {
						erase = dt;
						break;
					}
				}

				if (erase != null) {
					log.finest("ELEMBH type already exists " + typename);
					existing_types.remove(erase);
					deleted_types.add(erase);
				} else {
					final Dwhtype newtype = new Dwhtype(dwhRepRock);

					newtype.setTechpack_name(techpackname);
					newtype.setTypename(typename);
					newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
					newtype.setStorageid(typename + ":PLAIN");
					newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
					newtype.setStatus(DWHTYPE_STATUS_ENABLED);
					newtype.setOwner(DC);
					newtype.setViewtemplate("");
					newtype.setCreatetemplate(CREATEPARTITION_VM);
					newtype.setNextpartitiontime(null);
					newtype.setBasetablename(typename);
					newtype.setDatadatecolumn(null);
					newtype.setPartitionsize(-1L);
					newtype.setType(DWHTYPE_TYPE_SIMPLE);
					newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
					created_types.add(newtype);
					newtype.saveDB();
					log.fine("A new ELEMBH DWHType created "
							+ newtype.getStorageid());
				}
			}

			// OBJBH
			if (mt.getRankingtable() != null && mobhsF != null
					&& !mobhsF.get().isEmpty() && mt.getRankingtable() > 0) {
				// Create BH DIM table
				// replace DC_E_XXX_YYY with DIM_E_XXX_YYY_BHTYPE
				final String typename = "DIM" + mt.getTypename().substring(
								mt.getTypename().indexOf("_")) + "_BHTYPE";

				final Vector<Dwhtype> tmp = new Vector<Dwhtype>();
				tmp.addAll(existing_types);
				tmp.addAll(created_types);
				tmp.addAll(deleted_types);

				final Iterator<Dwhtype> e = tmp.iterator();
				Dwhtype erase = null;
				while (e.hasNext()) {
					final Dwhtype dt = e.next();
					if (dt.getTypename().equalsIgnoreCase(typename)) {
						erase = dt;
						break;
					}
				}

				if (erase != null) {
					log.finest("OBJBH type already exists " + typename);
					existing_types.remove(erase);
					deleted_types.add(erase);
				} else {
					final Dwhtype newtype = new Dwhtype(dwhRepRock);

					newtype.setTechpack_name(techpackname);
					newtype.setTypename(typename);
					newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
					newtype.setStorageid(typename + ":PLAIN");
					newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
					newtype.setStatus(DWHTYPE_STATUS_ENABLED);
					newtype.setOwner(DC);
					newtype.setViewtemplate("");
					newtype.setCreatetemplate(CREATEPARTITION_VM);
					newtype.setNextpartitiontime(null);
					newtype.setBasetablename(typename);
					newtype.setDatadatecolumn(null);
					newtype.setPartitionsize(-1L);
					newtype.setType(DWHTYPE_TYPE_SIMPLE);
					newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
					created_types.add(newtype);
					newtype.saveDB();
					log.fine("A new OBJBH DWHType created " + newtype.getStorageid());
				}
			}

			if (TechPackType.EVENTS == techPackType) {
				// if type has ETL duplicate check enabled add table for that
				if (mt.getLoadfile_dup_check() != null && mt.getLoadfile_dup_check() == 1) {
					verifyETLDuplicateCheckTable(mt, created_types, existing_types);
				}
			}
		} 

		// Measurement types checked and missing types added
		if (TechPackType.EVENTS == techPackType) {
			// do the group types tables now...
			verifyGroupTypes(dwhRepRock, newVersionID, created_types, existing_types);
		}
		
		if(newVectorFlag) {
			if (oldVectors.isEmpty() && isVector) {
				if (existing_types.size() > 0) {
					Vector<Dwhtype> vecToRemove = new Vector<Dwhtype>();
					for (Dwhtype vecType : existing_types) {
						if (vectorDimTables.contains(vecType.getBasetablename())) {
							log.fine("Updating to new vector handling");
							vecToRemove.add(vecType);
							vecType.setStatus(NEWVECTOR);
							vecType.updateDB();
							log.info("Type " + vecType.getStorageid() + " follows old vector handling");
						}
					}
					
					existing_types.removeAll(vecToRemove);
				}
			}
		}

		if (existing_types.size() > 0) {
			// Workaround for EBS techpacks for CR 132: Started
			for (Dwhtype tempType : existing_types) {
				log.info(" existing_types value :: TypeName is "
						+ tempType.getTypename() + "and storageID is"
						+ tempType.getStorageid() + " and TypeLevel is "
						+ tempType.getTablelevel());
				if (tempType.getStorageid().startsWith("PM_E")
						&& tempType.getTablelevel().equalsIgnoreCase("DAYBH")) {
					log.info(existing_types.size()
							+ " types found from dwhrep but not from techpack");
				}

				else {
					for (Dwhtype type : existing_types) {
						if (ebs) {
							log.info(" ebs value : " + ebs);
							type.deleteDB();
							log.info("Type " + type.getStorageid() + " is REMOVED");
						} else if (newVectorFlag && vectorDimTables.contains(type.getBasetablename())) {
							log.fine("Updating to new vector handling");
							type.setStatus(NEWVECTOR);
							type.updateDB();
							log.info("Type " + type.getStorageid() + " follows old vector handling");
						}
						else {
							type.setStatus(OBSOLETE);
							type.updateDB();
							log.info("Type " + type.getStorageid() + " is OBSOLETE");
						}
					}
				}
			}
			// Workaround for EBS techpacks for CR 132: End
		}
	}

	/**
	 * Filter out old vector tables from dwhtypes
	 * 
	 * @param existing_types
	 * @param oldVectors 
	 * @return existing_types - with old vector tables filtered out
	 */
	private Vector<Dwhtype> filterOldVector(Vector<Dwhtype> existing_types) {
		Vector<Dwhtype> toRemove = new Vector<Dwhtype>();
		for (Dwhtype vect : existing_types) {
			if (vect.getStatus().equals(NEWVECTOR)) {
				toRemove.add(vect);
			}
		}
		existing_types.removeAll(toRemove);
		return existing_types;
	}

	/**
	 * Check if new vector implementation is done (search for OLDVECTOR
	 * in DWHType table)
	 *  
	 * @return 	False if not implemented
	 * 			True if implemented
	 * @throws RockException 
	 * @throws SQLException 
	 */
	private Vector<Dwhtype> OldVectorTypes() throws SQLException, RockException {
		final Dwhtype dt_cond = new Dwhtype(dwhRepRock);
		dt_cond.setTechpack_name(techpackname);
		dt_cond.setStatus(NEWVECTOR);
		final DwhtypeFactory dt_fact = new DwhtypeFactory(dwhRepRock, dt_cond);
		return dt_fact.get();
	}

	/**
	 * Creates a new DwhType for this particular measurementType
	 * 
	 * @param measType
	 * @param measTable
	 * @return .
	 * @throws SQLException .
	 * @throws RockException .
	 */
	protected Dwhtype createDwhTypeForMeasType(final Measurementtype measType,
			final Measurementtable measTable) throws SQLException, RockException {
		log.finest("Creating new type");

		final Dwhtype newtype = new Dwhtype(dwhRepRock);

		newtype.setTechpack_name(techpackname);
		newtype.setTypename(measType.getTypename());
		newtype.setTablelevel(measTable.getTablelevel());
		newtype.setStorageid(measType.getTypename() + ":"
				+ measTable.getTablelevel());
		newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
		newtype.setStatus(ENABLED);
		newtype.setOwner(DC);

		final Integer eventsCalcTableValue = Utils.replaceNull(measType.getEventscalctable());
		// If eventsCalcTableValue is 1, then this means that this
		// MeasurementType is an Events Calc table type
		if (eventsCalcTableValue == 1) {
			newtype.setViewtemplate(CREATEEVENTSCALCVIEW_VM);
			newtype.setPublicviewtemplate(CREATEPUBLICEVENTSCALCVIEW_VM);
		} else {
			newtype.setViewtemplate(CREATEVIEW_VM);
			newtype.setPublicviewtemplate(CREATEPUBLICVIEW_VM);
		}

		newtype.setCreatetemplate(CREATEPARTITION_VM);
		newtype.setNextpartitiontime(null);
		newtype.setBasetablename(measTable.getBasetablename());
		newtype.setDatadatecolumn(DATE_ID);
		newtype.setType(PARTITIONED);

		newtype.setPartitionplan(measTable.getPartitionplan());
		newtype.setPartitionsize(-1L);
		newtype.saveDB();

		log.fine("A new DWHType created " + newtype.getStorageid() + " type " + newtype.getType());
		return newtype;
	}

	/**
	 * Returns a map of measurement columns of new TP<br>
	 * Mapping: MTABLEID -> Vector of MeasurementColumns
	 * 
	 * @return .
	 * @throws Exception .
	 */
	private Map<String, Vector<Measurementcolumn>> getMeasurementColumns() throws Exception {
		final Map<String, Vector<Measurementcolumn>> result = new HashMap<String, Vector<Measurementcolumn>>();

		try {
			// versioning
			final Versioning ver = new Versioning(dwhRepRock);
			ver.setTechpack_name(techpackname);
			ver.setVersionid(newVersionID);

			final VersioningFactory verf = new VersioningFactory(dwhRepRock, ver);

			if (verf != null) {
				final Vector<Versioning> vers = verf.get();

				if (vers.size() >= 1) {
					final Versioning vertmp = vers.get(0);

					// Measurementtype
					final Measurementtype meast = new Measurementtype(dwhRepRock);
					meast.setVersionid(vertmp.getVersionid());
					// For Node Version Update - Filter out the exact meas type.
					filterMeasType(meast);
					final MeasurementtypeFactory meastf = new MeasurementtypeFactory(dwhRepRock, meast);

					if (meastf != null) {
						for (Measurementtype measttmp : meastf.get()) {
							try {
								if (measttmp.getJoinable() != null && measttmp.getJoinable().length() > 0) {

									// columns for _PREV tables
									final Measurementtable mta = new Measurementtable(dwhRepRock);
									mta.setTypeid(measttmp.getTypeid());
									final MeasurementtableFactory mtaf = new MeasurementtableFactory(
											dwhRepRock, mta);

									if (mtaf != null) {
										for (Measurementtable mtatmp : mtaf.get()) {
											try {
												if (mtatmp.getTablelevel().equalsIgnoreCase("raw")) {

													// Measurementcolumn
													final Measurementcolumn meas = new Measurementcolumn(
															dwhRepRock);
													meas.setMtableid(mtatmp.getMtableid());
													final MeasurementcolumnFactory measf = new MeasurementcolumnFactory(
															dwhRepRock, meas);

													final Vector<Measurementcolumn> measurementcolumns = measf.get();
													Collections.sort(measurementcolumns,new Comparator<Object>() {
																		@Override
																		public int compare(final Object o1, final Object o2) {
																			final Measurementcolumn m1 = (Measurementcolumn) o1;
																			final Measurementcolumn m2 = (Measurementcolumn) o2;

																			final Long l1 = m1.getColnumber();
																			final Long l2 = m2.getColnumber();

																			return l1.compareTo(l2);
																		}

																		public boolean equals(final Object o1, final Object o2) {
																			final Measurementcolumn m1 = (Measurementcolumn) o1;
																			final Measurementcolumn m2 = (Measurementcolumn) o2;

																			return m1.equals(m2);
																		}
																	});

													final String mtID = mtatmp.getMtableid();
													final String start = mtID.substring(0, mtID.lastIndexOf(":"));
													result.put(start + "_PREV:PLAIN", measurementcolumns);
												}
											} catch (Exception e) {
												log.log(Level.WARNING,
														"Error while iterating Measurementcolumn table", e);
												throw (e);
											}
										}
									}
								}

								// datatitem
								final Measurementtable mta = new Measurementtable(dwhRepRock);
								mta.setTypeid(measttmp.getTypeid());
								final MeasurementtableFactory mtaf = new MeasurementtableFactory(
										dwhRepRock, mta);

								if (mtaf != null) {
									for (Measurementtable mtatmp : mtaf.get()) {
										try {

											// Measurementcolumn
											final Measurementcolumn meas = new Measurementcolumn(dwhRepRock);
											meas.setMtableid(mtatmp
													.getMtableid());
											final MeasurementcolumnFactory measf = new MeasurementcolumnFactory(
													dwhRepRock, meas);

											final Vector<Measurementcolumn> measurementcolumns = measf.get();
											Collections.sort(measurementcolumns, new Comparator<Object>() {

														@Override
														public int compare(final Object o1, final Object o2) {
															final Measurementcolumn m1 = (Measurementcolumn) o1;
															final Measurementcolumn m2 = (Measurementcolumn) o2;

															final Long l1 = m1.getColnumber();
															final Long l2 = m2.getColnumber();

															return l1.compareTo(l2);
														}

														public boolean equals(final Object o1, final Object o2) {
															final Measurementcolumn m1 = (Measurementcolumn) o1;
															final Measurementcolumn m2 = (Measurementcolumn) o2;

															return m1.equals(m2);
														}
													});

											result.put(mtatmp.getMtableid(), measurementcolumns);

										} catch (Exception e) {
											log.log(Level.WARNING,
													"Error while iterating Measurementcolumn table", e);
											throw (e);
										}
									}
								}
							} catch (Exception e) {
								log.log(Level.WARNING,
										"Error while iterating Measurementtable table", e);
								throw (e);
							}
						}
					}
				} else {
					log.warning("No such version " + newVersionID + " of techpack " + techpackname);
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Error while retrieving Versioning table", e);
			throw (e);
		}

		return result;
	}

	/**
	 * Returns map of reference columns of new techpack<br>
	 * Map: typeID -> Vector of Referencecolumns
	 * 
	 * @return .
	 * @throws Exception .
	 */
	private Map<String, Vector<Referencecolumn>> getReferenceColumns()
			throws Exception {

		Map<String, Vector<Referencecolumn>> result;

		try {
			result = new HashMap<String, Vector<Referencecolumn>>();

			// versioning
			final Versioning ver = new Versioning(dwhRepRock);
			ver.setTechpack_name(techpackname);
			ver.setVersionid(newVersionID);

			final VersioningFactory verf = new VersioningFactory(dwhRepRock, ver);

			if (verf != null) {
				for (Versioning vertmp : verf.get()) {
					try {
						// Reference Table
						final Referencetable reft = new Referencetable(dwhRepRock);
						reft.setVersionid(vertmp.getVersionid());
						// For Node Version Update - Filter out the exact meas  type.
						filterMeasType(reft);
						final ReferencetableFactory reftf = new ReferencetableFactory(
								dwhRepRock, reft);

						if (reftf != null) {

							for (Referencetable refttmp : reftf.get()) {

								try {

									// eeoidiv 20091203 : Timed Dynamic topology
									// handling in ENIQ, WI 6.1.2, (284/159
									// 41-FCP 103 8147) Improved WRAN Topology
									// in ENIQ
									// if reference tables update policy is
									// dynamic(2) or timed dynamic(3)
									// 20110830 EANGUAN :: Adding comparison for
									// policy number 4 for History Dynamic (SON)
									if ((refttmp.getUpdate_policy() == 2)
											|| (refttmp.getUpdate_policy() == 3)
											|| (refttmp.getUpdate_policy() == 4)) {

										final String typeid = refttmp.getTypeid() + "_CURRENT_DC";

										// Referencecolumn
										final Referencecolumn reas = new Referencecolumn(dwhRepRock);
										reas.setTypeid(refttmp.getTypeid());
										final ReferencecolumnFactory reff = new ReferencecolumnFactory(
												dwhRepRock, reas);

										final Vector<Referencecolumn> referencecolumns = new Vector<Referencecolumn>();

										for (Referencecolumn refc : reff.get()) {
											final Referencecolumn newref = (Referencecolumn) refc.clone();
											newref.setIncludesql(0);
											newref.setTypeid(typeid);
											referencecolumns.add(newref);
										}

										Collections.sort(referencecolumns, new Comparator<Object>() {

													@Override
													public int compare(final Object o1, final Object o2) {
														final Referencecolumn r1 = (Referencecolumn) o1;
														final Referencecolumn r2 = (Referencecolumn) o2;

														final Long l1 = r1.getColnumber();
														final Long l2 = r2.getColnumber();

														return l1.compareTo(l2);
													}

													public boolean equals(final Object o1, final Object o2) {
														final Referencecolumn r1 = (Referencecolumn) o1;
														final Referencecolumn r2 = (Referencecolumn) o2;

														return r1.equals(r2);
													}
												});

										result.put(typeid, referencecolumns);

										// eromsza: History Dynamic: If data
										// format support is false, don't handle
										// _HIST and _CALC tables
										boolean dataFormatSupport = true;
										if (refttmp.getDataformatsupport() != null) {
											dataFormatSupport = refttmp.getDataformatsupport().intValue() == 1;
										}

										// eeoidiv,20110926:Automatically create
										// _CALC table for update
										// policy=4=HistoryDynamic (like
										// _CURRENT_DC).
										if (refttmp.getUpdate_policy() == 4 && dataFormatSupport) {
											final String typeidCalc = refttmp.getTypeid() + "_CALC";
											result.put(typeidCalc, createReferenceColumnCopies(
															refttmp, typeidCalc));
											final String typeidHist = refttmp.getTypeid() + "_HIST_RAW";
											result.put(typeidHist, createReferenceColumnCopies(refttmp, typeidHist));
										}// if(refttmp.getUpdate_policy() == 4)
									}

									// Referencecolumn
									final Referencecolumn reas = new Referencecolumn(dwhRepRock);
									reas.setTypeid(refttmp.getTypeid());
									final ReferencecolumnFactory reff = new ReferencecolumnFactory(
											dwhRepRock, reas);

									final Vector<Referencecolumn> referencecolumns = reff.get();

									Collections.sort(referencecolumns, new Comparator<Object>() {

												@Override
												public int compare(final Object o1, final Object o2) {
													final Referencecolumn r1 = (Referencecolumn) o1;
													final Referencecolumn r2 = (Referencecolumn) o2;

													final Long l1 = r1.getColnumber();
													final Long l2 = r2.getColnumber();

													return l1.compareTo(l2);
												}

												public boolean equals(final Object o1, final Object o2) {
													final Referencecolumn r1 = (Referencecolumn) o1;
													final Referencecolumn r2 = (Referencecolumn) o2;

													return r1.equals(r2);
												}
											});

									result.put(refttmp.getTypeid(), referencecolumns);

								} catch (Exception e) {
									log.log(Level.WARNING,
											"Error while iterating Referencecolumn table", e);
									throw (e);
								}
							}
						}
					} catch (Exception e) {
						log.log(Level.WARNING, "Error while iterating Referencetable table", e);
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
	 * Want to create new ReferenceColumns that are copies based on another
	 * ReferenceTable (refttmp). New columns will belong to ReferenceTable named
	 * (refTblName).
	 * 
	 * @param refttmp
	 * @param appendStr
	 * @throws SQLException
	 * @throws RockException
	 */
	private Vector<Referencecolumn> createReferenceColumnCopies(
			final Referencetable refttmp, final String refTblName)
			throws SQLException, RockException {
		// Query for list of Referencecolumns in existing ReferenceTable
		final Referencecolumn refCol = new Referencecolumn(dwhRepRock);
		refCol.setTypeid(refttmp.getTypeid());
		final ReferencecolumnFactory reffCal = new ReferencecolumnFactory(
				dwhRepRock, refCol);
		final Vector<Referencecolumn> referencecolumnsCalc = new Vector<Referencecolumn>();
		// Loop through list of existing ReferenceColumns
		for (Referencecolumn refc : reffCal.get()) {
			// Clone ReferenceColumn and set to belong to new Typeid.
			final Referencecolumn newref = (Referencecolumn) refc.clone();
			newref.setIncludesql(0); // Set IncludeSQL to false
			newref.setTypeid(refTblName);
			referencecolumnsCalc.add(newref);
		}
		// Sort based on column number
		Collections.sort(referencecolumnsCalc, new Comparator<Object>() {
			@Override
			public int compare(final Object o1, final Object o2) {
				final Referencecolumn r1 = (Referencecolumn) o1;
				final Referencecolumn r2 = (Referencecolumn) o2;
				final Long l1 = r1.getColnumber();
				final Long l2 = r2.getColnumber();
				return l1.compareTo(l2);
			}

			public boolean equals(final Object o1, final Object o2) {
				final Referencecolumn r1 = (Referencecolumn) o1;
				final Referencecolumn r2 = (Referencecolumn) o2;
				return r1.equals(r2);
			}
		});
		return referencecolumnsCalc;
	} // createReferenceColumnCopies

	/**
	 * Returns map of reference columns of vector counter<br>
	 * Map: typeID -> Vector of Referencecolumns
	 * 
	 * @return .
	 * @throws Exception .
	 */
	private Map<String, Vector<Referencecolumn>> getVectorCounterColumns()
			throws Exception {
		Map<String, Vector<Referencecolumn>> result;
		try {
			result = new HashMap<String, Vector<Referencecolumn>>();

			// versioning
			final Versioning ver = new Versioning(dwhRepRock);
			ver.setTechpack_name(techpackname);
			ver.setVersionid(newVersionID);

			final VersioningFactory verf = new VersioningFactory(dwhRepRock, ver);

			if (verf != null) {
				final Vector<Versioning> vers = verf.get();

				if (vers.size() >= 1) {
					final Versioning vertmp = vers.get(0);

					// Measurementtype
					final Measurementtype meast = new Measurementtype(dwhRepRock);
					meast.setVersionid(vertmp.getVersionid());
					// For Node Version Update - Filter out the exact meas type.
					filterMeasType(meast);
					final MeasurementtypeFactory meastf = new MeasurementtypeFactory(dwhRepRock, meast);

					if (meastf != null) {
						for (Measurementtype measttmp : meastf.get()) {
							try {
								if (measttmp != null && measttmp.getVectorsupport() != null
										&& measttmp.getVectorsupport() > 0) {
	
									// vector counter
									final Measurementtable mta = new Measurementtable(dwhRepRock);
									mta.setTypeid(measttmp.getTypeid());
									final MeasurementtableFactory mtaf = new MeasurementtableFactory(
											dwhRepRock, mta);
	
									if (mtaf != null) {
										for (Measurementtable mtatmp : mtaf.get()) {
											try {
												if (mtatmp.getTablelevel().equalsIgnoreCase("raw")) {
	
													final Measurementtype mt_cond = new Measurementtype(
															dwhRepRock);
													mt_cond.setTypeid(measttmp.getTypeid());
													final MeasurementtypeFactory mt_condf = new MeasurementtypeFactory(
															dwhRepRock, mt_cond);
	
													// there should be only one
													final Measurementtype mt = mt_condf.getElementAt(0);
	
													// Vectorcounter
													final Measurementvector mv_cond = new Measurementvector(
															dwhRepRock);
													mv_cond.setTypeid(mt.getTypeid());
													final MeasurementvectorFactory vc_condF = new MeasurementvectorFactory(
															dwhRepRock, mv_cond);
	
													for (Measurementvector vc : vc_condF.get()) {
	
														final Vector<Referencecolumn> referencecolumns = new Vector<Referencecolumn>();
	
														final String typeid = newVersionID + ":DIM"
																+ mt.getTypename().substring(mt.getTypename().indexOf("_"))
																+ "_" + vc.getDataname();
														final Referencecolumn dcvec = new Referencecolumn(dwhRepRock);
														dcvec.setTypeid(typeid);
														dcvec.setDataname(vc.getDataname() + "_DCVECTOR");
														dcvec.setColnumber((long) 1);
														dcvec.setDatatype("int");
														dcvec.setDatasize(0);
														dcvec.setDatascale(0);
														dcvec.setUniquevalue((long) 255);
														dcvec.setNullable(1);
														dcvec.setIndexes("HG");
														dcvec.setUniquekey(1);
														dcvec.setIncludesql(0);
														dcvec.setIncludeupd(0);
														dcvec.setColtype(null);
														dcvec.setDescription(null);
														dcvec.setUniverseclass(null);
														dcvec.setUniverseobject(null);
														dcvec.setUniversecondition(null);
														referencecolumns.add(dcvec);
	
														final Referencecolumn value = new Referencecolumn(dwhRepRock);
														value.setTypeid(typeid);
														value.setDataname(vc.getDataname() + "_VALUE");
														value.setColnumber((long) 2);
														value.setDatatype("varchar");
														value.setDatasize(80);
														value.setDatascale(0);
														value.setUniquevalue((long) 255);
														value.setNullable(1);
														value.setIndexes("HG");
														value.setUniquekey(1);
														value.setIncludesql(0);
														value.setIncludeupd(0);
														value.setColtype(null);
														value.setDescription(null);
														value.setUniverseclass(null);
														value.setUniverseobject(null);
														value.setUniversecondition(null);
														referencecolumns.add(value);
	
														final Referencecolumn dcrel = new Referencecolumn(dwhRepRock);
														dcrel.setTypeid(typeid);
														dcrel.setDataname("DC_RELEASE");
														dcrel.setColnumber((long) 3);
														dcrel.setDatatype("varchar");
														dcrel.setDatasize(16);
														dcrel.setDatascale(0);
														dcrel.setUniquevalue((long) 255);
														dcrel.setNullable(1);
														dcrel.setIndexes("HG");
														dcrel.setUniquekey(1);
														dcrel.setIncludesql(0);
														dcrel.setIncludeupd(0);
														dcrel.setColtype(null);
														dcrel.setDescription(null);
														dcrel.setUniverseclass(null);
														dcrel.setUniverseobject(null);
														dcrel.setUniversecondition(null);
														referencecolumns.add(dcrel);
	
														// only do the pmRes
														// stuff in stats
														if (TechPackType.EVENTS != techPackType
																&& vc.getDataname().startsWith("pmRes")) {
															final Referencecolumn quantity = new Referencecolumn(
																	dwhRepRock);
															quantity.setTypeid(typeid);
															quantity.setDataname("QUANTITY");
															quantity.setColnumber((long) 4);
															quantity.setDatatype("int");
															quantity.setDatasize(0);
															quantity.setDatascale(0);
															quantity.setUniquevalue((long) 255);
															quantity.setNullable(1);
															quantity.setIndexes("HG");
															quantity.setUniquekey(0);
															quantity.setIncludesql(0);
															quantity.setIncludeupd(0);
															quantity.setColtype(null);
															quantity.setDescription(null);
															quantity.setUniverseclass(null);
															quantity.setUniverseobject(null);
															quantity.setUniversecondition(null);
															referencecolumns.add(quantity);
														}
	
														result.put(typeid, referencecolumns);
													}
												}
											} catch (Exception e) {
												log.log(Level.WARNING,
														"Error while iterating Measurementcolumn table", e);
												throw (e);
											}
										}
									}
								}
							} catch (Exception e) {
								log.log(Level.WARNING,
										"Error while retrieving Versioning table", e);
								throw (e);
							}
						}
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
	 * Creates a DWHType entry for measurement types that have the ETL file
	 * dublicate check feature enabled.
	 * 
	 * @param measType
	 *            .
	 * @param created_types
	 *            .
	 * @param existing_types
	 *            .
	 * @throws java.sql.SQLException .
	 * @throws ssc.rockfactory.RockException .
	 */
	private void verifyETLDuplicateCheckTable(final Measurementtype measType,
			final Vector<Dwhtype> created_types,
			final Vector<Dwhtype> existing_types) throws RockException,
			SQLException {

		final String typename = measType.getTypename() + "_DUBCHECK";

		Dwhtype erase = null;
		for (Dwhtype dt : existing_types) {
			if (dt.getTypename().equalsIgnoreCase(typename)) {
				erase = dt;
				break;
			}
		}

		if (erase != null) {
			log.finest("Type already exists " + typename);
			existing_types.remove(erase);
		} else {
			final Dwhtype newtype = new Dwhtype(dwhRepRock);
			newtype.setTechpack_name(techpackname);
			newtype.setTypename(typename);
			newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
			newtype.setStorageid(typename + ":PLAIN");
			newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
			newtype.setStatus(DWHTYPE_STATUS_ENABLED);
			newtype.setOwner(DC);
			newtype.setViewtemplate("");
			newtype.setCreatetemplate(CREATEPARTITION_VM);
			newtype.setNextpartitiontime(null);
			newtype.setBasetablename(typename);
			newtype.setDatadatecolumn(null);
			newtype.setPartitionsize((long) -1);
			newtype.setType(DWHTYPE_TYPE_SIMPLE);
			newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
			newtype.saveDB();
			created_types.add(newtype);
			log.fine("A new PREV DWHType " + newtype.getStorageid());
		}

	}

	/**
	 * Creates a column map for measurement types that have the ETL file
	 * dublicate check feature enabled.
	 * 
	 * @return ..
	 * @throws Exception .
	 */
	private Map<String, Vector<Referencecolumn>> getETLDubCheckColumns()
			throws Exception {

		final Map<String, Vector<Referencecolumn>> result = new HashMap<String, Vector<Referencecolumn>>();

		final Versioning ver = new Versioning(dwhRepRock);
		ver.setTechpack_name(techpackname);
		ver.setVersionid(newVersionID);
		final VersioningFactory verf = new VersioningFactory(dwhRepRock, ver);

		final Vector<Versioning> vers = verf.get();

		if (vers.size() >= 1) {

			final Versioning vertmp = vers.get(0);

			final Measurementtype meast = new Measurementtype(dwhRepRock);
			meast.setVersionid(vertmp.getVersionid());
			meast.setLoadfile_dup_check(1);
			final MeasurementtypeFactory meastf = new MeasurementtypeFactory(
					dwhRepRock, meast);

			for (Measurementtype measttmp : meastf.get()) {

				final Vector<Referencecolumn> referencecolumns = new Vector<Referencecolumn>();

				final String typeid = newVersionID + ":" + measttmp.getTypename() + "_DUBCHECK";

				final Referencecolumn bhtype = new Referencecolumn(dwhRepRock);
				bhtype.setTypeid(typeid);
				bhtype.setDataname("TABLENAME");
				bhtype.setColnumber((long) 1);
				bhtype.setDatatype("varchar");
				bhtype.setDatasize(255);
				bhtype.setDatascale(0);
				bhtype.setUniquevalue((long) 64);
				bhtype.setNullable(0);
				bhtype.setIndexes("HG");
				bhtype.setUniquekey(1);
				bhtype.setIncludesql(0);
				bhtype.setIncludeupd(0);
				bhtype.setColtype(null);
				bhtype.setDescription(null);
				bhtype.setUniverseclass(null);
				bhtype.setUniverseobject(null);
				bhtype.setUniversecondition(null);

				referencecolumns.add(bhtype);

				final Referencecolumn desc = new Referencecolumn(dwhRepRock);
				desc.setTypeid(typeid);
				desc.setDataname("FILENAME");
				desc.setColnumber((long) 2);
				desc.setDatatype("varchar");
				desc.setDatasize(255);
				desc.setDatascale(0);
				desc.setUniquevalue((long) 16777216);
				desc.setNullable(0);
				desc.setIndexes("HG");
				desc.setUniquekey(1);
				desc.setIncludesql(0);
				desc.setIncludeupd(0);
				desc.setColtype(null);
				desc.setDescription(null);
				desc.setUniverseclass(null);
				desc.setUniverseobject(null);
				desc.setUniversecondition(null);
				referencecolumns.add(desc);

				result.put(typeid, referencecolumns);
			}
		}

		return result;
	}

	/**
	 * @return .
	 * @throws Exception .
	 */
	protected Map<String, Vector<Referencecolumn>> getBHColumns()
			throws Exception {
		Map<String, Vector<Referencecolumn>> result;

		try {
			result = new HashMap<String, Vector<Referencecolumn>>();

			// versioning
			final Versioning ver = new Versioning(dwhRepRock);
			ver.setTechpack_name(techpackname);
			ver.setVersionid(newVersionID);

			final VersioningFactory verf = new VersioningFactory(dwhRepRock, ver);

			if (verf != null) {
				final Vector<Versioning> vers = verf.get();
				if (vers.size() >= 1) {
					final Versioning vertmp = vers.get(0);

					// Measurementtype
					final Measurementtype meast = new Measurementtype(dwhRepRock);
					meast.setVersionid(vertmp.getVersionid());
					// For Node Version Update - Filter out the exact meas type.
					filterMeasType(meast);
					final MeasurementtypeFactory meastf = new MeasurementtypeFactory(dwhRepRock, meast);
					if (meastf != null) {
						for (Measurementtype measttmp : meastf.get()) {
							try {
								final Measurementobjbhsupport mobhs = new Measurementobjbhsupport(dwhRepRock);
								mobhs.setTypeid(measttmp.getTypeid());
								final MeasurementobjbhsupportFactory mobhsF = new MeasurementobjbhsupportFactory(
										dwhRepRock, mobhs);

								// ELEMBH
								if (measttmp != null && measttmp.getRankingtable() != null && measttmp.getElementbhsupport() != null
										&& measttmp.getElementbhsupport() > 0 && measttmp.getRankingtable() > 0) {

									final Vector<Referencecolumn> referencecolumns = new Vector<Referencecolumn>();

									// DC_E_MGW -> yyy:DIM_E_MGW_ELEMBH_BHTYPE
									final String typeid = newVersionID + ":DIM" + measttmp.getVendorid().substring(
													measttmp.getVendorid().indexOf("_")) + "_ELEMBH_BHTYPE";

									// BHTYPE
									final Referencecolumn bhtype = new Referencecolumn(dwhRepRock);
									bhtype.setTypeid(typeid);
									bhtype.setDataname("BHTYPE");
									bhtype.setColnumber((long) 1);
									bhtype.setDatatype("varchar");
									bhtype.setDatasize(50);
									bhtype.setDatascale(0);
									bhtype.setUniquevalue((long) 255);
									bhtype.setNullable(1);
									bhtype.setIndexes("HG");
									bhtype.setUniquekey(1);
									bhtype.setIncludesql(1);
									bhtype.setIncludeupd(0);
									bhtype.setColtype(null);
									bhtype.setDescription(null);
									bhtype.setUniverseclass(null);
									bhtype.setUniverseobject(null);
									bhtype.setUniversecondition(null);

									referencecolumns.add(bhtype);

									// Description

									final Referencecolumn desc = new Referencecolumn(dwhRepRock);
									desc.setTypeid(typeid);
									desc.setDataname("DESCRIPTION");
									desc.setColnumber((long) 2);
									desc.setDatatype("varchar");
									desc.setDatasize(80);
									desc.setDatascale(0);
									desc.setUniquevalue((long) 255);
									desc.setNullable(1);
									desc.setIndexes("HG");
									desc.setUniquekey(1);
									desc.setIncludesql(1);
									desc.setIncludeupd(0);
									desc.setColtype(null);
									desc.setDescription(null);
									desc.setUniverseclass(null);
									desc.setUniverseobject(null);
									desc.setUniversecondition(null);
									referencecolumns.add(desc);

									result.put(typeid, referencecolumns);
								}

								// OBJBH
								if (mobhsF != null && measttmp.getRankingtable() != null
										&& !mobhsF.get().isEmpty() && measttmp.getRankingtable() > 0) {

									final Vector<Referencecolumn> referencecolumns = new Vector<Referencecolumn>();

									// DC_E_MGW_XXX -> yyy:DIM_E_MGW_XXX_BHTYPE
									final String typeid = newVersionID + ":DIM" + measttmp.getTypename().substring(
													measttmp.getTypename() .indexOf("_")) + "_BHTYPE";

									// BHTYPE

									final Referencecolumn bhtype = new Referencecolumn(dwhRepRock);
									bhtype.setTypeid(typeid);
									bhtype.setDataname("BHTYPE");
									bhtype.setColnumber((long) 1);
									bhtype.setDatatype("varchar");
									bhtype.setDatasize(50);
									bhtype.setDatascale(0);
									bhtype.setUniquevalue((long) 255);
									bhtype.setNullable(1);
									bhtype.setIndexes("HG");
									bhtype.setUniquekey(1);
									bhtype.setIncludesql(1);
									bhtype.setIncludeupd(0);
									bhtype.setColtype(null);
									bhtype.setDescription(null);
									bhtype.setUniverseclass(null);
									bhtype.setUniverseobject(null);
									bhtype.setUniversecondition(null);

									referencecolumns.add(bhtype);

									// Description

									final Referencecolumn desc = new Referencecolumn(dwhRepRock);
									desc.setTypeid(typeid);
									desc.setDataname("DESCRIPTION");
									desc.setColnumber((long) 2);
									desc.setDatatype("varchar");
									desc.setDatasize(80);
									desc.setDatascale(0);
									desc.setUniquevalue((long) 255);
									desc.setNullable(1);
									desc.setIndexes("HG");
									desc.setUniquekey(1);
									desc.setIncludesql(1);
									desc.setIncludeupd(0);
									desc.setColtype(null);
									desc.setDescription(null);
									desc.setUniverseclass(null);
									desc.setUniverseobject(null);
									desc.setUniversecondition(null);
									referencecolumns.add(desc);

									result.put(typeid, referencecolumns);

								}

							} catch (Exception e) {
								log.log(Level.WARNING,
										"Error while retrieving Versioning table", e);
								throw (e);
							}
						}
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
	 * Returns a map of active DWHColumns<br>
	 * Map: StorageID -> Vector of DWHColumns
	 * 
	 * @return .
	 * @throws Exception .
	 */
	private Map<String, Vector<Dwhcolumn>> getActiveDWHColumns()
			throws Exception {
		final Map<String, Vector<Dwhcolumn>> result = new HashMap<String, Vector<Dwhcolumn>>();
		try {
			// Tpactivation
			final Tpactivation tpa = new Tpactivation(dwhRepRock);
			tpa.setStatus(ACTIVE);
			tpa.setTechpack_name(techpackname);

			final TpactivationFactory tpaf = new TpactivationFactory(dwhRepRock, tpa);

			for (Tpactivation tpaftmp : tpaf.get()) {
				try {
					// Dwhtype
					final Dwhtype dtp = new Dwhtype(dwhRepRock);
					dtp.setTechpack_name(tpaftmp.getTechpack_name());
					final DwhtypeFactory dtpf = new DwhtypeFactory(dwhRepRock, dtp);

					Iterator<Dwhtype> inneriter;
					if (this.measurementType != null) {
						// Implies NVU.
						Vector<Dwhtype> tempVector = dtpf.get();
						tempVector = filterExistingTypes(tempVector);
						inneriter = tempVector.iterator();
					} else {
						inneriter = dtpf.get().iterator();
					}

					while (inneriter.hasNext()) {
						try {
							final Dwhtype dtpftmp = inneriter.next();

							// Dwhcolumn
							final Dwhcolumn dwc = new Dwhcolumn(dwhRepRock);
							dwc.setStorageid(dtpftmp.getStorageid());
							final DwhcolumnFactory dwcf = new DwhcolumnFactory(dwhRepRock, dwc);
							final Vector<Dwhcolumn> dwhcolumns = dwcf.get();

							Collections.sort(dwhcolumns, new Comparator<Object>() {
										@Override
										public int compare(final Object o1, final Object o2) {
											final Dwhcolumn d1 = (Dwhcolumn) o1;
											final Dwhcolumn d2 = (Dwhcolumn) o2;

											final Long l1 = d1.getColnumber();
											final Long l2 = d2.getColnumber();

											return l1.compareTo(l2);
										}

										public boolean equals(final Object o1, final Object o2) {
											final Dwhcolumn d1 = (Dwhcolumn) o1;
											final Dwhcolumn d2 = (Dwhcolumn) o2;

											return d1.equals(d2);
										}
									});

							result.put(dtpftmp.getStorageid(), dwhcolumns);

						} catch (Exception e) {
							log.log(Level.WARNING,
									"Error while iterating Dataformat table", e);
							throw (e);
						}
					}

				} catch (Exception e) {
					log.log(Level.WARNING,
							"Error while iterating Versioning table", e);
					throw (e);
				}
			}

		} catch (Exception e) {
			log.log(Level.WARNING, "Error while retrieving Versioning table", e);
			throw (e);
		}

		return result;

	}

	/**
	 * @param type
	 *            .
	 * @return .
	 */
	private boolean isSQLUnsignedNumeric(final String type) {
		return type.equalsIgnoreCase("unsigned bigint")
				|| type.equalsIgnoreCase("unsigned int");
	}

	/**
	 * @param type
	 *            .
	 * @return .
	 */
	private boolean isSQLSignedNumeric(final String type) {

		return type.equalsIgnoreCase("double")
				|| type.equalsIgnoreCase("numeric")
				|| type.equalsIgnoreCase("bigint")
				|| type.equalsIgnoreCase("int")
				|| type.equalsIgnoreCase("smallint")
				|| type.equalsIgnoreCase("tinyint")
				|| type.equalsIgnoreCase("binary")
				|| type.equalsIgnoreCase("real")
				|| type.equalsIgnoreCase("float");

	}

	/**
	 * @param type
	 *            .
	 * @return .
	 */
	private boolean isSQLNumeric(final String type) {

		return type.equalsIgnoreCase("double")
				|| type.equalsIgnoreCase("numeric")
				|| type.equalsIgnoreCase("unsigned bigint")
				|| type.equalsIgnoreCase("bigint")
				|| type.equalsIgnoreCase("unsigned int")
				|| type.equalsIgnoreCase("int")
				|| type.equalsIgnoreCase("smallint")
				|| type.equalsIgnoreCase("tinyint")
				|| type.equalsIgnoreCase("binary")
				|| type.equalsIgnoreCase("real")
				|| type.equalsIgnoreCase("float");

	}

	/**
	 * @param type
	 *            .
	 * @return .
	 */
	private boolean isSQLDate(final String type) {

		return type.equalsIgnoreCase("date")
				|| type.equalsIgnoreCase("datetime");

	}

	/**
	 * @param type
	 *            .
	 * @return .
	 */
	private boolean isSQLCharacter(final String type) {

		return type.equalsIgnoreCase("varchar")
				|| type.equalsIgnoreCase("char");

	}

	/**
	 * @param type
	 *            .
	 * @return .
	 */
	private boolean hasParam(final String type) {

		return type.equalsIgnoreCase("decimal")
				|| type.equalsIgnoreCase("numeric")
				|| type.equalsIgnoreCase("varchar")
				|| type.equalsIgnoreCase("char")
				|| type.equalsIgnoreCase("binary");

	}

	/**
	 * @param type
	 *            .
	 * @return .
	 */
	private boolean hasTwoParams(final String type) {

		return type.equalsIgnoreCase("numeric")
				|| type.equalsIgnoreCase("decimal");

	}

	/**
	 * Verifies column structure of a techpack.
	 * 
	 * @param currentdwh
	 *            .
	 * @param newmeas
	 *            .
	 * @param newref
	 *            .
	 * @throws Exception .
	 */
	private void verifyColumns(final Map<String, Vector<Dwhcolumn>> currentdwh,
			final Map<String, Vector<Measurementcolumn>> newmeas,
			final Map<String, Vector<Referencecolumn>> newref) throws Exception {

		log.info("Checking columns of measurement types...");

		for (String mtableid : newmeas.keySet()) {
			try {
				final String storageID = match(mtableid, storageIDMask);
				log.info("Checking Measurementtable \"" + mtableid
						+ "\" StorageID \"" + storageID + "\"");

				// Columns currently in DWHColumn for this type
				final Vector<Dwhcolumn> currentdwhcolumns = currentdwh
						.get(storageID);

				// Columns of type in new techpack
				final Vector<Measurementcolumn> newcolumns = newmeas
						.get(mtableid);

				if (currentdwhcolumns == null || currentdwhcolumns.size() <= 0) {
					log.fine("New type " + storageID);

					int colno = 1;

					for (Measurementcolumn mcol : newcolumns) {

						try {

							final Dwhcolumn dwhcolum = new Dwhcolumn(dwhRepRock);

							dwhcolum.setStorageid(storageID);
							dwhcolum.setDataname(mcol.getDataname());
							dwhcolum.setColnumber((long) colno++);
							dwhcolum.setDatatype(mcol.getDatatype());
							dwhcolum.setDatasize(mcol.getDatasize());
							dwhcolum.setDatascale(mcol.getDatascale());
							dwhcolum.setUniquevalue(mcol.getUniquevalue());
							dwhcolum.setNullable(mcol.getNullable());
							dwhcolum.setIndexes(mcol.getIndexes());
							dwhcolum.setUniquekey(mcol.getUniquekey());
							dwhcolum.setStatus(ENABLED);
							dwhcolum.setIncludesql(mcol.getIncludesql());

							log.finest("Inserting new DWHColumn "
									+ mcol.getDataname() + "  " + storageID);
							dwhcolum.saveDB();
							log.fine("New column " + mcol.getDataname()
									+ " inserted succesfully to " + storageID);

						} catch (Exception e) {
							log.log(Level.WARNING,
									"Error while creating Dwhcolumn (storageID: "
											+ storageID + ") ", e);
						}

					} // foreach new column

				} else {
					log.fine("Existing type " + storageID);

					long lastexistingcolnumber = (currentdwhcolumns
							.get(currentdwhcolumns.size() - 1)).getColnumber();

					log.finer("Last existing col number is "
							+ lastexistingcolnumber);

					// Iterate existing columns and check that new techpack got
					// them all

					for (Dwhcolumn dcol : currentdwhcolumns) {
						boolean found = false;

						final Iterator<Measurementcolumn> newcolumns_i = newcolumns
								.iterator();

						while (newcolumns_i.hasNext()) {

							final Measurementcolumn mcol = newcolumns_i.next();

							if (mcol.getDataname().equalsIgnoreCase(
									dcol.getDataname())) {

								// Found match. Checking column definition and
								// reporting
								// differences

								boolean altered = false;

								if (!dcol.getDatatype().equalsIgnoreCase(
										mcol.getDatatype())) {
									log.info("Datatype changed from "
											+ dcol.getDatatype() + " to "
											+ mcol.getDatatype());

									// string to numeric not allowed
									if (isSQLCharacter(dcol.getDatatype())
											&& isSQLNumeric(mcol.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ mcol.getDatatype());
									}

									// date to numeric not allowed
									if (isSQLDate(dcol.getDatatype())
											&& isSQLNumeric(mcol.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ mcol.getDatatype());
									}

									// numeric to date not allowed
									if (isSQLNumeric(dcol.getDatatype())
											&& isSQLDate(mcol.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ mcol.getDatatype());
									}

									// signed numeric to unsigned numeric not
									// allowed
									if (isSQLSignedNumeric(dcol.getDatatype())
											&& isSQLUnsignedNumeric(mcol
													.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ mcol.getDatatype());
									}

									dcol.setDatatype(mcol.getDatatype());
									altered = true;
								}

								if (dcol.getDatasize().longValue() != mcol
										.getDatasize().longValue()) {
									log.info("Datasize changed from "
											+ dcol.getDatasize() + " to "
											+ mcol.getDatasize());
									dcol.setDatasize(mcol.getDatasize());
									altered = true;
								}

								if (dcol.getDatascale().longValue() != mcol
										.getDatascale().longValue()) {
									log.info("Datascale changed from "
											+ dcol.getDatascale() + " to "
											+ mcol.getDatascale());
									dcol.setDatascale(mcol.getDatascale());
									altered = true;
								}

								if (dcol.getUniquevalue().longValue() != mcol
										.getUniquevalue().longValue()) {
									log.info("Uniquevalue changed from "
											+ dcol.getUniquevalue() + " to "
											+ mcol.getUniquevalue());
									dcol.setUniquevalue(mcol.getUniquevalue());
									altered = true;
								}
								if (dcol.getNullable().longValue() != mcol
										.getNullable().longValue()) {
									log.warning("Nullable changed from "
											+ dcol.getNullable() + " to "
											+ mcol.getNullable());
									dcol.setNullable(mcol.getNullable());
									altered = true;
								}
								if (!dcol.getIndexes().equalsIgnoreCase(
										mcol.getIndexes())) {
									log.info("Indexes changed from "
											+ dcol.getIndexes() + " to "
											+ mcol.getIndexes());
									dcol.setIndexes(mcol.getIndexes());
									altered = true;
								}

								if (dcol.getUniquekey().longValue() != mcol
										.getUniquekey().longValue()) {
									log.info("Uniquekey changed from "
											+ dcol.getUniquekey() + " to "
											+ mcol.getUniquekey());
									dcol.setUniquekey(mcol.getUniquekey());
									altered = true;
								}

								if (dcol.getIncludesql().longValue() != mcol
										.getIncludesql().longValue()) {
									log.info("Include SQL changed from "
											+ dcol.getIncludesql() + " to "
											+ mcol.getIncludesql());
									dcol.setIncludesql(mcol.getIncludesql());
									altered = true;
								}

								if (altered) {
									log.finest("Saving changes to DWHcolumn");
									dcol.updateDB();
									log.finest("Changes saved succesfully to DWHColumn");
								} else {
									log.finest("No Changes to DWHcolumn");
								}

								found = true;
								newcolumns_i.remove();
								break;

							}

						} // foreach column in new techpack

						if (found) {
							if (!dcol.getStatus().equalsIgnoreCase(ENABLED)) {
								log.info("Column "
										+ dcol.getDataname()
										+ " is again active in techpack. Marking with "
										+ ENABLED);
								dcol.setStatus(ENABLED);
								dcol.updateDB();
								log.finest("Enabled status written");
							} // else column was already ENABLED
						} else {
							if (ebs
									|| !dcol.getStatus().equalsIgnoreCase(
											OBSOLETE)) {

								if (ebs) {
									log.info("Column "
											+ dcol.getDataname()
											+ " no longer in techpack. -> Removing it ");
									dcol.deleteDB();
								} else {
									log.info("Column "
											+ dcol.getDataname()
											+ " no longer in techpack. Marking with "
											+ OBSOLETE);
									dcol.setStatus(OBSOLETE);
									dcol.updateDB();
									log.finest("Obsolete status written");
								}

							} // else column was already OBSOLETE
						}

					} // foreach column in existing techpack

					// If new columns still have elements -> there are new
					// columns

					if (newcolumns.size() > 0) {
						log.info(newcolumns.size() + " new columns for "
								+ storageID);

						for (Measurementcolumn mcol : newcolumns) {

							final Dwhcolumn dwhcolum = new Dwhcolumn(dwhRepRock);

							dwhcolum.setStorageid(storageID);
							dwhcolum.setDataname(mcol.getDataname());
							dwhcolum.setColnumber(++lastexistingcolnumber);
							dwhcolum.setDatatype(mcol.getDatatype());
							dwhcolum.setDatasize(mcol.getDatasize());
							dwhcolum.setDatascale(mcol.getDatascale());
							dwhcolum.setUniquevalue(mcol.getUniquevalue());
							dwhcolum.setNullable(mcol.getNullable());
							dwhcolum.setIndexes(mcol.getIndexes());
							dwhcolum.setUniquekey(mcol.getUniquekey());
							dwhcolum.setStatus(ENABLED);
							dwhcolum.setIncludesql(mcol.getIncludesql());

							log.finest("Inserting new DWHColumn "
									+ mcol.getDataname() + "  " + storageID);
							dwhcolum.saveDB();
							log.fine("New column " + mcol.getDataname()
									+ " inserted succesfully to " + storageID);

						} // foreach new column

					} // if new columns

				} // for exiting type

			} catch (Exception e) {
				log.log(Level.WARNING,
						"verifyColums unrecoverable error on type " + mtableid,
						e);
				throw (e);
			}

		} // For each meastype in new TP

		log.info("Checking columns of reference types...");

		for (String typeid : newref.keySet()) {
			try {
				final String storageID = match(typeid, storageIDMask)
						+ ":PLAIN";

				log.info("Checking Referencetable \"" + typeid
						+ "\" StorageID \"" + storageID + "\"");

				// Columns currently in DWHColumn for this type
				final Vector<Dwhcolumn> currentdwhcolumns = currentdwh
						.get(storageID);

				// Columns of type in new techpack
				final Vector<Referencecolumn> newcolumns = newref.get(typeid);

				if (currentdwhcolumns == null || currentdwhcolumns.size() <= 0) {

					log.fine("New type " + storageID);

					int colno = 1;

					for (Referencecolumn rcol : newcolumns) {

						try {

							final Dwhcolumn dwhcolum = new Dwhcolumn(dwhRepRock);

							dwhcolum.setStorageid(storageID);
							dwhcolum.setDataname(rcol.getDataname());
							dwhcolum.setColnumber((long) colno++);
							dwhcolum.setDatatype(rcol.getDatatype());
							dwhcolum.setDatasize(rcol.getDatasize());
							dwhcolum.setDatascale(rcol.getDatascale());
							dwhcolum.setUniquevalue(rcol.getUniquevalue());
							dwhcolum.setNullable(rcol.getNullable());
							dwhcolum.setIndexes(rcol.getIndexes());
							dwhcolum.setUniquekey(rcol.getUniquekey());
							dwhcolum.setStatus(ENABLED);
							dwhcolum.setIncludesql(rcol.getIncludesql());

							log.finest("Inserting new DWHColumn "
									+ rcol.getDataname() + "  " + storageID);
							dwhcolum.saveDB();
							log.fine("New column " + rcol.getDataname()
									+ " inserted succesfully to " + storageID);

						} catch (Exception e) {
							log.log(Level.WARNING,
									"Error while creating Dwhcolumn (storageID: "
											+ storageID + ") ", e);
							throw (e);
						}

					} // foreach new column

				} else {
					log.fine("Existing type " + storageID);

					long lastexistingcolnumber = (currentdwhcolumns
							.get(currentdwhcolumns.size() - 1)).getColnumber();

					// Iterate existing columns and check that new techpack got
					// them all

					for (Dwhcolumn dcol : currentdwhcolumns) {
						boolean found = false;

						final Iterator<Referencecolumn> newcolumns_i = newcolumns
								.iterator();

						while (newcolumns_i.hasNext()) {

							final Referencecolumn rcol = newcolumns_i.next();
							if (rcol.getDataname().equalsIgnoreCase(
									dcol.getDataname())) {

								// Found match. Checking column definition and
								// reporting
								// differences
								boolean altered = false;

								if (!dcol.getDatatype().equalsIgnoreCase(
										rcol.getDatatype())) {
									log.info("Datatype changed from "
											+ dcol.getDatatype() + " to "
											+ rcol.getDatatype());

									// string to numeric not allowed
									if (isSQLCharacter(dcol.getDatatype())
											&& isSQLNumeric(rcol.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ rcol.getDatatype());
									}

									// date to numeric not allowed
									if (isSQLDate(dcol.getDatatype())
											&& isSQLNumeric(rcol.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ rcol.getDatatype());
									}

									// numeric to date not allowed
									if (isSQLNumeric(dcol.getDatatype())
											&& isSQLDate(rcol.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ rcol.getDatatype());
									}

									// signed numeric to unsigned numeric not
									// allowed
									if (isSQLSignedNumeric(dcol.getDatatype())
											&& isSQLUnsignedNumeric(rcol
													.getDatatype())) {

										throw new Exception(
												"Illegal datatype conversion "
														+ dcol.getDataname()
														+ " "
														+ dcol.getDatatype()
														+ " -> "
														+ rcol.getDatatype());
									}

									dcol.setDatatype(rcol.getDatatype());
									altered = true;
								}

								if (dcol.getDatasize().longValue() != rcol
										.getDatasize().longValue()) {
									log.info("Datasize changed from "
											+ dcol.getDatasize() + " to "
											+ rcol.getDatasize());
									dcol.setDatasize(rcol.getDatasize());
									altered = true;
								}

								if (dcol.getDatascale().longValue() != rcol
										.getDatascale().longValue()) {
									log.info("Datascale changed from "
											+ dcol.getDatascale() + " to "
											+ rcol.getDatascale());
									dcol.setDatascale(rcol.getDatascale());
									altered = true;
								}

								if (dcol.getUniquevalue().longValue() != rcol
										.getUniquevalue().longValue()) {
									log.info("Uniquevalue changed from "
											+ dcol.getUniquevalue() + " to "
											+ rcol.getUniquevalue());
									dcol.setUniquevalue(rcol.getUniquevalue());
									altered = true;
								}

								if (dcol.getNullable().longValue() != rcol
										.getNullable().longValue()) {
									log.info("Nullable changed from "
											+ dcol.getNullable() + " to "
											+ rcol.getNullable());
									dcol.setNullable(rcol.getNullable());
									altered = true;
								}

								if (!dcol.getIndexes().equalsIgnoreCase(
										rcol.getIndexes())) {
									log.info("Indexes changed from "
											+ dcol.getIndexes() + " to "
											+ rcol.getIndexes());
									dcol.setIndexes(rcol.getIndexes());
									altered = true;
								}

								if (dcol.getUniquekey().longValue() != rcol
										.getUniquekey().longValue()) {
									log.info("Uniquekey changed from "
											+ dcol.getUniquekey() + " to "
											+ rcol.getUniquekey());
									dcol.setUniquekey(rcol.getUniquekey());
									altered = true;
								}

								if (dcol.getIncludesql().longValue() != rcol
										.getIncludesql().longValue()) {
									log.info("Include SQL changed from "
											+ dcol.getIncludesql() + " to "
											+ rcol.getIncludesql());
									dcol.setIncludesql(rcol.getIncludesql());
									altered = true;
								}

								if (altered) {
									log.finest("Saving changes to DWHcolumn");
									dcol.updateDB();
									log.finest("Changes saved succesfully to DWHcolumn");
								} else {
									log.finest("No Changes to DWHcolumn");
								}

								found = true;
								newcolumns_i.remove();
								break;

							}

						} // foreach column in new techpack

						if (!found) {

							if (ebs) {

								log.info("Column "
										+ dcol.getDataname()
										+ " no longer in techpack. -> Removing it ");
								dcol.deleteDB();

							} else {

								log.info("Column "
										+ dcol.getDataname()
										+ " no longer in techpack. Marking with "
										+ OBSOLETE);
								dcol.setStatus(OBSOLETE);
								dcol.updateDB();
								log.finest("Obsolete status written");
							}

						}

					} // foreach column in existing techpack

					// If new columns still have elements -> there are new
					// columns

					if (newcolumns.size() > 0) {
						log.info(newcolumns.size() + " new columns for "
								+ storageID);

						for (Referencecolumn rcol : newcolumns) {

							final Dwhcolumn dwhcolum = new Dwhcolumn(dwhRepRock);

							dwhcolum.setStorageid(storageID);
							dwhcolum.setDataname(rcol.getDataname());
							dwhcolum.setColnumber(++lastexistingcolnumber);
							dwhcolum.setDatatype(rcol.getDatatype());
							dwhcolum.setDatasize(rcol.getDatasize());
							dwhcolum.setDatascale(rcol.getDatascale());
							dwhcolum.setUniquevalue(rcol.getUniquevalue());
							dwhcolum.setNullable(rcol.getNullable());
							dwhcolum.setIndexes(rcol.getIndexes());
							dwhcolum.setUniquekey(rcol.getUniquekey());
							dwhcolum.setStatus(ENABLED);
							dwhcolum.setIncludesql(rcol.getIncludesql());

							log.finest("Inserting new DWHColumn "
									+ rcol.getDataname() + "  " + storageID);
							dwhcolum.saveDB();
							log.fine("New column " + rcol.getDataname()
									+ " inserted succesfully to " + storageID);

						} // foreach new column

					} // if new columns

				} // for exiting type

			} catch (Exception e) {
				log.log(Level.WARNING,
						"verifyColums unrecoverable error on type " + typeid, e);
				throw (e);
			}

		} // For each reftype in new TP

		if (TechPackType.EVENTS == techPackType) {
			verifyGroupColumns(currentdwh, newVersionID);
		}
	}

	/**
	 * Verify the Dwhcolumns exist and are up to date for the Grouptypes
	 * 
	 * @param currentdwh
	 *            Current list of Dwhcolumns
	 * @param versionId
	 *            The techpack versionid
	 * @throws Exception
	 *             Any errors.
	 */
	private void verifyGroupColumns(
			final Map<String, Vector<Dwhcolumn>> currentdwh,
			final String versionId) throws Exception {
		if (!GroupTypesCache.areGroupsDefined(versionId)) {
			return;
		}
		final Map<String, GroupTypeDef> gTypes = GroupTypesCache
				.getGrouptypesDef(versionId);
		for (final GroupTypeDef group : gTypes.values()) {
			final String storageID = group.getStorageId();
			final Vector<Dwhcolumn> currentdwhcolumns = currentdwh
					.get(storageID);
			final Collection<GroupTypeKeyDef> newGroupTypeDefKeys = group
					.getKeys();

			if (currentdwhcolumns == null || currentdwhcolumns.isEmpty()) {
				// new group type being added..
				long colno = 1;
				for (GroupTypeKeyDef keyDef : newGroupTypeDefKeys) {
					createNewGroupDwhcolumn(dwhRepRock, keyDef, storageID, colno++);
				}
			} else {
				final List<GroupTypeKeyDef> existGroupTypeDefKey = getExistingGroupKeyCols(
						group, currentdwhcolumns);
				addnewGroupKeyCols(storageID, currentdwhcolumns,
						newGroupTypeDefKeys, existGroupTypeDefKey);
			}
		}
	}

	private void addnewGroupKeyCols(final String storageID,
			final List<Dwhcolumn> currentdwhcolumns,
			final Collection<GroupTypeKeyDef> newGroupTypeDefKeys,
			final List<GroupTypeKeyDef> existGroupTypeDefKey)
			throws SQLException, RockException {
		long lastexistingcolnumber = (currentdwhcolumns.get(currentdwhcolumns
				.size() - 1)).getColnumber();
		for (GroupTypeKeyDef newGroupTypeDefKey : newGroupTypeDefKeys) {
			if (existGroupTypeDefKey.contains(newGroupTypeDefKey)) {
				log.fine("Group Key name : " + newGroupTypeDefKey.getKeyName()
						+ " already exists.");
			} else {
				log.fine("New Group Key name: "
						+ newGroupTypeDefKey.getKeyName());
				createNewGroupDwhcolumn(dwhRepRock, newGroupTypeDefKey, storageID,
						++lastexistingcolnumber);
			}
		}
	}

	private List<GroupTypeKeyDef> getExistingGroupKeyCols(
			final GroupTypeDef group, final List<Dwhcolumn> currentdwhcolumns)
			throws RockException, SQLException {
		final List<GroupTypeKeyDef> existGroupTypeDefKey = new ArrayList<GroupTypeKeyDef>();

		// check existing cols wernt changed....
		for (Dwhcolumn aCol : currentdwhcolumns) {
			try {
				final GroupTypeKeyDef keyDef = group.getKey(aCol.getDataname());
				verifyExistingGroupDwhcolumn(aCol, keyDef);
				if (keyDef != null) {
					existGroupTypeDefKey.add(keyDef);
				}
			} catch (IndexOutOfBoundsException e) {
				log.log(Level.WARNING,
						"Group Key name undefined: " + aCol.getDataname(), e);
			}
		}
		return existGroupTypeDefKey;
	}

	private void verifyExistingGroupDwhcolumn(final Dwhcolumn aCol,
			final GroupTypeKeyDef keyDef) throws RockException, SQLException {
		if (keyDef == null) {
			aCol.setStatus(OBSOLETE);
			aCol.updateDB();
			log.info("Column " + aCol.getDataname()
					+ " no longer in techpack group. Marked as " + OBSOLETE);
			return;
		}
		boolean colChanged = false;
		if (!aCol.getDatatype().equalsIgnoreCase(keyDef.getKeyType())) {
			aCol.setDatatype(keyDef.getKeyType());
			colChanged = true;
		}
		if (aCol.getDatasize() != keyDef.getKeySize()) {
			aCol.setDatasize(keyDef.getKeySize());
			colChanged = true;
		}
		if (aCol.getDatascale() != keyDef.getKeyScale()) {
			aCol.setDatascale(keyDef.getKeyScale());
			colChanged = true;
		}
		if (aCol.getUniquevalue() != keyDef.getKeyUniqueValue()) {
			aCol.setUniquevalue(keyDef.getKeyUniqueValue());
			colChanged = true;
		}
		if (!aCol.getIndexes().equalsIgnoreCase(keyDef.getKeyIndexType())) {
			aCol.setIndexes(keyDef.getKeyIndexType());
			colChanged = true;
		}
		if (aCol.getUniquekey() != keyDef.getKeyUniqueKey()) {
			aCol.setUniquekey(keyDef.getKeyUniqueKey());
			colChanged = true;
		}
		if (aCol.getNullable() != keyDef.isKeyNullable()) {
			aCol.setNullable(keyDef.isKeyNullable());
			colChanged = true;
		}
		if (colChanged) {
			aCol.updateDB();
			log.fine("Existing Group DWHColumn " + aCol.getStorageid() + "::"
					+ keyDef.getKeyName() + " updated");
		} else {
			log.fine("Existing Group DWHColumn " + aCol.getStorageid() + "::"
					+ keyDef.getKeyName() + " unchanged");
		}
	}

	private void createNewGroupDwhcolumn(final RockFactory dwhrep,
			final GroupTypeKeyDef keyDef, final String storageID,
			final long colno) throws SQLException, RockException {
		final Dwhcolumn dwhcolum = new Dwhcolumn(dwhrep);
		dwhcolum.setStorageid(storageID);
		dwhcolum.setDataname(keyDef.getKeyName());
		dwhcolum.setColnumber(colno);
		dwhcolum.setDatatype(keyDef.getKeyType());
		dwhcolum.setDatasize(keyDef.getKeySize());
		dwhcolum.setDatascale(keyDef.getKeyScale());
		dwhcolum.setUniquevalue(keyDef.getKeyUniqueValue());
		dwhcolum.setIndexes(keyDef.getKeyIndexType());
		dwhcolum.setUniquekey(keyDef.getKeyUniqueKey());
		dwhcolum.setStatus(ENABLED);
		dwhcolum.setIncludesql(0);
		dwhcolum.setNullable(keyDef.isKeyNullable());
		log.finest("Inserting new DWHColumn " + keyDef.getKeyName() + "  "
				+ storageID);
		dwhcolum.saveDB();
		log.fine("New DWHColumn " + keyDef.getKeyName()
				+ " inserted succesfully to " + storageID);
	}

	/**
	 * Verify that the Dwhtype and Typeactivation exist and are up to date for
	 * hte Group Management tables
	 * 
	 * @param dwhrep
	 *            Connection to dwhrep
	 * @param versionId
	 *            The twch pack versionid
	 * @param created_types
	 *            List of types newly created, if the DWhtype is created it gets
	 *            added to this list.
	 * @param existing_types
	 *            List of existing Dwhtypes, if the Dwhtype exists for the
	 *            Grouptype, it gets removed from this list
	 * @throws RockException
	 *             Any errors
	 * @throws SQLException
	 *             Any errors
	 */
	private void verifyGroupTypes(final RockFactory dwhrep,
			final String versionId, final Vector<Dwhtype> created_types,
			final Vector<Dwhtype> existing_types) throws RockException,
			SQLException {
		GroupTypesCache.init(dwhRepRock);
		if (!GroupTypesCache.areGroupsDefined(versionId)) {
			// probably a topology techpack!!
			return;
		}
		final Map<String, GroupTypeDef> gTypes = GroupTypesCache
				.getGrouptypesDef(versionId);
		for (final GroupTypeDef group : gTypes.values()) {
			final Dwhtype dwhere = new Dwhtype(dwhrep);
			dwhere.setTypename(group.getTypename());
			final DwhtypeFactory fac = new DwhtypeFactory(dwhrep, dwhere);
			final List<Dwhtype> typeList = fac.get();
			if (typeList.isEmpty()) {
				final Dwhtype newtype = createNewGroupDwhtype(dwhrep, group);
				created_types.add(newtype);
			} else {
				// already existing....
				// typeList should be length 1 when all's going well.....
				for (Dwhtype myType : typeList) {
					for (Dwhtype fType : existing_types) {
						if (myType.getStorageid().equals(fType.getStorageid())
								&& myType.getTablelevel().equals(
										fType.getTablelevel())) {
							existing_types.remove(fType);
							log.fine("Group type " + group.getTypename()
									+ " already defined as "
									+ fType.getStorageid());
							break;
						}
					}
				}
			}
			verifyGroupActivation(group, dwhrep);
		}
	}

	private void verifyGroupActivation(final GroupTypeDef group,
			final RockFactory dwhrep) throws RockException, SQLException {
		// also do the Typeactivaction...
		final Typeactivation where = new Typeactivation(dwhrep);
		where.setTypename(group.getTypename());
		final TypeactivationFactory tfac = new TypeactivationFactory(dwhrep,
				where);
		final List<Typeactivation> ctypes = tfac.get();
		if (ctypes.isEmpty()) {
			final Typeactivation newType = new Typeactivation(dwhrep);
			newType.setTechpack_name(techpackname);
			newType.setStatus(ACTIVE);
			newType.setTypename(group.getTypename());
			newType.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
			newType.setStoragetime(-1L);
			newType.setType("GroupMgt");
			newType.setPartitionplan("medium_plain");
			newType.saveDB();
			log.finer("New GroupType " + group.getTypename() + " activated");
		} else {
			log.finer("Existing GroupType " + group.getTypename()
					+ " already activated");
		}
	}

	private Dwhtype createNewGroupDwhtype(final RockFactory dwhrep,
			final GroupTypeDef group) throws SQLException, RockException {
		final Dwhtype newtype = new Dwhtype(dwhrep);
		newtype.setTechpack_name(group.getTechpackName());
		newtype.setTypename(group.getTypename());
		newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
		newtype.setStorageid(group.getStorageId());
		newtype.setPartitioncount(1L);
		newtype.setStatus(DWHTYPE_STATUS_ENABLED);
		newtype.setOwner(DWHTYPE_OWNER_DC);
		newtype.setViewtemplate("");
		newtype.setCreatetemplate(CREATEPARTITION_VM);
		newtype.setNextpartitiontime(null);
		newtype.setBasetablename(group.getTableName());
		newtype.setDatadatecolumn(null);
		newtype.setPartitionsize(-1L);
		newtype.setType(DWHTYPE_TYPE_SIMPLE);
		newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
		newtype.saveDB();
		log.fine("New group type created " + group.getTypename()
				+ " and inserted succesfully to " + group.getStorageId());
		return newtype;
	}

	/**
	 * @param getTablename
	 *            .
	 * @return .
	 * @throws Exception .
	 */
	protected ArrayList<String> getTablesRealColumnList(
			final String getTablename) throws Exception {

		Statement stmt = null;
		final ArrayList<String> result = new ArrayList<String>();
		ResultSet rSet = null;

		try {

			stmt = dcRock.getConnection().createStatement();
			stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			final StringBuilder query = new StringBuilder(
					"SELECT cname, colno FROM SYS.SYSCOLUMNS WHERE tname='");
			query.append(getTablename).append(
					"' and creator='dc' ORDER BY colno");

			sqlLog.log(Level.FINEST, "sql: " + query.toString());
			final long start = System.currentTimeMillis();
			rSet = stmt.executeQuery(query.toString());
			final long end = System.currentTimeMillis();
			performanceLog.info("getTablesRealColumnList Query:"
					+ query.toString() + " completed in " + (end - start)
					+ " ms");

			if (rSet != null) {
				while (rSet.next()) {

					result.add(rSet.getString("cname").trim());

				}
			}

			return result;

		} catch (Exception e) {
			log.log(Level.WARNING, "Error in getTablesRealColumns", e);
			throw (e);

		} finally {

			try {

				if (rSet != null) {
					rSet.close();
				}

				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				log.log(Level.FINEST, "error closing statement", e);
			}
		}
	}

	/**
	 * @param getTablename
	 *            .
	 * @return .
	 * @throws Exception .
	 */
	protected Map<String, HashMap<String, Object>> getTablesRealColumns(
			final String getTablename) throws Exception {

		final Map<String, HashMap<String, Object>> result = new HashMap<String, HashMap<String, Object>>();
		Statement stmt = null;
		ResultSet rSet = null;
		try {

			stmt = dcRock.getConnection().createStatement();
			stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			final String sql = "SELECT cname, tname, coltype, length, syslength, nulls FROM SYS.SYSCOLUMNS where tname = '"
					+ getTablename + "' and creator='dc' ";
			sqlLog.log(Level.FINEST, "sql: " + sql);
			final long start = System.currentTimeMillis();
			rSet = stmt.executeQuery(sql);
			final long end = System.currentTimeMillis();
			performanceLog.info("getTablesRealColumns Query:" + sql
					+ " completed in " + (end - start) + " ms");

			if (rSet != null) {
				while (rSet.next()) {
					final HashMap<String, Object> tmpMap = new HashMap<String, Object>();
					for (int i = 1; i <= rSet.getMetaData().getColumnCount(); i++) {
						tmpMap.put(rSet.getMetaData().getColumnName(i)
								.toLowerCase(), rSet.getObject(i));// lower case
																	// to hsql
																	// tests
					}
					result.put(rSet.getString("cname").trim(), tmpMap);
				}
			}
			return result;

		} catch (Exception e) {
			log.log(Level.WARNING, "Error in getTablesRealColumns", e);
			throw (e);

		} finally {

			try {
				if (rSet != null) {
					rSet.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				log.log(Level.FINEST, "error closing statement", e);
			}
		}
	}

	/**
	 * @param getTablename
	 *            .
	 * @return .
	 * @throws Exception .
	 */
	private Map<String, HashMap<String, HashMap<String, Object>>> getTablesRealColumnsIndexes(
			final String getTablename) throws Exception {

		final Map<String, HashMap<String, HashMap<String, Object>>> result = new HashMap<String, HashMap<String, HashMap<String, Object>>>();
		Statement stmt = null;
		ResultSet rSet = null;
		try {

			stmt = dcRock.getConnection().createStatement();
			stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);

			// final String sql =
			// "SELECT index_name, index_type, colnames FROM SYS.SYSINDEX as s, SYS.SYSTABLE as t, SYS.SYSINDEXES as i where t.table_name = '"
			// + getTablename +
			// "' and i.tname = t.table_name and s.index_name = i.iname and index_owner = 'USER'";

			final String sql = "SELECT  i.iname, x.index_type, i.colnames FROM SYS.SYSIDX as s, SYS.SYSTAB as t, "
					+ "SYS.SYSINDEXES as i, SYS.SYSIQIDX as x where t.table_name = '"
					+ getTablename
					+ "' and i.tname = t.table_name "
					+ "and s.index_name = i.iname and s.table_id = t.table_id and x.table_id = t.table_id and "
					+ "x.index_id = s.index_id  and x.index_owner = 'USER'";

			sqlLog.log(Level.FINEST, "sql: " + sql);
			final long start = System.currentTimeMillis();
			rSet = stmt.executeQuery(sql);
			final long end = System.currentTimeMillis();
			performanceLog.info("getTablesRealColumnsIndexes Query:" + sql
					+ " completed in " + (end - start) + " ms");

			if (rSet != null) {
				while (rSet.next()) {
					final HashMap<String, Object> tmpMap = new HashMap<String, Object>();
					for (int i = 1; i <= rSet.getMetaData().getColumnCount(); i++) {
						tmpMap.put(rSet.getMetaData().getColumnName(i),
								rSet.getObject(i));
					}

					final String keyStr = rSet.getString("colnames").trim();

					// remove the ASC string from key
					final String key = keyStr.substring(0,
							keyStr.lastIndexOf("ASC")).trim();

					if (!result.containsKey(key)) {
						result.put(key,
								new HashMap<String, HashMap<String, Object>>());
					}

					result.get(key).put(rSet.getString("index_type").trim(),
							tmpMap);

				}
			}
			return result;

		} catch (Exception e) {
			log.log(Level.WARNING, "Error in getTablesRealColumns", e);
			throw (e);

		} finally {

			try {
				if (rSet != null) {
					rSet.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				log.log(Level.FINEST, "error closing statement", e);
			}
		}
	}

	/**
	 * @param sql
	 *            .
	 * @throws Exception .
	 */
	private void executeSQL(final String sql) throws Exception {

		Statement stmt = null;

		try {

			//if (forceCheck){
				//stmt = dbConnection.getConnection().createStatement();
			//} else {
				stmt = dcRock.getConnection().createStatement();
			//}
			stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
			sqlLog.log(Level.INFO, "sql: " + sql);
			stmt.executeUpdate(sql);

		} catch (Exception e) {
			log.log(Level.WARNING, "Error in executeSQL", e);
			throw (e);

		} finally {

			try {
				if (stmt != null) {
					while (stmt.getMoreResults()) {
						stmt.getResultSet().close();
					}
					stmt.close();
				}
			} catch (Exception e) {
				log.log(Level.FINEST, "error closing statement", e);
			}
		}
	}

	protected String getTableSearchFilter(final String tableName) {
		return "select count(table_id) as tableCount from SYS.SYSTABLE where table_type = 'BASE' and table_name = '"
				+ tableName + "'";
	}

	/**
	 * 
	 * @param tablename
	 *            The table name to check the existance of.
	 * @return true if partition named tablename exists.
	 * @throws Exception
	 *             errors if the systable query fails or no results returned
	 */
	protected boolean checkRealPartitions(final String tablename)
			throws Exception {
		log.log(Level.FINEST, "Checking if partition " + tablename
				+ " exists or not");
		final RockFactory rf = getDwhDbaConnection();
		final String toGet = "tableCount";
		final String sql = getTableSearchFilter(tablename);
		sqlLog.log(Level.FINEST, "sql: " + sql);
		log.log(Level.FINEST, "Checking sys for table existance");
		final Connection c = rf.getConnection();
		final Statement stmt = c.createStatement();
		stmt.setQueryTimeout(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS);
		ResultSet rs = null;
		try {
			final long start = System.currentTimeMillis();
			rs = stmt.executeQuery(sql);
			final long end = System.currentTimeMillis();
			performanceLog.info("checkRealPartitions Query:" + sql
					+ " completed in " + (end - start) + " ms");
			if (rs.next()) {
				final int tCount = Integer.parseInt(rs.getString(toGet));
				log.log(Level.FINEST, "Table " + tablename + " exists is "
						+ tCount);
				return tCount == 1;
			} else {
				throw new Exception("Couldn't get table count for " + tablename
						+ " no results returned?");
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Throwable t) {/**/
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Throwable t) {/**/
				}
			}
			c.close();
		}
	}

	/**
	 * Get the ETLREP db connection details. Doing it this way so as not to
	 * change the constructor which would effect Engine. Looks for
	 * System.getProperty("CONF_DIR") + "/ETLCServer.properties"; Returned map
	 * contains the following keys: url --> database url user --> user name pass
	 * --> password driver -- jdbc driver to use
	 * 
	 * @return Map containing the connection details to create a RockFactory for
	 *         etlrep
	 * @throws Exception
	 *             If there are any errors..
	 */
	protected Map<String, String> getEtlRepConnectionDetails() throws Exception {
		final String propertiesFile = System.getProperty("CONF_DIR",
				"/eniq/sw/conf") + "/ETLCServer.properties";
		log.log(Level.FINEST, "Looking up " + propertiesFile
				+ " for meta connection details");
		final File propFile = new File(propertiesFile);
		if (!propFile.exists()) {
			throw new Exception("Failed to load " + propertiesFile);
		}
		@SuppressWarnings({ "MismatchedQueryAndUpdateOfCollection" })
		final Properties connProps = new ETLCServerProperties(propertiesFile);
		final Map<String, String> cp = new HashMap<String, String>(6);
		cp.put("url", connProps.getProperty("ENGINE_DB_URL"));
		cp.put("user", connProps.getProperty("ENGINE_DB_USERNAME"));
		cp.put("pass", connProps.getProperty("ENGINE_DB_PASSWORD"));
		cp.put("driver", connProps.getProperty("ENGINE_DB_DRIVERNAME"));
		return cp;
	}

	private RockFactory getDwhDbaConnection() throws Exception {
		final Map<String, String> connectionProps = getEtlRepConnectionDetails();
		final RockFactory etlrepConn = new RockFactory(
				connectionProps.get("url"), connectionProps.get("user"),
				connectionProps.get("pass"), connectionProps.get("driver"),
				"VUA-GetDbaAccess", true);
		log.log(Level.FINEST, "Looking up DBA access to dwhdb");
		final Meta_databases where = new Meta_databases(etlrepConn);
		where.setType_name("DBA");
		where.setConnection_name("dwh");
		final Meta_databasesFactory fac = new Meta_databasesFactory(etlrepConn,
				where);
		final Vector v = fac.get();
		if (v.isEmpty()) {
			throw new Exception("No DBA access found?");
		}
		final Meta_databases dwhDba = (Meta_databases) v.get(0);
		log.log(Level.FINEST, "Found access.");
		final RockFactory etl = new RockFactory(dwhDba.getConnection_string(),
				dwhDba.getUsername(), dwhDba.getPassword(),
				dwhDba.getDriver_name(), "checkRealPartitions", true);
		etlrepConn.getConnection().close();
		return etl;
	}

	/**
	 * @param str
	 *            .
	 * @return .
	 */
	private String alias(final String str) {

		if (str.equalsIgnoreCase("datetime")) {
			return "timestamp";
		}

		if (str.equalsIgnoreCase("int")) {
			return "integer";
		}

		return str;

	}

	/**
	 * @param realMap
	 *            .
	 * @param table
	 *            .
	 * @return .
	 */
	private String dropIndex(
			final HashMap<String, HashMap<String, Object>> realMap,
			final String table) {

		String sql = "";
		for (String key : realMap.keySet()) {

			final HashMap<String, Object> iMap = realMap.get(key);
			sql += " DROP INDEX " + this.dcUser + '.' + table + '.'
					+ ((String) iMap.get("iname")).trim() + "; ";
		}

		return sql;

	}

	// code changes for TR HR23292

	private RockFactory connectEtlrep() {
		try {
			final Map<String, String> connectionProps = getEtlRepConnectionDetails();
			etlRepRock = new RockFactory(connectionProps.get("url"),
					connectionProps.get("user"), connectionProps.get("pass"),
					connectionProps.get("driver"), "GetETLRepAccess", true);
		} catch (Exception e) {
			log.warning("Cannot create Connection to ETL Repository.");
		}

		return etlRepRock;
	}

	/**
	 * Gets the User "dc" from the meta_databases table in the ETL Repository
	 * 
	 * @return user "dc"
	 */
	private String getDCUser() {
		final Meta_databases dcUser = new Meta_databases(etlRepRock);
		dcUser.setType_name("USER");
		dcUser.setConnection_name("dwh");
		String dcUserName = "";
		try {
			final Meta_databasesFactory metaDbFacory = new Meta_databasesFactory(
					etlRepRock, dcUser);
			final Vector<Meta_databases> vecUsers = metaDbFacory.get();
			if (vecUsers.isEmpty()) {
				log.warning("User DC Not Found");
			}
			final Meta_databases dwhDc = vecUsers.get(0);
			dcUserName = dwhDc.getUsername();
		} catch (Exception e) {
			log.warning("User DC Not Found");
		}
		return dcUserName;
	}

	private String getPassword(final String service, final String user) {
		final Meta_databases dcUser = new Meta_databases(etlRepRock);
		dcUser.setType_name(user);
		dcUser.setUsername(user);
		dcUser.setConnection_name(service);
		try {
			final Meta_databasesFactory metaDbFacory = new Meta_databasesFactory(
					etlRepRock, dcUser);
			final Vector<Meta_databases> vecUsers = metaDbFacory.get();
			if (vecUsers.isEmpty()) {
				log.warning("User " + user + " Not Found");
			}
			final Meta_databases dwhDc = vecUsers.get(0);
			return dwhDc.getPassword();
		} catch (Exception e) {
			log.warning("User " + user + " Not Found");
		}
		return null;
	}

	/**
	 * @param table
	 *            .
	 * @param column
	 *            .
	 * @param newIndexes
	 *            .
	 * @return .
	 */
	private String createIndex(final String table, final String column,
			final List<String> newIndexes) {

		String sql = "";

		if (!newIndexes.isEmpty()) {

			for (String indx : newIndexes) {

				final String indexname = table + "_" + column + "_" + indx;

				// and create a new index.
				sql += " CREATE " + indx + " INDEX " + indexname + " ON "
						+ table + " (" + column + "); ";
			}
		}

		return sql;

	}

	/**
	 * @param index
	 *            .
	 * @return .
	 */
	private ArrayList<String> getIndexList(final String index) {

		final ArrayList<String> newIndexes = new ArrayList<String>();

		if (index.length() > 0) {
			final String[] tmp = index.split(",");
			// loop new indexes
			newIndexes.addAll(Arrays.asList(tmp));
		}

		return newIndexes;
	}

	/**
	 * @throws Exception .
	 */
	private void alterPartitions() throws Exception {
		try {
			// check if techpacktype is not EVENTS type
			if (TechPackType.STATS == techPackType) {
				// check if VersionUpdateAction is called by EBSUpdater or not
				/*if (!forceCheck) {
					dbConnection = dcRock;
				} else {
					dbConnection = getDwhDbaConnection();
				}*/

				try {
					// Remove all views
					long dropST = System.currentTimeMillis();
					ForceDropView fd = new ForceDropView(techpackname, log);
					fd.execute();
					log.info("Total time taken to drop views is " + (System.currentTimeMillis() - dropST) + " ms. ");
				} catch (Exception s) {
					log.log(Level.SEVERE, "Not all views have been dropped. Check for remaining views of " +techpackname);
					throw (new Exception("Not all views have been dropped. Check for remaining views of " +techpackname));
				}
				
				try {
					// lock all the partitions
					long lockST = System.currentTimeMillis();
					LockTable locktab = new LockTable(dwhRepRock, dcRock, log);
					locktab.lockPartition(techpackname);
					log.info("Total time taken to acquire lock is " + (System.currentTimeMillis() - lockST) + " ms. ");
				} catch (Exception s) {
					log.log(Level.WARNING, "Error while locking tables", s.getMessage());
					throw (new Exception("Error while locking tables " + s.getMessage()));
				}
			}
			long start = System.currentTimeMillis();
			final List<Dwhpartition> partitionList = getDWHPartitions();
			log.log(Level.INFO, "Need to check " + partitionList.size() + " partitions for alterations...");
			
			final Iterator<Dwhpartition> iter = partitionList.iterator();
			final ArrayList<String> allIndexes = new ArrayList<String>();
			long total_store_time = 0;
			boolean runUnloadTable = false;

			// loop all active techpack partitions.
			while (iter.hasNext()) {
				boolean altered = false;
				final Dwhpartition dwhTmp = iter.next();
				// Dwhcolumn
				final Dwhcolumn dwc = new Dwhcolumn(dwhRepRock);
				dwc.setStorageid(dwhTmp.getStorageid());
				final DwhcolumnFactory dwcf = new DwhcolumnFactory(dwhRepRock, dwc, "order by COLNUMBER");
				if (!checkRealPartitions(dwhTmp.getTablename())) {
					// partition does not exists
					log.log(Level.WARNING, "Partition " + dwhTmp.getTablename()
							+ " does not exists, removing row from DWHPartition and subtracting one from DWHType partition count.");

					final Dwhpartition removeDWHPartition = new Dwhpartition(dwhRepRock);
					removeDWHPartition.setTablename(dwhTmp.getTablename());
					removeDWHPartition.deleteDB();
					log.log(Level.FINE, dwhTmp.getTablename() + " removed from DWHPartition");

					final Dwhtype subDWHtypePartitionCount = new Dwhtype(dwhRepRock);
					subDWHtypePartitionCount.setStorageid(dwhTmp.getStorageid());
					final DwhtypeFactory dwhtf = new DwhtypeFactory(dwhRepRock, subDWHtypePartitionCount);
					final Dwhtype dwhttmp = (dwhtf.get()).get(0);
					final int count = dwhttmp.getPartitioncount().intValue() - 1;
					dwhttmp.setPartitioncount((long) count);
					dwhttmp.updateDB(); // HK79178
					log.log(Level.FINE, dwhTmp.getStorageid() + " partition count changed to " + count);
				} else {
					// partition exist
					// get partitions real columns
					final TableAlterDetails tad = new TableAlterDetails(dwhTmp.getTablename(), MAX_COLS_PER_STATEMENT);
					log.log(Level.FINER, "Generating altering table info for " + tad.getTableName());
					final Map<String, HashMap<String, Object>> realColumnsMap = getTablesRealColumns(dwhTmp.getTablename());
					
					// get partitions real indexes
					final Map<String, HashMap<String, HashMap<String, Object>>> realIndexes = getTablesRealColumnsIndexes(dwhTmp.getTablename());

					if (dwcf != null) {
						// loop columns in DWHColumns with specific storageID
						for (Dwhcolumn dwhcolum : dwcf.get()) {
							// does the DWHCOLUMN (dataname) exists in realColumns
							if (!realColumnsMap.keySet().contains(dwhcolum.getDataname())) {
								altered = alterTableAddColumn(allIndexes, altered, dwhTmp, dwhcolum, tad);
							} else {
								altered = alterTableAlterColumn(allIndexes, altered, realColumnsMap, realIndexes, dwhcolum, tad);
							}
						}
						altered = alterTableDeleteColumn(altered, realColumnsMap, tad);
					}

					log.log(Level.FINER, "Need to alter " + tad.getTotalAlterCount() + " columns in " + tad.getTableName());
					final long startA = System.currentTimeMillis();
					final Iterator<String> sql = tad.getIterator();
					while (sql.hasNext()) {
						final String stmt = sql.next();
						executeSQL(stmt);
					}

					final long stopA = System.currentTimeMillis();
					if (tad.getTotalAlterCount() > 0) {
						// since partition changed, unload table from cache
						runUnloadTable = true;
						final long alterTime = stopA - startA;
						performanceLog.info("Table " + tad.getTableName() + " alteration for " + tad.getTotalAlterCount()
								+ " columns done in " + alterTime + " ms");
					}
				}
				// execute alteration sqls for this partition.
				// re-order colnumbers in DWHCOLUM to match real tables col
				// order if table has been altered.

				if (altered) {
					final ArrayList<String> list = getTablesRealColumnList(dwhTmp.getTablename());
					if (!list.isEmpty() && dwcf != null) {
						for (Dwhcolumn dwhcolum : dwcf.get()) {
							final int i = list.indexOf(dwhcolum.getDataname());
							if (i >= 0) {
								dwhcolum.setColnumber((long) i);
								dwhcolum.updateDB();
							}
						}
					}
				}
				
				if (runUnloadTable){
					// Call store procedure to release catalog cache
					if (dcRock != null) {
						long store_start_time = System.currentTimeMillis();
						final Connection con = dcRock.getConnection();
						Statement stmnt = con.createStatement();
						String cacheProcSql = "call cache_proc_user.sp_unload_table_from_cache('DC','"
								+ dwhTmp.getTablename() + "');";
						ResultSet res = null;
						try {
							log.log(Level.FINE, "Execute store procedure using username : "
											+ con.getMetaData().getUserName() + " and SQL : " + cacheProcSql);
							res = stmnt.executeQuery(cacheProcSql);
						} catch (SQLException esc) {
							log.warning("Exception while running sp_unload_table_from_cache store procedure. " + esc);
							esc.getMessage();
						}finally {
							try {
								if (res != null) {
									res.close();
								} else {
									log.fine("Res is null");
								}
								if (stmnt != null) {
									stmnt.close();
								}
							} catch (Exception e) {
								log.log(Level.WARNING, "Error closing sp_unload_table_from_cache store procedure ", e.getMessage());
							}
						}
						total_store_time += (System.currentTimeMillis() - store_start_time);
					} else {
						log.log(Level.WARNING, "Error during alter partition. " +
								"Cannot execute store procedure (sp_unload_table_from_cache) to release catalog cache as connection is NULL. ");
					}
					runUnloadTable = false;
				}
				
			}

			performanceLog.fine("Columns added and altered in " + (start - System.currentTimeMillis()) + " ms");
			performanceLog.info("Total time spend to call store procedure (sp_unload_table_from_cache) is "
					+ total_store_time + " ms");

			// execute all index creations in parallel for this Tech Pack.
			start = System.currentTimeMillis();
			final int ais = allIndexes.size();

			if (ais > 0) {
				int cpuCores = getNumberOfPhysicalCPUs();
				if (cpuCores == -1) {
					log.warning("Could not get CPU Core count, defaulting to a value of 1");
					// -1 gets returned if the /usr/sbin/psrinfo executable isnt
					// found, chances are
					// this is being run via tpide ona windows machine
					cpuCores = 1;
				}

				while (allIndexes.size() > 0) {

					final StringBuilder sql = new StringBuilder();
					sql.append("BEGIN PARALLEL IQ\n");

					int i = 0;
					while ((allIndexes.size() > 0) && (i < cpuCores)) {
						final String indexStr = allIndexes.remove(0);
						sql.append(indexStr).append("\n");
						i++;
					}

					sql.append("END PARALLEL IQ;\n");
					log.log(Level.FINEST, "Index creation SQL: " + sql.toString());
					executeSQL(sql.toString());
				}

				performanceLog.fine("Total " + ais + " index changes performed in "
						+ (start - System.currentTimeMillis()) + " ms");
			}

		} catch (Exception e) {
			log.log(Level.WARNING, "Error in alterPartitions", e);
			throw (e);
		}
	}

	/**
	 * Get the number of cores that are on the host the database is running on
	 * 
	 * @return database host core count
	 * @throws Exception
	 *             No host info avaiable to other database query errors.
	 */
	int getNumberOfPhysicalCPUs() throws Exception {
		final String regex = "jdbc:sqlanywhere:host=(.*?):.*";
		final Pattern p = Pattern.compile(regex);
		final Matcher m = p.matcher(this.dcRock.getDbURL());
		final String dbName;
		if (m.matches()) {
			dbName = m.group(1);
		} else {
			log.fine("Could not extract database host from dwhdb URL "
					+ this.dcRock.getDbURL());
			dbName = "dwhdb";
		}

		final Map<String, ServicenamesHelper.ServiceHostDetails> services = ServicenamesHelper
				.getServiceDetails();
		ServicenamesHelper.ServiceHostDetails dwhdbService = null;
		if (services.containsKey(dbName)) {
			// The jdbc url has a service name in it
			dwhdbService = services.get(dbName);
		} else {
			// The jdbc name is not a servicename, assume its a physical
			// hostname or ip address..
			for (ServicenamesHelper.ServiceHostDetails entry : services
					.values()) {
				if (entry.getServiceHostname().equalsIgnoreCase(dbName)
						|| entry.getServiceIpAddress().equals(dbName)) {
					// get any serivce that matches the host/ip, just need the
					// username & password for it...
					dwhdbService = entry;
					break;
				}
			}
			if (dwhdbService == null) {
				// shouldnt really happen??
				throw new Exception("No host information for '" + dbName
						+ "' available in service_names");
			}
		}
		final String dwhdbIpAddress = dwhdbService.getServiceIpAddress();
		final String localIpAddress = InetAddress.getLocalHost()
				.getHostAddress();

		if (localIpAddress.equals(dwhdbIpAddress)) {
			// On the same host as dwhdb
			return DefaultLicensingCache.getNumberOfPhysicalCPUs(false/*
																	 * Use the
																	 * lwp
																	 * helper if
																	 * available
																	 * i.e dont
																	 * spawn the
																	 * process
																	 * from
																	 * engines
																	 * JVM
																	 */);
		} else {
			// dwhdb and this jvm are on different hosts, need to get the core
			// count from a remote host..
			final ServicenamesHelper serviceNameHelper = ServicenamesHelperFactory
					.getInstance();
			final String pathSeparator = System
					.getProperty("java.path.separator");
			String password = null;
			if (!":".equals(pathSeparator)) {
				// password is only used when called via TPIDE
				password = getPassword(dwhdbService.getServiceName(), "dcuser");
			}
			return serviceNameHelper.getServiceHostCpuCount(dwhdbService,
					"dcuser", password);
		}
	}

	/**
	 * @param allIndexes
	 *            .
	 * @param altered
	 *            .
	 * @param realColumnsMap
	 *            .
	 * @param realIndexes
	 *            .
	 * @param dwhcolum
	 *            .
	 * @param tad
	 *            .
	 * @return .
	 * @throws Exception .
	 */
	protected boolean alterTableAlterColumn(
			final ArrayList<String> allIndexes,
			boolean altered,
			final Map<String, HashMap<String, Object>> realColumnsMap,
			final Map<String, HashMap<String, HashMap<String, Object>>> realIndexes,
			final Dwhcolumn dwhcolum, final TableAlterDetails tad)
			throws Exception {
		// old column
		// retrieve the realcolumn from realcolumns

		boolean columnAltered = false;
		boolean indexdropped = false;

		final Map<String, Object> realColumn = realColumnsMap.get(dwhcolum
				.getDataname());

		String sql = "";
		String sqlnew = "";
		final String table = ((String) realColumn.get("tname")).trim();
		final String column = dwhcolum.getDataname();
		final String datatype = dwhcolum.getDatatype();
		final String datasize = dwhcolum.getDatasize().toString();
		// String datascale = dwhcolum.getDatascale().toString();
		final String index = dwhcolum.getIndexes();
		final String nullable = "null";
		final int dataSize = getValueFromRealColumn(realColumn, "length");

		log.log(Level.FINEST, "DWHTableName:" + table);
		log.log(Level.FINEST, "DWHDataname:" + column);
		log.log(Level.FINEST, "DWHDatatype:" + datatype + "  REALDatatype:"
				+ ((String) realColumn.get("coltype")).trim());
		log.log(Level.FINEST, "DWHDatasize:" + datasize + "  REALDatasize:"
				+ dataSize);
		log.log(Level.FINEST,
				"DWHDatascale:" + dwhcolum.getDatascale() + "  REALDatascale:"
						+ getValueFromRealColumn(realColumn, "syslength"));

		log.log(Level.FINEST, "DWHIndexes:" + index + "  REALIndexes:"
				+ realIndexes.get(column));

		// check is the type changed
		if (!alias(dwhcolum.getDatatype()).equalsIgnoreCase(
				((String) realColumn.get("coltype")).trim())) {

			// check if datasize(length) needs changing too.
			String value = "";

			if (hasParam(dwhcolum.getDatatype())) {

				value += "(" + dwhcolum.getDatasize();

				// if numeric and decimal add datascale
				if (hasTwoParams(dwhcolum.getDatatype())) {
					value += "," + dwhcolum.getDatascale();
				}

				value += ")";
			}

			sql = " add tmp_" + column + " " + datatype + value + " "
					+ nullable + "; " + "update " + table + " as a set tmp_"
					+ column + " = cast(a." + column + " as " + datatype
					+ "); " + "alter table " + table + " delete " + column
					+ "; " + "alter table " + table + " RENAME  tmp_" + column
					+ " TO " + column;
			log.log(Level.FINEST, "change datatype " + sql);
			columnAltered = true;

		} else
		// datatype did not change but
		// check is the length or scale changed.
		if (hasParam(datatype)) {
			String value = "";

			// DWHManager Query :
			// Case 1: Both Datasize and Datascale changed

			if (hasTwoParams(dwhcolum.getDatatype()) &&
			// (dwhcolum.getDatasize().intValue() !=
			// convertObjectToInteger(realColumn.get("length")))
					(dwhcolum.getDatasize() != getValueFromRealColumn(
							realColumn, "length"))
					&& dwhcolum.getDatascale().intValue() != convertObjectToInteger(realColumn
							.get("syslength"))) {

				value += "(" + dwhcolum.getDatasize() + ","
						+ dwhcolum.getDatascale();
				log.info("Both datasize and datascale changed ("
						+ dwhcolum.getDatasize() + ","
						+ dwhcolum.getDatascale() + ")");
			}

			// Case 2 : datasize has changed

			else if (hasTwoParams(dwhcolum.getDatatype())
					&& (dwhcolum.getDatasize().intValue() != convertObjectToInteger(realColumn
							.get("length")))
					&& dwhcolum.getDatascale().intValue() == convertObjectToInteger(realColumn
							.get("syslength"))) {

				value += "(" + dwhcolum.getDatasize() + ","
						+ convertObjectToInteger(realColumn.get("syslength"));
				log.info("DataSize Only changed. Retaining same datascale(datacolumn size:"
						+ dwhcolum.getDatasize().intValue()
						+ ",convertObject:"
						+ convertObjectToInteger(realColumn.get("length"))
						+ ",dwhcolum.getDatascale:"
						+ dwhcolum.getDatascale().intValue()
						+ ",convertObjectTo:"
						+ convertObjectToInteger(realColumn.get("syslength")));
				// log.info("Datasize Only changed .Retaining same datascale ("+dwhcolum.getDatasize()+","+convertObjectToInteger(realColumn.get("syslength"))+")");
			}

			// Case 3: datascale has changed (only for numeric)

			else if (hasTwoParams(dwhcolum.getDatatype())
					&& (dwhcolum.getDatasize().intValue() == convertObjectToInteger(realColumn
							.get("length")))
					&& dwhcolum.getDatascale().intValue() != convertObjectToInteger(realColumn
							.get("syslength"))) {

				value += "(" + convertObjectToInteger(realColumn.get("length"))
						+ "," + dwhcolum.getDatascale().intValue();
				log.info("Datascale Only changed. Retaining same datasize(datacolumn size:"
						+ dwhcolum.getDatasize().intValue()
						+ ",convertObject:"
						+ convertObjectToInteger(realColumn.get("length"))
						+ ",dwhcolum.getDatascale:"
						+ dwhcolum.getDatascale().intValue()
						+ ",convertObjectTo:"
						+ convertObjectToInteger(realColumn.get("syslength")));
				// log.info("Datasize Only changed .Retaining same datascale ("+convertObjectToInteger(realColumn.get("length"))+","+dwhcolum.getDatascale().intValue()+")");
			} else { // check if size has changed (for single param types e.g.
						// binary)

				if ((dwhcolum.getDatasize().intValue() > convertObjectToInteger(realColumn
						.get("length")))) {
					value += "(" + dwhcolum.getDatasize();
					log.info("Datasize changed (" + dwhcolum.getDatasize()
							+ ")");
				}

			}

			// if value is empty nothing changed so no need to alter
			// anything.

			if (value.length() > 0) {

				value += ")";

				sql = " add tmp_" + column + " " + datatype + value + " "
						+ nullable + "; " + "update " + table
						+ " as a set tmp_" + column + " = cast(a." + column
						+ " as " + datatype + value + "); " + "alter table "
						+ table + " delete " + column + "; " + "alter table "
						+ table + " RENAME  tmp_" + column + " TO " + column;
				log.log(Level.FINEST, "Change datasize and scale " + sql);
				columnAltered = true;
			}

		}

		// contains indexes that should be in this column
		final ArrayList<String> newIndexes = getIndexList(index);

		if (realIndexes.containsKey(column) && newIndexes.isEmpty()) {
			// there is real indexes for this column but there should not
			// be (new indexes is empty) -> remove all
			// real indexes

			// column is altered so all indexes for this colun is dropped,
			// no need to drop anything.
			if (!columnAltered) {
				final HashMap<String, HashMap<String, Object>> realMap = realIndexes
						.get(column);
				sqlnew += dropIndex(realMap, table);
				log.log(Level.FINEST, "DWHDataname:" + column
						+ " not altered. Drop Index statement: " + sqlnew);
				indexdropped = true;
			}

		} else { // if (realIndexes.containsKey(column) && newIndexes.isEmpty())
			if (!newIndexes.isEmpty() && !columnAltered) {
				// there is indexses both in real indexes and in new indexes
				// and column is not altered, inspect more
				if (realIndexes.containsKey(column)) {
					final HashMap<String, HashMap<String, Object>> realMap = realIndexes
							.get(column);
					final Iterator<String> nIter = newIndexes.iterator();

					// loop new indexes
					while (nIter.hasNext()) {

						final String indx = nIter.next();

						if (realMap.containsKey(indx)) {
							// real list contains this new index (no change) ->
							// remove from list
							realMap.remove(indx);
							nIter.remove();
						}
					} // foreach newIndex

					// after going through all new indexes..
					// is there any real indexses left -> remove them
					sqlnew += dropIndex(realMap, table);
					log.log(Level.FINEST, "DWHDataname:" + column
							+ " altered: " + columnAltered
							+ ", Drop Index statement: " + sqlnew);
					indexdropped = true;
					// is there any new indexses left -> add them
					// >> Parallel execution
					final String tmpSql = createIndex(table, column, newIndexes);
					if (tmpSql.length() > 0) {
						allIndexes.add(tmpSql);
					}
					// >> Parallel execution
					log.log(Level.FINEST, "Create Index " + tmpSql);

				} else {
					// there are no real indexes but there are new indexes so
					// lets create the missing indexes.
					// is there any new indexses left -> add them
					// >> Parallel execution
					final String tmpSql = createIndex(table, column, newIndexes);
					if (tmpSql.length() > 0) {
						allIndexes.add(tmpSql);
					}
					// << Parallel execution
					log.log(Level.FINEST, "Create Index " + tmpSql);

				}
			} else { // if (!newIndexes.isEmpty() && !columnAltered)
				// column was altered so all old index are dropped just
				// create the index in new indexes.
				// >> Parallel execution
				final String tmpSql = createIndex(table, column, newIndexes);
				if (tmpSql.length() > 0) {
					allIndexes.add(tmpSql);
				}
				// >> Parallel execution
				log.log(Level.FINEST, "Create Index " + tmpSql);

			}
		}

		if (!columnAltered) {
			// Check if column has changed from "NOT NULLABLE" to "NULLABLE"
			if (((String) realColumn.get("nulls")).equalsIgnoreCase("N")
					&& dwhcolum.getNullable() == 1) {
				sql += " modify " + column + " null; ";
				columnAltered = true;
			}
		}

		// is there a sql clause to execute.
		if ((sql != null && sql.length() > 0)
				|| (sqlnew != null && sqlnew.length() > 0)) {
			try {

				log.log(Level.INFO, "Altering column " + column + " in table "
						+ table);

				// executeSQL(sql);

				if (columnAltered) {
					if ((indexdropped)
							&& ((sqlnew != null) && (sqlnew.length() > 0))) {

						log.log(Level.FINEST, "Column index for " + column
								+ " altered: " + indexdropped + " in table "
								+ table + ". SQLNEW: \n" + sqlnew);
						tad.alterOnlyIndex(sqlnew);
					}
					if ((sql != null) && (sql.length() > 0)) {
						log.log(Level.FINEST, "Column " + column + " altered: "
								+ columnAltered + " in table " + table
								+ ". SQL: \n" + sql);
						tad.alterExistingColumn(sql);
					}
				}
				/*
				 * if columnAltered) { // case when column definition changed //
				 * add alter table <table name> at the beginning of SQL
				 * statement tad.alterExistingColumn(sql);
				 */
				else {
					// case when only index definition changed
					// do not add anything at the beginning of SQL statement
					if ((sqlnew != null) && (sqlnew.length() > 0)) {
						log.log(Level.FINEST, "Column index for " + column
								+ " altered: " + indexdropped + " in table "
								+ table + ". SQLNEW: \n" + sqlnew);
						tad.alterOnlyIndex(sqlnew);
					}
				}
				altered = true;
			} catch (Exception e) {
				log.log(Level.WARNING,
						"Error in alterPartitions:alterTableAlterColumn while altering table: "
								+ table + " / column: " + column + " ", e);
				throw (e);
			}
		} else {
			log.log(Level.FINEST,
					"No alterations done for column " + dwhcolum.getDataname());
		}

		// remove checked realcol from realcolumns
		realColumnsMap.remove(dwhcolum.getDataname());
		return altered;
	} // alterTableAlterColumn

	/**
	 * Needed to update this handling to cover both sybase 12 and sybase 15 In
	 * sybase 12, the field length in the sys.syscolumns table is stored as a
	 * smallint, which is mapped to a Integer in Java
	 * 
	 * In sybase 15, its stored as an unsigned int, which is mapped to a
	 * BigDecimal in Java
	 * 
	 * Both BigDecimal and Integer extend the java class Number, so can safely
	 * cast the object to that, regardless of the version of sybase in play
	 * 
	 * @param realColumn
	 *            map of column values
	 * @param attName
	 *            The key name e.g. length
	 * @return length/data size as stored in the map, returned as a primitive
	 *         int
	 */
	int getValueFromRealColumn(final Map<String, Object> realColumn,
			final String attName) {
		if (realColumn.containsKey(attName)) {
			final Number realColumnValue = (Number) realColumn.get(attName);
			return realColumnValue.intValue();
		}
		throw new NullPointerException("No Key called '" + attName + "' in Map");
	}

	/**
	 * @param altered
	 *            .
	 * @param realColumnsMap
	 *            .
	 * @param tad
	 *            .
	 * @return .
	 * @throws Exception .
	 */
	private boolean alterTableDeleteColumn(boolean altered,
			final Map<String, HashMap<String, Object>> realColumnsMap,
			final TableAlterDetails tad) throws Exception {
		// remove not checked (extra) realcols from table
		for (String key : realColumnsMap.keySet()) {
			// 20111028:don't want to drop column: auto_key, Hack for Son Vis
			// WP00006 //TODO: Fix
			if (!key.equalsIgnoreCase("auto_key")) {

				final Map<String, Object> realCol = realColumnsMap.get(key);
				final String col = ((String) realCol.get("cname")).trim();

				// log.log(Level.INFO, "Deleting column " + col + " from  " +
				// dwhTmp.getTablename());
				// executeSQL("ALTER TABLE " + dwhTmp.getTablename() +
				// " DELETE " + col);
				tad.deleteColumn(col);
				// sqlStatements.add("ALTER TABLE " + dwhTmp.getTablename() +
				// " DELETE " +
				// col);
				altered = true;
			}
		}
		return altered;
	}

	/**
	 * @param allIndexes
	 *            .
	 * @param altered
	 *            .
	 * @param dwhTmp
	 *            .
	 * @param dwhcolum
	 *            .
	 * @param tad
	 *            .
	 * @return .
	 * @throws Exception .
	 */
	protected boolean alterTableAddColumn(final List<String> allIndexes,
			boolean altered, final Dwhpartition dwhTmp,
			final Dwhcolumn dwhcolum, final TableAlterDetails tad)
			throws Exception {
		// new column found
		// add column to table

		String value = "";
		final String index = dwhcolum.getIndexes();
		final String table = dwhTmp.getTablename();
		final String column = dwhcolum.getDataname();
		final String datasize = dwhcolum.getDatasize().toString();
		final String datascale = dwhcolum.getDatascale().toString();
		final String datatype = dwhcolum.getDatatype();
		final String dataname = dwhcolum.getDataname();

		if (hasParam(datatype)) {
			// if numeric and datascale changed
			if ((datatype.equalsIgnoreCase("decimal") || datatype
					.equalsIgnoreCase("numeric"))
					&& dwhcolum.getDatascale() != 0) {
				value = "(" + datasize + "," + datascale + ")";
			} else {
				value = "(" + datasize + ")";
			}
		}

		final String nullStr = " null ";

		// create new column to realcolumns
		// log.log(Level.INFO, "Adding column " + dataname + " " + datatype +
		// value
		// + " to " + table);
		// final String sql = "ALTER TABLE " + table + " ADD " + dataname + " "
		// +
		// datatype + value + nullStr + "; ";
		final String sql = dataname + " " + datatype + value + nullStr;
		// contains indexes that should be in this column
		final ArrayList<String> newIndexes = getIndexList(index);

		// >>> Into parallel execution
		if (!newIndexes.isEmpty()) {

			for (String indx : newIndexes) {

				final String indexname = table + "_" + column + "_" + indx;

				// and create a new index.
				final String tmpSql = " CREATE " + indx + " INDEX " + indexname
						+ " ON " + table + " (" + column + "); ";
				log.log(Level.FINEST, "add new index " + tmpSql);
				if (tmpSql.length() > 0) {
					allIndexes.add(tmpSql);
				}

				altered = true;
			}
		}
		// << Into parallel execution

		// executeSQL(sql);
		tad.addColumn(sql);
		return altered;
	}

	/**
	 * @return List of DWH Partitions.
	 * @throws Exception .
	 */
	private List<Dwhpartition> getDWHPartitions() throws Exception {

		final List<Dwhpartition> result = new ArrayList<Dwhpartition>();

		try {

			// Tpactivation
			final Tpactivation tpa = new Tpactivation(dwhRepRock);
			tpa.setStatus(ACTIVE);
			tpa.setTechpack_name(techpackname);

			final TpactivationFactory tpaf = new TpactivationFactory(dwhRepRock,
					tpa);

			if (tpaf != null) {

				for (Tpactivation tpaftmp : tpaf.get()) {

					try {

						// Dwhtype
						final Dwhtype dtp = new Dwhtype(dwhRepRock);
						dtp.setTechpack_name(tpaftmp.getTechpack_name());
						final DwhtypeFactory dtpf = new DwhtypeFactory(dwhRepRock,
								dtp);

						if (dtpf != null) {
							Iterator<Dwhtype> inneriter;
							if (this.measurementType != null) {
								// Implies NVU.
								Vector<Dwhtype> tempVector = dtpf.get();
								tempVector = filterExistingTypes(tempVector);
								inneriter = tempVector.iterator();
							} else {
								inneriter = dtpf.get().iterator();
							}
							while (inneriter.hasNext()) {

								try {

									final Dwhtype dtpftmp = inneriter.next();

									// DWHPartitions
									final Dwhpartition dwhp = new Dwhpartition(
											dwhRepRock);
									dwhp.setStorageid(dtpftmp.getStorageid());
									final DwhpartitionFactory dwhpf = new DwhpartitionFactory(
											dwhRepRock, dwhp);

									if (dwhpf != null) {

										// loop DWHPartitions
										for (Dwhpartition dwhptmp : dwhpf.get()) {

											try {

												// collect datanames
												result.add(dwhptmp);

											} catch (Exception e) {
												log.log(Level.WARNING,
														"Error while iterating Dwhpartition",
														e);
												throw (e);
											}
										}
									}

								} catch (Exception e) {
									log.log(Level.WARNING,
											"Error while iterating Dwhtype", e);
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
	 * @param str
	 *            .
	 * @param mask
	 *            .
	 * @return .
	 * @throws Exception .
	 */
	private String match(final String str, final String mask) throws Exception {

		String retval = "";
		final Pattern patt = Pattern.compile(mask);
		final Matcher m = patt.matcher(str);
		if (m.find()) {
			retval = m.group(1);
		}

		return retval;
	}

	/**
	 * @param type
	 *            New method to filter the Dwhtype for a specific measurment
	 *            type.
	 */
	private void filterMeasType(final Measurementtype type) {
		if (measurementType != null && measurementType.length() > 0) {
			type.setTypename(measurementType);
		}
	}

	/**
	 * @param type
	 *            New method to filter the Dwhtype for a specific measurment
	 *            type.
	 */
	private void filterMeasType(final Referencetable type) {
		if (measurementType != null && measurementType.length() > 0) {
			type.setTypename(measurementType);
		}
	}

	/**
	 * Method to filter out measurement types based on measName (for STN) plus
	 * measName_PREV (for BSS). This method is called only for Generic SIU
	 * feature.
	 * 
	 * @param existing_types
	 *            .
	 * @return .
	 */
	private Vector<Dwhtype> filterExistingTypes(
			final Vector<Dwhtype> existing_types) {
		// New field for Node Version Update feature for BSS
		final String underscorePrev = "_PREV";
		final Vector<Dwhtype> filtered_types = new Vector<Dwhtype>();
		if (measurementType != null && measurementType.length() > 0) {
			if (existing_types.size() > 0) {
				for (Dwhtype type : existing_types) {
					if (type.getTypename().equalsIgnoreCase(measurementType)
							|| type.getTypename().equalsIgnoreCase(
									measurementType + underscorePrev)) {
						filtered_types.add(type);
					}
				}
			}

		}
		return filtered_types;
	}

	/**
	 * This is a utility method to convert NUMERIC types (which are returned
	 * from the database as BigDecimal) to Integers. Pass in the Object
	 * (BigDecimal|Integer) and it'll return an int.
	 * 
	 * @param objectToConvert
	 *            .
	 * @return .
	 */
	public int convertObjectToInteger(final Object objectToConvert) {
		int result = 0;
		if (objectToConvert instanceof BigDecimal) {
			result = ((BigDecimal) objectToConvert).intValue();
		} else if (objectToConvert instanceof Long) {
			result = ((Long) objectToConvert).intValue();
		} else if (objectToConvert instanceof Integer) {
			result = ((Integer) objectToConvert).intValue();
		} else {
			result = ((Number) objectToConvert).intValue();
		}
		return result;
	}

	private String getServerType() {
		File iniFile;
		// First look for dwh.ini file. If it isn't found, fall back to niq.ini
		iniFile = new File(System.getProperty(CONF_DIR, CONF_DIR_DEFAULT),
				DWH_INI_FILENAME);
		if (!iniFile.exists()) {
			iniFile = new File(System.getProperty(CONF_DIR, CONF_DIR_DEFAULT),
					NIQ_INI_FILENAME);
		}

		final INIGet iniGet = new INIGet();

		iniGet.setFile(iniFile.getPath());
		iniGet.setSection("ETLC");
		iniGet.setParameter("Server_Type");
		iniGet.execute(log);
		return iniGet.getParameterValue().toString();
	}

	public int getMaxColsPerStatement() {
		return MAX_COLS_PER_STATEMENT;
	}
	
	/**
	 * Returns map of reference columns of vector reference table<br>
	 * Re-written method to iterate for only one table
	 * 
	 * @return Map: typeID -> Vector of Referencecolumns
	 * @throws Exception
	 */
	private Map<String, Vector<Referencecolumn>> getVectorCounterColumnsNew() throws Exception{
		boolean isVector = false;
		Map<String, Vector<Referencecolumn>> result;
		try {
			result = new HashMap<String, Vector<Referencecolumn>>();
			
			// versioning
			final Versioning ver = new Versioning(dwhRepRock);
			ver.setTechpack_name(techpackname);
			ver.setVersionid(newVersionID);
			final VersioningFactory verf = new VersioningFactory(dwhRepRock, ver);
			if (verf != null) {
				final Vector<Versioning> vers = verf.get();
				if (vers.size() >= 1) {
					final Versioning vertmp = vers.get(0);
					// Measurementtype
					final Measurementtype meast = new Measurementtype(dwhRepRock);
					meast.setVersionid(vertmp.getVersionid());
					// For Node Version Update - Filter out the exact meas type.
					filterMeasType(meast);
					final MeasurementtypeFactory meastf = new MeasurementtypeFactory(dwhRepRock, meast);
					if (meastf != null) {
						for (Measurementtype measttmp : meastf.get()) {
							try {
								if (!isVector && measttmp != null && measttmp.getVectorsupport() != null
										&& measttmp.getVectorsupport() > 0) {
									// vector counter
									final Measurementtable mta = new Measurementtable(dwhRepRock);
									mta.setTypeid(measttmp.getTypeid());
									final MeasurementtableFactory mtaf = new MeasurementtableFactory(
											dwhRepRock, mta);
	
									if (mtaf != null) {
										for (Measurementtable mtatmp : mtaf.get()) {
											try {
												if (mtatmp.getTablelevel().equalsIgnoreCase("raw")) {													
													final Vector<Referencecolumn> referencecolumns = new Vector<Referencecolumn>();
													final String typeid = newVersionID + ":DIM"
																+ techpackname.substring(techpackname.indexOf("_"))
																+ "_VECTOR_REFERENCE";
													
													final Referencecolumn tabCnt = new Referencecolumn(dwhRepRock);
													tabCnt.setTypeid(typeid);
													tabCnt.setDataname("TABLE_COUNTER");
													tabCnt.setColnumber((long) 1);
													tabCnt.setDatatype("varchar");
													tabCnt.setDatasize(100);
													tabCnt.setDatascale(0);
													tabCnt.setUniquevalue((long) 255);
													tabCnt.setNullable(1);
													tabCnt.setIndexes("HG");
													tabCnt.setUniquekey(1);
													tabCnt.setIncludesql(0);
													tabCnt.setIncludeupd(0);
													tabCnt.setColtype(null);
													tabCnt.setDescription(null);
													tabCnt.setUniverseclass(null);
													tabCnt.setUniverseobject(null);
													tabCnt.setUniversecondition(null);
													referencecolumns.add(tabCnt);
													
													final Referencecolumn dcvec = new Referencecolumn(dwhRepRock);
													dcvec.setTypeid(typeid);
													dcvec.setDataname("DCVECTOR");
													dcvec.setColnumber((long) 1);
													dcvec.setDatatype("int");
													dcvec.setDatasize(0);
													dcvec.setDatascale(0);
													dcvec.setUniquevalue((long) 255);
													dcvec.setNullable(1);
													dcvec.setIndexes("HG");
													dcvec.setUniquekey(1);
													dcvec.setIncludesql(0);
													dcvec.setIncludeupd(0);
													dcvec.setColtype(null);
													dcvec.setDescription(null);
													dcvec.setUniverseclass(null);
													dcvec.setUniverseobject(null);
													dcvec.setUniversecondition(null);
													referencecolumns.add(dcvec);
													
													final Referencecolumn dcrel = new Referencecolumn(dwhRepRock);
													dcrel.setTypeid(typeid);
													dcrel.setDataname("DCRELEASE");
													dcrel.setColnumber((long) 3);
													dcrel.setDatatype("varchar");
													dcrel.setDatasize(16);
													dcrel.setDatascale(0);
													dcrel.setUniquevalue((long) 255);
													dcrel.setNullable(1);
													dcrel.setIndexes("HG");
													dcrel.setUniquekey(1);
													dcrel.setIncludesql(0);
													dcrel.setIncludeupd(0);
													dcrel.setColtype(null);
													dcrel.setDescription(null);
													dcrel.setUniverseclass(null);
													dcrel.setUniverseobject(null);
													dcrel.setUniversecondition(null);
													referencecolumns.add(dcrel);
													
													final Referencecolumn quantity = new Referencecolumn(dwhRepRock);
													quantity.setTypeid(typeid);
													quantity.setDataname("QUANTITY");
													quantity.setColnumber((long) 4);
													quantity.setDatatype("int");
													quantity.setDatasize(0);
													quantity.setDatascale(0);
													quantity.setUniquevalue((long) 255);
													quantity.setNullable(1);
													quantity.setIndexes("HG");
													quantity.setUniquekey(0);
													quantity.setIncludesql(0);
													quantity.setIncludeupd(0);
													quantity.setColtype(null);
													quantity.setDescription(null);
													quantity.setUniverseclass(null);
													quantity.setUniverseobject(null);
													quantity.setUniversecondition(null);
													referencecolumns.add(quantity);
	
													final Referencecolumn value = new Referencecolumn(dwhRepRock);
													value.setTypeid(typeid);
													value.setDataname("VALUE");
													value.setColnumber((long) 2);
													value.setDatatype("varchar");
													value.setDatasize(100);
													value.setDatascale(0);
													value.setUniquevalue((long) 255);
													value.setNullable(1);
													value.setIndexes("HG");
													value.setUniquekey(1);
													value.setIncludesql(0);
													value.setIncludeupd(0);
													value.setColtype(null);
													value.setDescription(null);
													value.setUniverseclass(null);
													value.setUniverseobject(null);
													value.setUniversecondition(null);
													referencecolumns.add(value);
	
													result.put(typeid, referencecolumns);
													isVector = true;
												}
											}
											catch (Exception e) {
												log.log(Level.WARNING,"Error while iterating Measurementcolumn table", e);
												throw (e);
											}
										}
									}
								}
							} 
							catch (Exception e) {
								log.log(Level.WARNING,"Error while retrieving Versioning table", e);
								throw (e);
							}
						}
					}
				}
			}
		} 
		catch (Exception e) {
			log.log(Level.WARNING, "Error while retrieving Versioning table", e);
			throw (e);
		}
		return result;
	}
	
	/**
	 * Checks if techpack has been updated with 
	 * latest vector handling implementation
	 */
	private void checkVectorFlag() {
		String vectorConfDir = CONF_DIR_DEFAULT + "/vectorflags/";
		String vectorConfFlag = vectorConfDir + "New_Vector_" + this.techpackname;
		
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
	 * Adding new vector counters to Dwhtype table
	 * 
	 * @param vecType
	 * @throws SQLException
	 * @throws RockException
	 */
	@SuppressWarnings("unused")
	private void addVecToDwhtype(String vecType) throws SQLException, RockException {
		final Dwhtype newtype = new Dwhtype(dwhRepRock);
		newtype.setTechpack_name(techpackname);
		newtype.setTypename(vecType);
		newtype.setTablelevel(DWHTYPE_TABLE_LEVEL_PLAIN);
		newtype.setStorageid(vecType + ":PLAIN");
		newtype.setPartitioncount(DEFAULT_PARTITION_COUNT);
		newtype.setStatus(NEWVECTOR);
		newtype.setOwner(DC);
		newtype.setViewtemplate("");
		newtype.setCreatetemplate(CREATEPARTITION_VM);
		newtype.setNextpartitiontime(null);
		newtype.setBasetablename(vecType);
		newtype.setDatadatecolumn(null);
		newtype.setPartitionsize(-1L);
		newtype.setType(DWHTYPE_TYPE_SIMPLE);
		newtype.setPublicviewtemplate(CREATEPUBLICVIEWFORSIMPLE_VM);
		newtype.saveDB();
		log.fine("Adding new vector type " + newtype.getStorageid());
		
	}
	
}
