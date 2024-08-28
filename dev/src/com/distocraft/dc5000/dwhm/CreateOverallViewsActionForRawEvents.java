/**------------------------------------------------------------------------
 *
 *
 *      COPYRIGHT (C)                   ERICSSON RADIO SYSTEMS AB, Sweden
 *
 *      The  copyright  to  the document(s) herein  is  the property of
 *      Ericsson Radio Systems AB, Sweden.
 *
 *      The document(s) may be used  and/or copied only with the written
 *      permission from Ericsson Radio Systems AB  or in accordance with
 *      the terms  and conditions  stipulated in the  agreement/contract
 *      under which the document(s) have been supplied.
 *
 *------------------------------------------------------------------------
 */
package com.distocraft.dc5000.dwhm;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;

/**
 * Creates an overall view across each set of SUC RAW and ERR RAW views for a
 * particular techpack.
 * 
 * @author EPAUJOR
 * 
 */
public class CreateOverallViewsActionForRawEvents extends AbstractCreateOverallViewsAction {

  private static final String RAW_VIEW_TEMPLATE_FOR_DC = "createoverallview.vm";

  private static final String RAW_VIEW_TEMPLATE_FOR_DC_PUBLIC = "createoverallpublicview.vm";

  /**
   * Creates an overall view across each set of SUC RAW and ERR RAW views for a
   * particular techpack.
   * 
   * @param dbaConnectionToDwhdb
   *          This allows a user to connection to the DWHDB database as user DBA
   * @param dcConnectiontoDwhdb
   *          This allows a user to connection to the DWHDB database as user dc
   * @param dwhrepConnectiontoRepdb
   *          This allows a user to connection to the REPDB database as user
   *          dwhrep
   * @param dwhTypes
   * @param loggerForClass
   *          Allows the logging of messages for this class
   * @param listOfViews
   *          list of views to create overall views for
   * @throws Exception
   */
  public CreateOverallViewsActionForRawEvents(final RockFactory dbaConnectionToDwhdb,
      final RockFactory dcConnectiontoDwhdb, final RockFactory dwhrepConnectiontoRepdb, final List<Dwhtype> dwhTypes,
      final Logger loggerForClass, final List<String> listOfViews) throws Exception {
    super(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb, dwhTypes, loggerForClass, listOfViews);
  }

  @Override
  protected String getOverallViewTemplateForDc() {
    return RAW_VIEW_TEMPLATE_FOR_DC;
  }

  @Override
  protected String getOverallViewTemplateForDcPublic() {
    return RAW_VIEW_TEMPLATE_FOR_DC_PUBLIC;
  }

  @Override
  protected void createOverallViewForDc(final List<Dwhcolumn> columns, final String... listOfViews) throws Exception {
    super.createOverallViewForDc(columns, listOfViews);
    createOverallTimeRangeViewForDc(listOfViews);
  }

  private void createOverallTimeRangeViewForDc(final String... listOfViews) throws SQLException, RockException,
      Exception {
    final String timeRangeViewName = viewName + "_TIMERANGE";
    final List<Dwhpartition> partitions = getAllPartitions(listOfViews);

    if (partitions.isEmpty()) {
      log.warning("No sane partitions found. View: '" + timeRangeViewName + "' will not be created.");
    } else {
      log.info("Creating overall time range view for type " + timeRangeViewName);
      log.finest("Using overall view " + RAW_TIME_RANGE_VIEW_TEMPLATE_FOR_DC);

      final VelocityContext velocityContext = new VelocityContext();
      velocityContext.put("baseTableName", timeRangeViewName);
      velocityContext.put("partitions", partitions);

      mergeVelocityTemplateAndCreateView(RAW_TIME_RANGE_VIEW_TEMPLATE_FOR_DC, dwhrock, "overall view", velocityContext);
    }
  }

  private List<Dwhpartition> getAllPartitions(final String... listOfViews) throws SQLException, RockException {
    final List<Dwhpartition> partitions = new ArrayList<Dwhpartition>();

    for(String view: listOfViews){
      partitions.addAll(getDwhPartitions(view));
    }
    return partitions;
  }

  private List<Dwhpartition> getDwhPartitions(final String selectedViewName) throws SQLException, RockException {
    List<Dwhpartition> partitions = new ArrayList<Dwhpartition>();
    for (Dwhtype type : dwhTypes) {
      if (type.getBasetablename().equals(selectedViewName)) {
        partitions = getPartitions(type.getStorageid());
        checkForInsanePartitions(partitions, type);
        break;
      }
    }
    return partitions;
  }

  private void checkForInsanePartitions(final List<Dwhpartition> partitions, final Dwhtype type) {
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
  }
}
