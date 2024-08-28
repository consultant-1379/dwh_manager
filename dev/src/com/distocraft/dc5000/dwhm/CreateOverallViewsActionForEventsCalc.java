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

import java.util.List;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Dwhtype;

/**
 * Creates an overall view across each set of SUC and ERR ENIQ Events pre-calculation views.
 * 
 * @author EPAUJOR
 * 
 */
public class CreateOverallViewsActionForEventsCalc extends AbstractCreateOverallViewsAction {

  private static final String EVENTS_CALC_VIEW_TEMPLATE_FOR_DC = "createoveralleventscalcview.vm";

  private static final String EVENTS_CALC_TEMPLATE_FOR_DC_PUBLIC = "createoverallpubliceventscalcview.vm";

  /**
   * Creates an overall view across each set of SUC and ERR ENIQ Events pre-calculation views for a particular techpack.
   * 
   * @param dbaConnectionToDwhdb
   *          This allows a user to connection to the DWHDB database as user DBA
   * @param dcConnectiontoDwhdb
   *          This allows a user to connection to the DWHDB database as user dc
   * @param dwhrepConnectiontoRepdb
   *          This allows a user to connection to the REPDB database as user dwhrep
   * @param dwhTypes
   * @param loggerForClass
   *          Allows the logging of messages for this class
   * @param listOfViews
   *          list of views to create overall views for
   * @throws Exception
   */
  public CreateOverallViewsActionForEventsCalc(final RockFactory dbaConnectionToDwhdb, final RockFactory dcConnectiontoDwhdb,
      final RockFactory dwhrepConnectiontoRepdb, final List<Dwhtype> dwhTypes, final Logger loggerForClass, final List<String> listOfViews)
      throws Exception {
    super(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb, dwhTypes, loggerForClass, listOfViews);
  }

  @Override
  protected String getOverallViewTemplateForDc() {
    return EVENTS_CALC_VIEW_TEMPLATE_FOR_DC;
  }

  @Override
  protected String getOverallViewTemplateForDcPublic() {
    return EVENTS_CALC_TEMPLATE_FOR_DC_PUBLIC;
  }

}
