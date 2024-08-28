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

import com.ericsson.eniq.common.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;

/**
 * Creates an overall view across all the tables type for a particular techpack.
 * The view creation may fail if there are locks on the database. However this
 * action retries view recreation for specified amount of times, three times by
 * default, before giving up.
 * 
 * @author EPAUJOR
 * 
 */
public class CreateOverallViewsFactory {

  public static final String EVENT_E_SGEH = "EVENT_E_SGEH";

  /**
   * Creates an overall view across all the tables type for a particular
   * techpack. Only implemented for EVENTS yet.
   * 
   * @param dbaConnectionToDwhdb
   * @param dcConnectiontoDwhdb
   * @param dwhrepConnectiontoRepdb
   * @param dwhTypes
   * @param loggerForClass
   * @param listOfCreatedViews
   * @param techpackName
   * @throws Exception
   */
  public static void createOverallViewsAction(final RockFactory dbaConnectionToDwhdb,
      final RockFactory dcConnectiontoDwhdb, final RockFactory dwhrepConnectiontoRepdb, final Vector<Dwhtype> dwhTypes,
      final Logger loggerForClass, final List<String> listOfCreatedViews, String techpackName) throws Exception {
    if (Utils.isEventsTechpack(techpackName)) {
      for (String tableLevel : Constants.LIST_OF_EVENT_TABLE_LEVELS) {
        createOverallViewsForTableLevel(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb, dwhTypes,
              loggerForClass, listOfCreatedViews, tableLevel);
      }
    }
  }

  private static void createOverallViewsForTableLevel(final RockFactory dbaConnectionToDwhdb,
      final RockFactory dcConnectiontoDwhdb, final RockFactory dwhrepConnectiontoRepdb, final Vector<Dwhtype> dwhTypes,
      final Logger loggerForClass, final List<String> listOfCreatedViews, String tableLevel) throws Exception {
    final List<String> listOfFilteredViews = getFilteredListOfViews(listOfCreatedViews, tableLevel);

    if (!listOfFilteredViews.isEmpty()) {
      if (tableLevel.equals(Constants.RAW)) {
        new CreateOverallViewsActionForRawEvents(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb,
            dwhTypes, loggerForClass, listOfFilteredViews);
      } else {
        new CreateOverallViewsActionForEventsCalc(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb,
            dwhTypes, loggerForClass, listOfFilteredViews);
      }
    }
  }

  /**
   * Gets a filter list of the views
   * 
   * @param listOfCreatedViews
   *          The full list of views
   * @param filter
   *          The filter used to get a subset of views list
   * @return returns the filtered lists of views
   */
  private static List<String> getFilteredListOfViews(final List<String> listOfCreatedViews, String filter) {
    final List<String> listOfViewsForRawData = new ArrayList<String>();

    for (String createdView : listOfCreatedViews) {
      if (createdView.endsWith(filter)) {
        listOfViewsForRawData.add(createdView);
      }
    }
    return listOfViewsForRawData;
  }

}
