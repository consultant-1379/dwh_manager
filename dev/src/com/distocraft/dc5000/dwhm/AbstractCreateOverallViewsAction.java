package com.distocraft.dc5000.dwhm;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;

/**
 * Abstract class for creating an overall view across each set of SUC and ERR views for a particular techpack. The view
 * creation may fail if there are locks on the database. However this action retries view recreation for specified
 * amount of times, three times by default, before giving up. See attmeptToCreateViews() in AbstractCreateViewsAction on
 * how it attempts retries.
 * 
 * @author EPAUJOR
 * 
 */
public abstract class AbstractCreateOverallViewsAction extends AbstractCreateViewsAction {

  private static final String UNDERSCORE = "_";

  private static final String ERR = "_ERR_";

  private static final String SUC = "_SUC_";

  protected List<Dwhtype> dwhTypes;

  /**
   * Abstract class for creating an overall view across each set of SUC and ERR
   * views for a particular techpack.
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
   * @param listOfViewsForTableLevel
   *          list of views for a particular table level (RAW, 15MIN, etc.) to
   *          create overall views for
   * @throws Exception
   */
  public AbstractCreateOverallViewsAction(final RockFactory dbaConnectionToDwhdb, final RockFactory dcConnectiontoDwhdb,
      final RockFactory dwhrepConnectiontoRepdb, final List<Dwhtype> dwhTypes, final Logger loggerForClass,
      final List<String> listOfViewsForTableLevel) throws Exception {
    super(dbaConnectionToDwhdb, dcConnectiontoDwhdb, dwhrepConnectiontoRepdb, loggerForClass);
    this.dwhTypes = dwhTypes;
    createOverallViews(listOfViewsForTableLevel);
  }

  /**
   * Creates all the overall views required
   * 
   * @param dwhTypes
   * @param listOfViewsForTableLevel
   * @throws Exception
   */
  private void createOverallViews(final List<String> listOfViewsForTableLevel) throws Exception {
    // While there are still elements in the listOfViewsForTableLevel check to see if an overall view is required for it
    while (!listOfViewsForTableLevel.isEmpty()) {
      // Get the first element in the list and remove it (and the opposite view name) at the end of the loop so that we
      // do not check it again
      final String selectedViewName = listOfViewsForTableLevel.get(0);
      final String oppositeViewName = getOppositeViewName(selectedViewName);

      if (oppositeViewName != null && listOfViewsForTableLevel.contains(oppositeViewName)) {
        // Get columns from selectedViewName or oppositeViewName as columns should be the same for both of these
        final List<Dwhcolumn> columns = getColumns(dwhTypes, selectedViewName);
        createOverallViewForDc(columns, selectedViewName, oppositeViewName);
        createOverallViewForDcPublic(columns, selectedViewName, oppositeViewName);
        listOfViewsForTableLevel.remove(oppositeViewName);
      }

      listOfViewsForTableLevel.remove(selectedViewName);
    }
  }

  protected void createOverallViewForDc(final List<Dwhcolumn> columns, final String... listOfViews) throws Exception {
    log.info("Creating overall view for type " + viewName);
    log.finest("Using overall view " + getOverallViewTemplateForDc());
    final VelocityContext velocityContext = createVelocityContext(columns, listOfViews);

    mergeVelocityTemplateAndCreateView(getOverallViewTemplateForDc(), dwhrock, "overall view", velocityContext);
  }

  private void createOverallViewForDcPublic(final List<Dwhcolumn> columns, final String... listOfViews)
      throws Exception {

    log.info("Creating overall public view for type " + viewName);
    log.finest("Using overall public view " + getOverallViewTemplateForDcPublic());
    final VelocityContext velocityContext = createVelocityContext(columns, listOfViews);

    mergeVelocityTemplateAndCreateView(getOverallViewTemplateForDcPublic(), dbadwhrock, "overall public view",
        velocityContext);
  }

  protected VelocityContext createVelocityContext(final List<Dwhcolumn> columns, final String... listOfViews) {
    final VelocityContext velocityContext = new VelocityContext();

    velocityContext.put("viewName", viewName);
    velocityContext.put("listOfViews", Arrays.asList(listOfViews));
    velocityContext.put("columns", columns);
    return velocityContext;
  }

  private List<Dwhcolumn> getColumns(final List<Dwhtype> types, final String selectedViewName) throws SQLException,
      RockException {
    List<Dwhcolumn> columns = null;

    for (Dwhtype type : types) {
      if (type.getBasetablename().equals(selectedViewName)) {
        columns = getDwhColumns(type);
        break;
      }
    }
    return columns;
  }

  /**
   * If the viewName is EVENT_E_SGEH_SUC_RAW, then this will return the opposite
   * view name, EVENT_E_SGEH_ERR_RAW. If the viewName is EVENT_E_SGEH_ERR_RAW,
   * then this will return the opposite view name, EVENT_E_SGEH_SUC_RAW.
   * Otherwise, return null
   * 
   * @param selectedViewName
   * @return
   */
  private String getOppositeViewName(final String selectedViewName) {
    String oppositeViewName = null;
    if (selectedViewName.contains(SUC)) {
      oppositeViewName = selectedViewName.replace(SUC, ERR);
      createOverallViewName(selectedViewName, SUC);
    } else if (selectedViewName.contains(ERR)) {
      oppositeViewName = selectedViewName.replace(ERR, SUC);
      createOverallViewName(selectedViewName, ERR);
    }
    return oppositeViewName;
  }

  /**
   * Overall view should not have _SUC or _ERR in it. Therefore, if the viewName
   * is EVENT_E_SGEH_SUC_RAW, overall view name will be EVENT_E_SGEH_RAW
   * 
   * @param selectedViewName
   * @param textToReplace
   */
  private void createOverallViewName(final String selectedViewName, final String textToReplace) {
    viewName = selectedViewName.replace(textToReplace, UNDERSCORE);
  }

  /**
   * Gets the overall view template for the user DC so that the overall view can
   * be created
   * 
   * @return
   */
  protected abstract String getOverallViewTemplateForDc();

  /**
   * Gets the overall view template for the user dcPublic so that the overall
   * view can be created
   * 
   * @return
   */
  protected abstract String getOverallViewTemplateForDcPublic();
}
