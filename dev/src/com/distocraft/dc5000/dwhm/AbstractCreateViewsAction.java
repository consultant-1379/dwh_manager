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

import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;

/**
 * Abstract class for creating the views for specified type.
 * 
 * @author epaujor
 * 
 */
public abstract class AbstractCreateViewsAction {

  private int maxRetries = 3;

  private int retryPeriod_in_seconds = 60;

  private int retryRandom_in_seconds = 120;
  
  protected RockFactory dwhrock;

  protected RockFactory reprock;

  protected RockFactory dbadwhrock;

  protected Logger log;

  protected String viewName = null;
  
  protected static final String RAW_TIME_RANGE_VIEW_TEMPLATE_FOR_DC = "createtimerangeview.vm";
  
  protected static final String CREATE_VIEW_TEMPLATE = "createview.vm"; 

  public AbstractCreateViewsAction(final RockFactory dbadwhrock, final RockFactory dwhrock, final RockFactory reprock,
      final Logger clog) throws Exception {
    this.dwhrock = dwhrock;
    this.reprock = reprock;
    this.dbadwhrock = dbadwhrock;
    initLogger(clog);
    getStaticProperties();
    initVelocity();
  }

  private void initLogger(final Logger clog) {
    log = Logger.getLogger(clog.getName() + ".dwhm.CreateViews");
  }

  private void getStaticProperties() {
    try {
      maxRetries = Integer.parseInt(StaticProperties.getProperty("DWHManager.viewCreateRetries", "3"));
    } catch (NumberFormatException nfe) {
      log.config("Parameter DWHManager.viewCreateRetries is invalid in static.properties");
    }

    try {
      retryPeriod_in_seconds = Integer.parseInt(StaticProperties.getProperty("DWHManager.viewCreateRetryPeriod", "60"));
    } catch (NumberFormatException nfe) {
      log.config("Parameter DWHManager.viewCreateRetryPeriod is invalid in static.properties");
    }

    try {
      retryRandom_in_seconds = Integer.parseInt(StaticProperties.getProperty("DWHManager.viewCreateRetryRandom", "120"));
    } catch (NumberFormatException nfe) {
      log.config("Parameter DWHManager.viewCreateRetryRandom is invalid in static.properties");
    }
  }

  private void initVelocity() throws Exception {
    Velocity.setProperty("resource.loader", "class,file");
    Velocity.setProperty("class.resource.loader.class",
        "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
    Velocity
        .setProperty("file.resource.loader.class", "org.apache.velocity.runtime.resource.loader.FileResourceLoader");
    Velocity.setProperty("file.resource.loader.path", StaticProperties.getProperty("dwhm.templatePath",
        "/dc/dc5000/conf/dwhm_templates"));
    Velocity.setProperty("file.resource.loader.cache", "true");
    Velocity.setProperty("file.resource.loader.modificationCheckInterval", "60");
    Velocity.setProperty( RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,"org.apache.velocity.runtime.log.AvalonLogChute,org.apache.velocity.runtime.log.Log4JLogChute,org.apache.velocity.runtime.log.CommonsLogLogChute,org.apache.velocity.runtime.log.ServletLogChute,org.apache.velocity.runtime.log.JdkLogChute" );
    Velocity.setProperty("runtime.log.logsystem.log4j.logger","/eniq/home/dcuser/velocity.log"); 
    Velocity.init();
  }

  /**
   * Merges Velocity template and then attempts to create the view
   * 
   * @param type
   * @param template
   * @param dbForCreatingView
   * @param infoStringForLogging
   * @param velocityContext
   * @throws Exception
   * @throws SQLException
   */
  protected void mergeVelocityTemplateAndCreateView(final String template, final RockFactory dbForCreatingView,
      final String infoStringForLogging, final VelocityContext velocityContext) throws Exception, SQLException {
    final StringWriter sqlWriter = new StringWriter();
    final boolean isMergeOk = Velocity.mergeTemplate(template, Velocity.ENCODING_DEFAULT, velocityContext, sqlWriter);

    if (isMergeOk) {
      attemptToCreateView(sqlWriter.toString(), dbForCreatingView);
    } else {
      log.warning("Velocity failed for " + infoStringForLogging + " ");
      throw new Exception("Velocity failed for " + infoStringForLogging + " ");
    }
  }

  private void attemptToCreateView(final String sqlWriter, final RockFactory dbForCreatingView) throws SQLException {
    log.finest("Create template ready: " + sqlWriter);
    int error=0;
    for (int i = 0; i < maxRetries; i++) {

      Statement stmt = null;

      try {
        stmt = dbForCreatingView.getConnection().createStatement();
        stmt.executeUpdate(sqlWriter);
        log.info("View succesfully created for " + viewName);
        break;
      }
      catch (SQLException e) {
				error = e.getErrorCode();
				if (e.getMessage().indexOf("SQL Anywhere Error -210") > 0|| e.getMessage().indexOf("ASA Error -210") > 0 || e.getMessage().indexOf("User 'another user' has the row") > 0) { // view was locked
					
					final Random rnd = new Random();
					final int secs = Math.abs(rnd.nextInt()	% retryRandom_in_seconds)+ retryPeriod_in_seconds;

					log.warning("SQLException: View creation failed to locked view. Retrying in "+ secs + " seconds");

					try {
						Thread.sleep(secs * 1000);
					} catch (Exception ie) {
					}
				} // 8405 - Sybase Error Code for row locking issue
				else if (error == 8405) {
					final Random rnd = new Random();
					final int secs = Math.abs(rnd.nextInt()% retryRandom_in_seconds)+ retryPeriod_in_seconds;
					log.warning("SybSQLException: View creation failed to locked view. Retrying in "+ secs + " seconds");
					try {
						Thread.sleep(secs * 1000);
					} catch (Exception ie) {
					}
				} else {
			          throw e;
				}
			} finally {
        try {
          if (stmt != null) {
            stmt.close();
          }
        } catch (Exception e) {
        }
      }
    }// for each retry
  }

  /**
   * Gets all the columns associated with this DwhType
   * 
   * @param type
   * @return
   * @throws SQLException
   * @throws RockException
   */
  protected Vector<Dwhcolumn> getDwhColumns(final Dwhtype type) throws SQLException, RockException {
    final Dwhcolumn dc_cond = new Dwhcolumn(reprock);
    dc_cond.setStorageid(type.getStorageid());
    final DwhcolumnFactory dc_fact = new DwhcolumnFactory(reprock, dc_cond);
    final Vector<Dwhcolumn> columns = dc_fact.get();
    sortColumns(columns);
    return columns;
  }

  public String getViewName() {
    return viewName;
  }

  private void sortColumns(final Vector<Dwhcolumn> columns) {
    Collections.sort(columns, new Comparator() {

      @Override
      public int compare(final Object o1, final Object o2) {
        final Dwhcolumn c1 = (Dwhcolumn) o1;
        final Dwhcolumn c2 = (Dwhcolumn) o2;

        return c1.getColnumber().compareTo(c2.getColnumber());
      }

      public boolean equals(final Object o1, final Object o2) {
        final Dwhcolumn c1 = (Dwhcolumn) o1;
        final Dwhcolumn c2 = (Dwhcolumn) o2;

        return c1.getColnumber().equals(c2.getColnumber());
      }
    });
  }

  protected Vector<Dwhpartition> getPartitions(final String storageId) throws SQLException, RockException {
    final Dwhpartition dwhPartitionCondition = new Dwhpartition(reprock);
    dwhPartitionCondition.setStorageid(storageId);
    final DwhpartitionFactory dp_fact = new DwhpartitionFactory(reprock, dwhPartitionCondition);

    final Vector<Dwhpartition> partitions = dp_fact.get();
    return partitions;
  }
}
