package com.distocraft.dc5000.dwhm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.sql.SQLActionExecute;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;

/**
 * Wander around in DWH database and find out weather there are such tables that
 * are not described in DWHPartition
 * 
 * @author lemminkainen
 * 
 */
public class DWHTableCheckAction extends SQLActionExecute {

  public static final String GETDWHTABLES = "select table_name,user_name from sys.SYSTABLE tab, sys.SYSUSERPERM perm where tab.creator = perm.user_id and perm.user_name=? and tab.table_type='BASE' ORDER BY table_name";

  private Logger log;

  private RockFactory reprock;

  private RockFactory dwhrock;

  private Properties conf;

  private String mode;

  public DWHTableCheckAction(RockFactory reprock, RockFactory dwhrock, Logger clog, Properties conf, String mode) {
    this.log = Logger.getLogger(clog.getName() + ".TableCheck");
    this.reprock = reprock;
    this.dwhrock = dwhrock;
    this.conf = conf;
    this.mode = mode;
  }

  public void execute() {

    try {

      Map tables = getDWHTables();

      Iterator i = tables.keySet().iterator();

      while (i.hasNext()) {
        String tableName = (String) i.next();
        String user = (String) tables.get(tableName);

        tableName = tableName.trim();
        user = user.trim();

        Dwhpartition dp_cond = new Dwhpartition(reprock);
        dp_cond.setTablename(tableName);
        DwhpartitionFactory dp_fact = new DwhpartitionFactory(reprock, dp_cond);

        Vector part = dp_fact.get();

        if (part == null || part.size() < 1) {
          
          log.warning("Table " + user + "." + tableName + " not found from DWHPartition");

          if ("DELETE".equalsIgnoreCase(mode)) {
            log.warning("Dropping table "+user+"."+tableName+"...");

            Statement stmt = null;
            try {
              stmt = dwhrock.getConnection().createStatement();
              String drop_clause = "DROP TABLE " + user + "." + tableName;
              log.fine("Executing: " + drop_clause);
              stmt.executeUpdate(drop_clause);
              log.info("Table " + user + "." + tableName + " dropped");
            } catch (Exception e) {
              log.log(Level.WARNING, "Failed to drop table " + user + "." + tableName, e);
            } finally {
              try {
                stmt.close();
              } catch (Exception e) {
              }
            }

          }
          
        }

      } // foreach table

      log.info("Successfully finished");

    } catch (Exception e) {
      log.log(Level.WARNING, "Failed exceptionally", e);
    }

  }

  public Map getDWHTables() throws Exception {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    Connection con = null;

    Map ret = new HashMap();
    

    try {
      con = dwhrock.getConnection();
      stmt = con.prepareStatement(GETDWHTABLES);
      stmt.setString(1, conf.getProperty("username", "dc"));
      rs = stmt.executeQuery();

      while (rs.next()) {
        String table = rs.getString("table_name").trim();
        String schema = rs.getString("user_name").trim();
        ret.put(table, schema);
      }

    }finally{

        try {
        	if(rs!=null)
        		rs.close();
        } catch (Exception e) {
        }
        try {
        	if(stmt!=null)
        		stmt.close();
        } catch (Exception e) {
        }
      }
    
    log.info("Found " + ret.size() + " tables from DWH");

    return ret;

  }
}
