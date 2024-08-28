package com.distocraft.dc5000.dwhm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

import com.distocraft.dc5000.common.StaticProperties;

public final class TableAlterDetails {

    /**
     * SQL statement delimiter
     */
    private static final String SQL_DELIMITER = ",";
    /**
     * SQL end line delimiter
     */
    private static final String SQL_END = ";";
    /**
     * Alter statement prefix
     */
    private static final String SQL_ALTER_TABLE = "alter table ";
    /**
     * The name of the table thats going to be changed.
     */
    private final String tableName;
    /**
     * Used to store all the alter statements as they are split between add, then modify, then delete calls.
     */
    private final Map<Integer, List<String>> tableAlterStatements = new HashMap<Integer, List<String>>();
    /**
     * Final list of SQL statements needed to alter the table.
     */
    private final List<String> generatedSqlAlterStatements = new ArrayList<String>();
    /**
     * The length of the current SQL statement in bytes
     */
    private static int currentSqlByteLength = 0;
    /**
     * The max statement size in bytes. This will overide the maxColsPerStatement setting if reached.
     * The value was previously set to 3072, which is now used as a default value for cases where the property is
     * not defined in the static.properties file. 
     */
    private final int maxStatementSize = Integer.parseInt(StaticProperties.getProperty("TableAlterDetails.maxStatementSize", "3072"));

	/**
     * The max number of alter <col>statements per <alter table> statement.
     */
    private final int maxColsPerStatement;
    /**
     * Count of the number of columns being altered in the current SQL statement
     */
    private int currentColsInStatement = 0;
    /**
     * Used to hold the overall alter statement command prefix i.e. 'alter table PM_E_EBSS_CELL_RAW_01 '
     */
    private static String alterTablePrefix = null;
    /**
     * Total number of modifications being made to the table.
     */
    private int totalAlterCount = 0;
    /**
     * Iterator used by VersionUpdateAction to execute all the batch alter statements.
     */
    private Iterator<String> interalIterator = null;

    private static final Integer ALTER_ADD = 0;
    private static final Integer ALTER_MODIFY = 1;
    private static final Integer ALTER_DELETE = 2;
    private static final Integer ALTER_ONLY_INDEX = 3;

    /**
     *
     * @param tableName The table that will be altered
     * @param maxColsPerStatement The max number of seperate alter statements that can be in the SQL sent to the db.
     */
    public TableAlterDetails(final String tableName, final int maxColsPerStatement){
        this.tableName = tableName;
        this.maxColsPerStatement = maxColsPerStatement;
    }

    /**
     * Get the name of the table the statements are generated for.
     * @return The table name
     */
    public final String getTableName(){
        return tableName;
    }

    /**
     * Used to generate the batch SQL statements so they are not altering more than the max columns per statement and the
     * batch statement is no bigger then the max bytes per statement.
     * @param allSql Used to hold the batch SQL statements
     * @param alterParts A list of all alter parts.
     * @param tableName The name of the table being modified.
     */
    private void constructSqlWithinLimits(final List<String> allSql, final List<String> alterParts, final String tableName){
        alterTablePrefix = SQL_ALTER_TABLE + tableName + " ";
        final StringBuilder currentSql = new StringBuilder();
        currentSqlByteLength = alterTablePrefix.getBytes().length;
        final Iterator<String> it = alterParts.iterator();
        while(it.hasNext()){
            final String col = it.next();
            appendPart(col, currentSql, allSql, !it.hasNext());
        }
    }

    /**
     * Append SQL to the batchs list line by line. Mainly used in the case of existing column alters
     * @param allSql Used to hold the batch SQL statements
     * @param alterParts A list of all alter parts.
     * @param tableName The name of the table being modified.
     */
    private void appendAlterSql(final List<String> allSql, final List<String> alterParts, final String tableName){
        alterTablePrefix = SQL_ALTER_TABLE + tableName + " ";
        for (String singleAlterStmt : alterParts) {
            final String oneFullAlterStmt = alterTablePrefix + singleAlterStmt + ";";
            allSql.add(oneFullAlterStmt);
        }
    }
    
    /**
     * Used for Drop Index statements
     * Each statement should be complete, so just add to resulting allSql.
     * @param allSql
     * @param alterParts
     */
    private void appendDropIndexSql(final List<String> allSql, final List<String> alterParts){
        for (String singleAlterStmt : alterParts) {
            allSql.add(singleAlterStmt);
        }
    }

    /**
     * Append an alter column statement to the batch statement. If the max limits on te batch statement has been reached
     * the current statement is stored to the overall list and the current statement is cleared, ready for any more alters.
     * @param sqlPart The explicit alter command to append
     * @param currentBuffer The current SQL batc command being generatd
     * @param statementList A list of all the generated batch commands
     * @param last Is this the last explicit alter command?
     */
    private void appendPart(final String sqlPart, final StringBuilder currentBuffer, final List<String> statementList, final boolean last){
        final int partLength  = sqlPart.getBytes().length;
        currentColsInStatement++;
        if(partLength + currentSqlByteLength < maxStatementSize && currentColsInStatement <= maxColsPerStatement){
            if(currentBuffer.length() > 0){
                currentBuffer.append(SQL_DELIMITER);
            }
            currentBuffer.append(sqlPart);
            if(last){
                resetBuffer(currentBuffer, statementList);
            }
            currentSqlByteLength += (partLength + SQL_DELIMITER.getBytes().length);
        } else {
            resetBuffer(currentBuffer, statementList);
            appendPart(sqlPart, currentBuffer, statementList, last);
        }
    }

    /**
     * Store the current SQL batch statement and reset the buffer.
     * @param buffer The current SQL batch buffer.
     * @param statementList A list of all the generated batch commands
     */
    private void resetBuffer(final StringBuilder buffer, final List<String> statementList){
        statementList.add(alterTablePrefix + buffer.toString() + SQL_END);
        buffer.delete(0, buffer.length());
        currentSqlByteLength = alterTablePrefix.getBytes().length;
        currentColsInStatement = 0;
    }

    /**
     * Adding a column to the table
     * @param colAddStatement The add statement ==> "column_name <data type> default_value" 
     */
    public final void addColumn(final String colAddStatement){
        addStatement(ALTER_ADD, "ADD " + colAddStatement);
    }

    /**
     * Used to drop a column
     * @param colDeleteStatement The column name
     */
    public final void deleteColumn(final String colDeleteStatement){
        addStatement(ALTER_DELETE, "DROP " + colDeleteStatement);
    }

    /**
     * Used to alter an existing column (e.g. datatype changing)
     * @param colAlterStatement The alter statement exclusing the 'alter table' part.
     */
    public final void alterExistingColumn(final String colAlterStatement){
        addStatement(ALTER_MODIFY, colAlterStatement);
    }
    
    public void alterOnlyIndex(String colAlterStatement) {
		addStatement(ALTER_ONLY_INDEX, colAlterStatement);
	}

    /**
     * Append the alter statement to a list of previous statements.
     * The list this method generates will be used to generate the final SQL statements
     * needed to modify the table
     * @param type The alter type
     * @param stmt The alter statement
     */
    private void addStatement(final Integer type, final String stmt){
        if(interalIterator != null){
            throw new RuntimeException("Iterator already started");
        }
        final List<String> tmp;
        if(tableAlterStatements.containsKey(type)){
            tmp = tableAlterStatements.get(type);
            tmp.add(stmt);
        } else {
            tmp = new ArrayList<String>();
            tmp.add(stmt);
            tableAlterStatements.put(type, tmp);
        }
        totalAlterCount++;
    }

    /**
     * Generate the list of SQL batch statements needed to change the able
     */
    private void generateSql(){
      final String formatterClass = System.getProperty("tad.formatter", "").trim();
      if(formatterClass == null || formatterClass.length() == 0){
        if(tableAlterStatements.containsKey(ALTER_ADD)){
            final List<String> adds = tableAlterStatements.get(ALTER_ADD);
            constructSqlWithinLimits(generatedSqlAlterStatements, adds, tableName);
        }
        if(tableAlterStatements.containsKey(ALTER_MODIFY)){
            final List<String> alters = tableAlterStatements.get(ALTER_MODIFY);
//            constructSqlWithinLimits(generatedSqlAlterStatements, alters, tableName);
            appendAlterSql(generatedSqlAlterStatements, alters, tableName);
        }
        if(tableAlterStatements.containsKey(ALTER_DELETE)){
            final List<String> deletes = tableAlterStatements.get(ALTER_DELETE);
            constructSqlWithinLimits(generatedSqlAlterStatements, deletes, tableName);
        }
        if(tableAlterStatements.containsKey(ALTER_ONLY_INDEX)){
        	// Index change, no change to column.
        	final List<String> drops = tableAlterStatements.get(ALTER_ONLY_INDEX);
        	appendDropIndexSql(generatedSqlAlterStatements, drops);
        }
        interalIterator = generatedSqlAlterStatements.iterator();
      } else {
        // yeah, the sybase alter table allows multiple add constraints in the one statement; hsql, h2 & derby dont!
        // so this is to get around that 'problem' in the unit tests....
        try{
          final Class klass = Class.forName(formatterClass);
          final Method getIterator = klass.getMethod("getIterator", String.class, List.class, List.class, List.class, List.class);
          final Object _instance = klass.newInstance();
          final List<String> adds = tableAlterStatements.get(ALTER_ADD);
          final List<String> alters = tableAlterStatements.get(ALTER_MODIFY);
          final List<String> deletes = tableAlterStatements.get(ALTER_DELETE);
          final List<String> indexes = tableAlterStatements.get(ALTER_ONLY_INDEX);
          interalIterator = (Iterator<String>)getIterator.invoke(_instance, tableName, adds, alters, deletes, indexes);
        } catch (ClassNotFoundException e){
          throw new RuntimeException("TAD Formatter errors (this should never show in production)", e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException("TAD Formatter errors (this should never show in production)", e.getCause());
        } catch (NoSuchMethodException e) {
          throw new RuntimeException("TAD Formatter errors (this should never show in production)", e);
        } catch (InstantiationException e) {
          throw new RuntimeException("TAD Formatter errors (this should never show in production)", e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("TAD Formatter errors (this should never show in production)", e);
        }
      }
    }

    /**
     * Get an interator of all the batch sql statements need to change the table.
     * Once this method has been called, the addColum, deleteColumn and alterExistingColumn wil throw
     * a RuntimeException error stating the iterator has already been started.
     * @return An interator containing all the statements needed to change the table.
     */
    public final Iterator<String> getIterator(){
        generateSql();
        return interalIterator;
    }

    /**
     * Get the total number of modifications being made to the table.
     * @return Total alter count ( add modify deletes)
     */
    public final int getTotalAlterCount(){
        return totalAlterCount;
    }

    /**
     * Get maximum statement size (in chars) 
     * @return
     */
	public int getMaxStatementSize() {
		return maxStatementSize;
	}

}
