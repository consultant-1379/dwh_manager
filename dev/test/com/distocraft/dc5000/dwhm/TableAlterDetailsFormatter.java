package com.distocraft.dc5000.dwhm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * So, the sybase alter table allows multiple add constraints in the one statement; hsql, h2 & derby dont!
 * This class is to get around that 'problem' in the unit tests.
 * Set the property 'tad.formatter' to this class name to get TableAlterDetails to use this class to generate
 * the interalIterator Iterator<String>
 */
public class TableAlterDetailsFormatter implements Iterator<String> {
  private static final Map<String, String> replaces = new HashMap<String, String>();
  public static void replace(final String from, final String to) {
    replaces.put(from, to);
  }

  private List<String> statements = null;
  private Iterator<String> iterator = null;
  public final Iterator<String> getIterator(final String tableName,
                                            final List<String> alterAdds, final List<String> alterModifies,
                                            final List<String> alterDeletes, final List<String> alterIndexes){
    statements = new ArrayList<String>();
    if(alterAdds != null){
      for(String addColumn : alterAdds){
        add("alter table " + tableName + " " + addColumn + ";");
      }
    }
    if(alterModifies != null){
      for(String modColumn : alterModifies){
        final String[] splits = modColumn.split(";");
        for(String _split : splits){
          final String split = _split.trim();
          if(split.toLowerCase().startsWith("alter ") || split.toLowerCase().startsWith("update ")) {
            add(split + ";");
          } else {
            add("alter table " + tableName + " " + split + ";");
          }
        }
      }
    }
    if(alterDeletes != null){
      for(String delColumn : alterDeletes){
        add("alter table " + tableName + " " + delColumn + ";");
      }
    }
    if(alterIndexes != null){
      for(String alterIndex : alterIndexes){
        add("alter table " + tableName + " " + alterIndex + ";");
      }
    }
    iterator = statements.iterator();
    return this;
  }

  private void add(final String statement){
    String tmp = statement;
    for(String from : replaces.keySet()){
      final int start = statement.indexOf(from);
      if(start >= 0){
        tmp = statement.replace(from, replaces.get(from));
      }
    }
    statements.add(tmp);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public String next() {
    return iterator.next().toUpperCase();
  }

  @Override
  public void remove() {
    throw new RuntimeException("No!");
  }
}
