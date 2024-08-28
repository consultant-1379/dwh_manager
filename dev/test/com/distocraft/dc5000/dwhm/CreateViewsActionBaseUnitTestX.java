package com.distocraft.dc5000.dwhm;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jmock.Expectations;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.testutilities.BaseUnitTestX;


public abstract class CreateViewsActionBaseUnitTestX extends BaseUnitTestX {

  protected RockFactory createMockConnectionAndStatementForDb(final String connectionName, final String statementName,
      final String dbName, final String sql) throws SQLException {
    final Connection dwhdbConnection = context.mock(Connection.class, connectionName);
    final Statement statement = context.mock(Statement.class, statementName);
    final RockFactory mockDb = context.mock(RockFactory.class, dbName);

    context.checking(new Expectations() {

      {
        one(mockDb).getConnection();
        will(returnValue(dwhdbConnection));
        one(dwhdbConnection).createStatement();
        will(returnValue(statement));
        // SQL should be correct when run
        one(statement).executeUpdate(sql);
        one(statement).close();
        // Connection should never be closed in here.
        // Caused failure of TP activation when close was put in this class
        never(dwhdbConnection).close();
      }
    });
    return mockDb;
  }

  protected RockFactory createMockConnectionAndStatementForDbException210(final String connectionName,
      final String statementName, final String dbName, final String sql) throws SQLException {
    final Connection dwhdbConnection = context.mock(Connection.class, connectionName);
    final Statement statement = context.mock(Statement.class, statementName);
    final RockFactory mockDb = context.mock(RockFactory.class, dbName);

    context.checking(new Expectations() {

      {
        allowing(mockDb).getConnection();
        will(returnValue(dwhdbConnection));

        // These could be called at least two times when 210 Exception occurs
        atLeast(2).of(dwhdbConnection).createStatement();
        will(returnValue(statement));
        // SQL should be correct when run
        atLeast(2).of(statement).executeUpdate(sql);
        will(throwException(new SQLException("", "", -210)));
        atLeast(2).of(statement).close();
        // Connection should never be closed in here.
        // Caused failure of TP activation when close was put in this class
        never(dwhdbConnection).close();
      }
    });
    return mockDb;
  }

  protected RockFactory createMockConnectionForDbWithTwoSqlStatements(final String connectionName,
      final String statementName, final String dbName, final String sql1, final String sql2) throws SQLException {
    final Connection dwhdbConnection = context.mock(Connection.class, connectionName);
    final Statement statement = context.mock(Statement.class, statementName);
    final RockFactory mockDb = context.mock(RockFactory.class, dbName);

    context.checking(new Expectations() {

      {
        allowing(mockDb).getConnection();
        will(returnValue(dwhdbConnection));
        allowing(dwhdbConnection).createStatement();
        will(returnValue(statement));
        // SQL should be correct when run
        one(statement).executeUpdate(sql1);
        one(statement).close();
        one(statement).executeUpdate(sql2);
        one(statement).close();
        // Connection should never be closed in here.
        // Caused failure of TP activation when close was put in this class
        never(dwhdbConnection).close();
      }
    });
    return mockDb;
  }

  protected RockFactory createMockConnectionForDbException210WithTwoSqlStatements(final String connectionName,
      final String statementName, final String dbName, final String sql1, final String sql2) throws SQLException {
    final Connection dwhdbConnection = context.mock(Connection.class, connectionName);
    final Statement statement = context.mock(Statement.class, statementName);
    final RockFactory mockDb = context.mock(RockFactory.class, dbName);

    context.checking(new Expectations() {

      {
        allowing(mockDb).getConnection();
        will(returnValue(dwhdbConnection));

        // These could be called at least two times when 210 Exception occurs
        atLeast(2).of(dwhdbConnection).createStatement();
        will(returnValue(statement));
        // SQL should be correct when run
        atLeast(2).of(statement).executeUpdate(sql1);
        will(throwException(new SQLException("", "", -210)));
        // SQL should be correct when run
        atLeast(2).of(statement).executeUpdate(sql2);
        will(throwException(new SQLException("", "", -210)));
        atLeast(2).of(statement).close();
        // Connection should never be closed in here.
        // Caused failure of TP activation when close was put in this class
        never(dwhdbConnection).close();
      }
    });
    return mockDb;
  }
}
