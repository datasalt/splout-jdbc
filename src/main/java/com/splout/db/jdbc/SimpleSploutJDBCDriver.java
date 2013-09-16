package com.splout.db.jdbc;

import com.splout.db.common.SploutClient;
import com.splout.db.qnode.beans.QueryStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleSploutJDBCDriver implements Driver {

  static {
    try {
      DriverManager.registerDriver(new SimpleSploutJDBCDriver());
    } catch (SQLException sqle) {
    }
  }

  SimpleSploutJDBCDriver() {
  }

  /**
   * Attempts to make a database connection to the given URL.
   * The driver should return "null" if it realizes it is the wrong kind
   * of driver to connect to the given URL.  This will be common, as when
   * the JDBC driver manager is asked to connect to a given URL it passes
   * the URL to each loaded driver in turn.
   * <p/>
   * <P>The driver should throw an <code>SQLException</code> if it is the right
   * driver to connect to the given URL but has trouble connecting to
   * the database.
   * <p/>
   * <P>The <code>java.util.Properties</code> argument can be used to pass
   * arbitrary string tag/value pairs as connection arguments.
   * Normally at least "user" and "password" properties should be
   * included in the <code>Properties</code> object.
   *
   * @param url  the URL of the database to which to connect
   * @param info a list of arbitrary string tag/value pairs as
   *             connection arguments. Normally at least a "user" and
   *             "password" property should be included.
   * @return a <code>Connection</code> object that represents a
   *         connection to the URL
   * @throws java.sql.SQLException if a database access error occurs
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (url.length() < 5) {
      throw new SQLException("Malformed connection string");
    }

    // removing "jdbc:"
    url = url.substring(5);
    try {
      URI uri = new URI(url);
      if (!"splout".equals(uri.getScheme())) {
        // Normal behavior for JDBC drivers
        return null;
      }

      int port = uri.getPort();
      String finalUrl = "http://" + uri.getHost() + (port != -1 ? ":" + port : "");

      String errorNoKeyProvided = "Non 'key' parameter provided in the connection string. It is mandatory to provide one or several key parameters that will be used to try to autodetect the partitioning columns on queries";
      String query = uri.getQuery();
      if (query == null) {
        throw new SQLException(errorNoKeyProvided);
      }

      String [] keys = parseQueryParams(query, "key");
      if (keys.length == 0) {
        throw new SQLException(errorNoKeyProvided);
      }

      String [] tablespaces = parseQueryParams(query, "tablespace");
      if (tablespaces.length == 0) {
        throw new SQLException("A tablespace must be provided using the 'tablespace' parameter");
      }

      return new SploutConnection(finalUrl,tablespaces[tablespaces.length-1] ,keys);

    } catch (URISyntaxException e) {
      throw new SQLException("Malformed connection string", e);
    }
  }

  private static String[] parseQueryParams(String query, String paramName) {
    Pattern pattern = Pattern.compile(paramName + "=['\"](([^'\"]*))['\"]");
    Matcher matcher = pattern.matcher(query);
    ArrayList<String> values = new ArrayList<String>();
    while (matcher.find()) {
      values.add(matcher.group(1));
    }
    return values.toArray(new String[0]);
  }

  /**
   * Retrieves whether the driver thinks that it can open a connection
   * to the given URL.  Typically drivers will return <code>true</code> if they
   * understand the subprotocol specified in the URL and <code>false</code> if
   * they do not.
   *
   * @param url the URL of the database
   * @return <code>true</code> if this driver understands the given URL;
   *         <code>false</code> otherwise
   * @throws java.sql.SQLException if a database access error occurs
   */
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url.length() < 5) {
      return false;
    }

    // removing "jdbc:"
    url = url.substring(5);
    try {
      URI uri = new URI(url);
      if ("splout".equals(uri.getScheme())) {
        // Normal behavior for JDBC drivers
        return true;
      }
    } catch (URISyntaxException e) {
    } finally {
      return false;
    }
  }

  /* ----------------------------------------------- */
  /*           UNIMPLEMENTED METHODS
  /* ----------------------------------------------- */

  /**
   * Gets information about the possible properties for this driver.
   * <p/>
   * The <code>getPropertyInfo</code> method is intended to allow a generic
   * GUI tool to discover what properties it should prompt
   * a human for in order to get
   * enough information to connect to a database.  Note that depending on
   * the values the human has supplied so far, additional values may become
   * necessary, so it may be necessary to iterate though several calls
   * to the <code>getPropertyInfo</code> method.
   *
   * @param url  the URL of the database to which to connect
   * @param info a proposed list of tag/value pairs that will be sent on
   *             connect open
   * @return an array of <code>DriverPropertyInfo</code> objects describing
   *         possible properties.  This array may be an empty array if
   *         no properties are required.
   * @throws java.sql.SQLException if a database access error occurs
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  /**
   * Retrieves the driver's major version number. Initially this should be 1.
   *
   * @return this driver's major version number
   */
  @Override
  public int getMajorVersion() {
    return 1;
  }

  /**
   * Gets the driver's minor version number. Initially this should be 0.
   *
   * @return this driver's minor version number
   */
  @Override
  public int getMinorVersion() {
    return 0;
  }

  /**
   * Reports whether this driver is a genuine JDBC
   * Compliant<sup><font size=-2>TM</font></sup> driver.
   * A driver may only report <code>true</code> here if it passes the JDBC
   * compliance tests; otherwise it is required to return <code>false</code>.
   * <p/>
   * JDBC compliance requires full support for the JDBC API and full support
   * for SQL 92 Entry Level.  It is expected that JDBC compliant drivers will
   * be available for all the major commercial databases.
   * <p/>
   * This method is not intended to encourage the development of non-JDBC
   * compliant drivers, but is a recognition of the fact that some vendors
   * are interested in using the JDBC API and framework for lightweight
   * databases that do not support full database functionality, or for
   * special databases such as document information retrieval where a SQL
   * implementation may not be feasible.
   *
   * @return <code>true</code> if this driver is JDBC Compliant; <code>false</code>
   *         otherwise
   */
  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  /**
   * Return the parent Logger of all the Loggers used by this driver. This
   * should be the Logger farthest from the root Logger that is
   * still an ancestor of all of the Loggers used by this driver. Configuring
   * this Logger will affect all of the log messages generated by the driver.
   * In the worst case, this may be the root Logger.
   *
   * @return the parent Logger for this driver
   * @throws java.sql.SQLFeatureNotSupportedException
   *          if the driver does not use <code>java.util.logging<code>.
   * @since 1.7
   */
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new RuntimeException("Not implemented");
  }

  public static void main(String args[]) throws URISyntaxException, IOException {
    String a = "jdbc:mysql://localhost/test?pepe='2'&luis='3'";


    a = a.substring(5);
    URI uri = new URI(a);
    //URL urk = new URL(a);
    uri.parseServerAuthority();
    System.out.println(uri);
    System.out.println("aut " + uri.getAuthority());
    System.out.println("frag " + uri.getFragment());
    System.out.println("port " + uri.getPort());
    System.out.println("host " + uri.getHost());
    System.out.println("path " + uri.getPath());
    System.out.println("query " + uri.getQuery());
    System.out.println("scheme " + uri.getScheme());
    uri.parseServerAuthority();

    Pattern pattern = Pattern.compile("key=([^;])*[;](.*)");
    Matcher matcher = pattern.matcher("key=DUE; select * from bblablba where key = 23.12 and key=12,323.42 and key =' pepe ' and key= \"luis\";");
    System.out.println(matcher.matches());
    // Check all occurance
    while (matcher.find()) {
      System.out.print("Start index: " + matcher.start());
      System.out.print(" End index: " + matcher.end() + " ");
      System.out.print(matcher.group());

      for(int i = 0; i<matcher.groupCount(); i++) {
        System.out.print(" group"+i+": " + matcher.group(i) );
      }
      System.out.println();
    }

    SploutClient client = new SploutClient("http://localhost:4412");
    QueryStatus status = client.query("city_pby_country_code", "DEU", "select * from city order by id desc", null);
    status.getResult();


  }

}
