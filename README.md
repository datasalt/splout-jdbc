splout-jdbc
===========

This project contains a simple JDBC Driver for connecting with Splout SQL. It is intendended to be the seed
of a future JDBC Driver for Splout SQL. We are looking for contributors that can help with that. Contact us if 
you are interested. 

Idea
----

Splout is an SQL database, so connecting to it using a JDBC driver should be possible. But there
is a problem related with the nature of the Splout SQL design: data is partitioned and a key
must be provided with each query so that Splout can route the query to the proper partition. 

That must be autodetected by the JDBC Driver, but this is not possible in all cases. For this first
version, we support several possibilities for the key autodetection:

  * Autodetecting from the query. You should provide the name of the columns used for partitioning in the
    connection string (i.e. jdbc:splout://localhost:4412?key='country_code'&tablespace='city_pby_country_code').
    The driver will autodetect the key in simple queries like that: <pre>select * from city where country_code = 'AFG';</pre>
  * An explicit key can be provided by prepending the query with "key=<key>;". For example, the following 
    query uses explicit key submission: <pre>key=AFG; select * from city where country_code = cCode("Afganistan");</pre>
    
A particular tablespace must be also provided in the connection string. 

Limitations:
------------
The current implementation is very simple and not complete. Particulary, it has the following limitations:

  * No server cursors supported. All the data from the given query is loaded in memory to provide the ResultSet
    functionality.
  * ResultSetMetaData is not implemented. For implementing it, it would be needed a change in the Splout SQL
    query API to provide more information about the columns names and types, even in the case of an empty
    result
  * Key autodetection is very weak. It only works if the column used for the partition is used in the query
    in the format <pre>column=<value></pre> or <pre>column='<value>'</pre>. 
  * Not all methods are implemented. The driver only covers the most important functions, but is far to be
    complete. 
    
Example of usage:
----------------
<pre>
    Class driver = Class.forName("com.splout.db.jdbc.SimpleSploutJDBCDriver");
    Connection conn = DriverManager.getConnection("jdbc:splout://localhost:4412?key='country_code'&tablespace='city_pby_country_code'");
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from city where country_code = 'AFG';");
    while(rs.next()){
      for (int i=1; i<6; i++) {
        System.out.print(rs.getObject(i) + "\t");
      }
      System.out.println();
    }
    stmt.close();
    conn.close();
</pre>


