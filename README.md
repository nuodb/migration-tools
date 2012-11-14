# migration-tools #

*A command line interface to help domain administrators migrate a legacy database to NuoDB.*

The NuoDB Customer Migration Data tool is a command-line interface designed to assist you in migrating legacy data from any SQL database to a NuoDB database. Use nuodb-migration dump and nuodb-migration load to copy, normalize, and load data from an existing database to a NuoDB database.

## Building from Source

        $ git clone https://github.com/nuodb/migration-tools
        $ cd migration-tools/
    	$ mvn install
        $ cd assembly/target/nuodb-migration/bin/
        $ ./nuodb-migration --help
        
## Synopsis ##

    $ ./nuodb-migration 
    	--help=<[dump] [load]> | 
    	--list | 
    	--config=<file> 
    	<[dump] [load]>

--------

    $ ./nuodb-migration dump 
    	--source.driver=<driver> | 
    	--source.url=<url> | 
    	--source.username=<username> | 
    	--source.password=<password> | 
    	--source.properties=<properties> | 
    	--source.catalog=<catalog> | 
    	--source.schema=<schema> | 
    	--output.type=<output type> | 
    	--output.path=<output path> | 
    	--output.*=<attribute value> | 
    	--table=<table> | 
    	--table.*.filter=<query filter> | 
    	--query=<query>

--------

    $ ./nuodb-migration load 
    	--target.url=<url> |
    	--target.username=<username> | 
    	--target.password=<password> | 
    	--target.properties=<properties> | 
    	--target.schema=<schema> | 
    	--input.type=<input type> | 
    	--input.path=<input path> | 
    	--input.*=<attribute value>

## Connect to Third-party Databases ##

To interface with third-party databases through JDBC-compliant drivers you should download & install appropriate JAR files.

Add required dependency to pom.xml, clean & package project. For example, to add MySQL JDBC connector:

    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        ...
        <dependencies>
            <dependency>
                <groupId>mysql</groupId>
                <artifactId>mysql-connector-java</artifactId>
                <version>5.1.20</version>
            </dependency>
        </dependencies>
        ...
    </project>

--------

    $ mvn clean install
    
Required JAR file will be download to assembly/target/nuodb-migration/jar/ directory

Alternatively, you can download & copy required JAR file to assembly/target/nuodb-migration/jar/ manually. For example, to install PosgreSQL JDBC4 Driver:

    $ mvn clean install
    $ curl http://jdbc.postgresql.org/download/postgresql-9.2-1001.jdbc4.jar > assembly/target/nuodb-migration/jar/postgresql-9.2-1001.jdbc4.jar
    
## Examples ##

The following examples show how to dump MySQL to a file (in the first case), and an existing NuoDB database (in the second case).  The third case shows how to use the load command. 

Example 1: Dump all tables from MySQL "enron" catalog in CSV format

    $ ./nuodb-migration dump --source.driver=com.mysql.jdbc.Driver \
        --source.url=jdbc:mysql://localhost:3306/enron --source.catalog=enron \
        --source.username=root --output.type=csv --output.path=/tmp/test/dump.cat

----

Example 2: Dump records from hockey table where "id" does not equal 25 from NuoDB "test" catalog in BSON format

    $ ./nuodb-migration dump --source.driver=com.nuodb.jdbc.Driver \  
        --source.url=jdbc:com.nuodb://localhost/test --source.username=dba \
        --source.password=goalie \ 
        --source.schema=hockey --table=hockey --table.hockey.filter=id<>25 \  
        --output.type=bson --output.path=/tmp/test/dump.cat

----

Example 3:  Load CSV data to the corresponding tables in NuoDB from /tmp/test/dump.cat

    $ ./nuodb-migration load --target.url=jdbc:com.nuodb://localhost/test \
        --target.username=dba --target.password=goalie \
        --input.path=/tmp/test/dump.cat


## Command Line Options ##

### nuodb-migration

<table>

<tr><td>
[--help=[command]] 
</td><td>
Prints help contents on the requested command 
</td></tr>

<tr><td>
[--list]
</td><td>
Lists available migration commands
</td></tr>

<tr><td>
[--config=file]
</td><td>
Reads definition of the migration process from the XML file and executes it
</td></tr>

<tr><td>
[command]
</td><td>
Executes specified migration command (dump or load) 
</td></tr>

</table>

### nuodb-migration dump
<table>
<tr><td colspan="2">
Source database connection options
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--source.driver=driver
</td><td>
JDBC driver class name
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--source.url=url
</td><td>
Source database connection URL in the standard syntax jdbc:&lt;subprotocol&gt;:&lt;subname&gt;
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.username=[username]]
</td><td>
Source database username
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.password=[password]]
</td><td>
Source database password
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.properties=[properties]]
</td><td>
Additional connection properties encoded as URL query string . For example: "property1=value1&property2=value2"
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.catalog=[catalog]]
</td><td>
Default database catalog name to use
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.schema=[schema]]
</td><td>
Default database schema name to use output specification  
</td></tr>

<tr><td colspan="2">
Output specification options
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--output.type=output type
</td><td>
Output type (cvs, xml, bson)
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--output.path=[output path]
</td><td>
Path on the file system
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--output.*=[attribute value]]
</td><td>
Output format attributes, i.e. -output.csv.delimiter, -output.csv.quoting, -output.csv.quote, -output.csv.escape, -output.csv.line.separator overriding attributes from com.nuodb.migration.result.format.csv.CsvAttributes; -output.xml.encoding, -output.xml.version options for XML to override default values of com.nuodb.migration.result.format.xml.XmlAttributes
</td></tr>

<tr><td colspan="2">
Table names and select filters
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--table=table]
</td><td>
Table name
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--table.*.filter=[query filter]]
</td><td>
Filters table records when appended to the query statement after the where clause
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--query=query]
</td><td>
Full select statement to dump
</td></tr>

</table>

### nuodb-migration load ###
<table>

<tr><td colspan="2">
NuoDB connection options
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--target.url=url
</td><td>
Target database connection URL in format jdbc:com.nuodb://{BROKER}:{PORT}/{DATABASE}
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.username=[username]]
</td><td>
Target database username
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.password=[password]]
</td><td>
Target database password
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.properties=[properties]]
</td><td>
Additional connection properties encoded as URL query string such as  "property1=value1&property2=value2"
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.schema=[schema]]
</td><td>
Default database schema name to use input specification
</td></tr>

<tr><td colspan="2">
Input specification options
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--input.path=[input path]
</td><td>
Path on the file system
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--input.*=[attribute value]]
</td><td>
Input format attributes
</td></tr>

</table>