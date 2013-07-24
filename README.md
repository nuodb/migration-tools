# NuoDB Migrator #

[![Build Status](https://travis-ci.org/nuodb/migration-tools.png?branch=master)](https://travis-ci.org/nuodb/migration-tools)

*A command-line interface for helping domain administrators manage backup and migration of their databases.*

This tool is designed to assist you in migrating data from supported SQL databases to a NuoDB database. Use *nuodb-migrator dump*, *nuodb-migrator load*, *nuodb-migrator schema* to copy, normalize, and load data from an existing database (NuoDB or 3rd party) to a NuoDB database.  With the command-line interface, domain administrators will be able to perform the following database backup and migration tasks:

1. Migrate (generate) a schema to a target NuoDB database
2. Copy data from an existing database to a target NuoDB database
3. Dump data from an existing database
4. Load data to a target NuoDB database

*These functions tested on MySQL, MSSQL Server, Oracle, PostgreSQL, IBM DB2, Sybase Adaptive Server Enterprise and supposed to work with any JDBC-compliant database.*

## Building from Source ##

    $ git clone https://github.com/nuodb/migration-tools
    $ cd migration-tools/
    $ mvn install
    $ cd assembly/target/nuodb-migrator/
    $ bin/nuodb-migrator --help

## Synopsis ##

### Root command line options ###

    $ bin/nuodb-migrator
        --help |
        --help=<[dump] | [load] | [schema]> |
        --list |
        --config=<file>
        <[dump] | [load] | [schema]>

### Dump data from an existing database ###

    $ bin/nuodb-migrator dump                                                
        [source database connection, required]                       
            --source.driver=driver                          JDBC driver class name
            --source.url=url                                Source database connection URL in the standard syntax jdbc:<subprotocol>:<subname>
            [--source.username=[username]]                  Source database username
            [--source.password=[password]]                  Source database password
            [--source.properties=[properties]]              Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--source.catalog=[catalog]]                    Default database catalog name to use
            [--source.schema=[schema]]                      Default database schema name to use
        [output specification, required]                              
            --output.type=output type                       Output type (CVS, XML, BSON)
            [--output.path=[output path]]                   Path on the file system
            [--output.*=[attribute value]]                  Output format attributes
        [table names, types & query filters, optional]              
            [--table=table [table ...]]                     Table name
            [--table.type=[table type [table type ...]]]    Comma separated types of tables (TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, ALIAS, SYNONYM, etc) to process, by default only TABLE is included into dump
            [--table.*.filter=[query filter]]               Filters table records using specified filter by appending it to the SELECT statement after WHERE clause
        [select statements, optional]                               
            [--query=query]                                 Select statement
        [--time.zone (-z)=time zone]                       Time zone enables date columns to be dumped and reloaded between servers in different time zones
        [--threads (-t)=[threads]]                          Number of worker threads (experimental), defaulted to the number of available processors
        [--query.limit=[query limit]]                       Query limit (experimental) is used to split table into chunks with LIMIT {limit} OFFSET {offset}, by default queries are not limited

### Load data to a target NuoDB database ###

    $ bin/nuodb-migrator load
        [target database connection, required]
            --target.url=url                      Target database connection URL in the format jdbc:com.nuodb://{broker}:{port}/{database}
            [--target.username=[username]]        Target database username
            [--target.password=[password]]        Target database password
            [--target.properties=[properties]]    Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--target.schema=[schema]]            Default database schema name to use
        [input specification, required]
            --input.path=[input path]             Path on the file system
            [--input.*=[attribute value]]         Input format attributes
        [--time.zone (-z)=time zone]              Time zone enables date columns to be dumped and reloaded between servers in different time zones

### Generate a schema for a target NuoDB database ###

    $ bin/nuodb-migrator schema
        [source database connection, required]                           
            --source.driver=driver                             JDBC driver class name
            --source.url=url                                   Source database connection URL in the standard syntax jdbc:<subprotocol>:<subname>
            [--source.username=[username]]                     Source database username
            [--source.password=[password]]                     Source database password
            [--source.properties=[properties]]                 Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--source.catalog=[catalog]]                       Default database catalog name to use
            [--source.schema=[schema]]                         Default database schema name to use
        [target database connection, optional]                         
            [--target.url=url]                                 Target database connection URL in the format jdbc:com.nuodb://{broker}:{port}/{database}
            [--target.username=[username]]                     Target database username
            [--target.password=[password]]                     Target database password
            [--target.properties=[properties]]                 Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--target.schema=[schema]]                         Default database schema name to use
        [script output, optional]                                      
            --output.path=output path                          Saves script to a file specified by path
        [custom type declarations, optional]                           
            [--type.name=type name]                            SQL type name template, i.e. decimal({p},{s}) or varchar({n}), where {p} is a placeholder for a precision, {s} is a scale and {n} is a maximum size
            [--type.code=type code]                            Integer code of declared SQL type
            [--type.size=[type size]]                          Maximum size of custom data type
            [--type.precision=[type precision]]                The maximum total number of decimal digits that can be stored, both to the left and to the right of the decimal point. Typically, type precision is in the range of 1 through the maximum precision of 38.
            [--type.scale=[type scale]]                        The number of fractional digits for numeric data types
        [--meta.data.*=[true | false]]                         Includes of excludes specific meta data type (catalog, schema, table, column, primary.key, index, foreign.key, check.constraint, auto.increment) from the generated output, by default all objects are generated
        [--script.type=drop [create]]                          Comma separated types of statements to be generated, default is drop & create
        [--group.scripts.by=[table | meta.data]]               Group generated DDL scripts, table by default
        [--identifier.quoting=[identifier quoting]]            Identifier quoting policy name, minimal, always or fully qualified class name implementing com.nuodb.migrator.jdbc.dialect.IdentifierQuoting, default is always
        [--identifier.normalizer=[identifier normalizer]]      Identifier transformer to use, available normalizers are noop, standard, lower.case, upper.case or fully qualified class name implementing com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer, default is noop
## Connect to Third-party Databases ##

To interface with third-party databases through JDBC-compliant drivers you should download & install appropriate JAR files.

You can add required dependency to pom.xml, then clean & package project:

    $ mvn clean install

The required JDBC driver JAR file will be download automatically to the assembly/target/nuodb-migrator/jar/ directory

Alternatively, you can download & copy required JAR file to assembly/target/nuodb-migrator/jar/ manually. For example, to install PostgreSQL JDBC4 Driver:

    $ mvn clean install
    $ curl http://jdbc.postgresql.org/download/postgresql-9.2-1001.jdbc4.jar > \
        assembly/target/nuodb-migrator/jar/postgresql-9.2-1001.jdbc4.jar

To include MySQL JDBC Driver into assembly using Maven add the following dependency to pom.xml, then clean & package project:

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

For Microsoft SQL Server you might use jTDS Type 4 JDBC Driver:

    <dependency>
        <groupId>net.sourceforge.jtds</groupId>
        <artifactId>jtds</artifactId>
        <version>1.2.4</version>
    <dependency>

For PostgreSQL add PostgreSQL Native Driver of the required version to the pom.xml file:

    <dependency>
        <groupId>postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>9.0-801.jdbc4</version>
    </dependency>

## Examples ##

The following examples show how to dump MySQL to a file (in the first case), and an existing NuoDB database (in the second case).  The third case shows how to use the load command.

Example 1: Dump all tables from MySQL "enron" catalog in CSV format

    $ bin/nuodb-migrator dump --source.driver=com.mysql.jdbc.Driver \
        --source.url=jdbc:mysql://localhost:3306/test --source.catalog=test \
        --source.username=<username> \
        --output.type=csv --output.path=/tmp/dump.cat

Example 2: Dump records from "hockey" table where "id" is not equal to 25 from NuoDB "test" catalog in BSON format

    $ bin/nuodb-migrator dump --source.driver=com.nuodb.jdbc.Driver \
        --source.url=jdbc:com.nuodb://localhost/test \
        --source.username=<username> \
        --source.schema=hockey --table=hockey --table.hockey.filter=id<>25 \
        --output.path=/tmp/dump.cat --output.type=bson

Example 3: Load CSV data to the corresponding tables in NuoDB from /tmp/dump.cat

    $ bin/nuodb-migrator load --target.url=jdbc:com.nuodb://localhost/test \
        --target.username=<username> --target.password=<password> \
        --input.path=/tmp/dump.cat

Example 4: Generate NuoDB schema from Oracle "test" database and output it to stdout stream. To save schema in a particular file on the file system add --output.path=<file> option, i.e. --output.path=/tmp/schema.sql

    $ bin/nuodb-migrator schema --source.driver=oracle.jdbc.driver.OracleDriver \
        --source.url=jdbc:oracle:thin:@//localhost:1521/test \
        --source.username=<username> --source.password=<password> --source.schema=test

Example 5: Migrate schema from Microsoft SQL Server "test" database to a NuoDB database excluding generation of foreign keys and check constraints on table and column levels. Generated table names and column identifiers will be transformed using "standard" identifier normalizer (changed to upper case if unquoted).

    $ bin/nuodb-migrator schema --source.driver=net.sourceforge.jtds.jdbc.Driver \
        --source.url=jdbc:jtds:sqlserver://localhost:1433/test \
        --source.username=<username> --source.password=<password> --source.schema=dbo \
        --target.url=jdbc:com.nuodb://localhost/test \
        --target.username=<username> --target.password=<password> --target.schema=hockey \
        --meta.data.foreign.key=false --meta.data.check.constraint=false \
        --identifier.normalizer=standard

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/6b3314b32dd6c95ab4e2cde9bb3c6f74 "githalytics.com")](http://githalytics.com/nuodb/migration-tools)

