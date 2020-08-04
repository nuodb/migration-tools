# NuoDB Migrator #

[![Build Status](https://travis-ci.org/nuodb/migration-tools.png?branch=master)](https://travis-ci.org/nuodb/migration-tools)

*A command-line interface for helping domain administrators manage backup and migration of their databases.*

This tool is designed to assist you in migrating data from supported SQL databases to a NuoDB database. Use *nuodb-migrator dump*, *nuodb-migrator load*, *nuodb-migrator schema* to copy, normalize, and load data from an existing database (NuoDB or 3rd party) to a NuoDB database.  With the command-line interface, domain administrators will be able to perform the following database backup and migration tasks:

1. Dump schema & data from an existing database to the file system
2. Load schema & data from the file system to a target NuoDB database
3. Generate a NuoDB schema from a source database
4. Copy data & schema from an existing database to a target NuoDB database in one step on the fly [under development]

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
        --help (-h) |
        --version (-v) |
        --help=<[dump] | [load] | [schema]> |
        --list |
        --config=<path>
        <[dump] | [load] | [schema]>

### Dump schema & data from an existing database ###

    $ bin/nuodb-migrator dump
        [source database connection, required]
            --source.driver=driver                                      JDBC driver class name
            --source.url=url                                            Source database connection URL in the standard syntax jdbc:<subprotocol>:<subname>
            [--source.username=[username]]                              Source database username
            [--source.password=[password]]                              Source database password (will prompt if this option is not provided)
            [--source.properties=[properties]]                          Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--source.catalog=[catalog]]                                Default database catalog name to use
            [--source.schema=[schema]]                                  Default database schema name to use
            [--source.auto.commit=[true | false]]                       If set to true each individual statement is treated as a transaction and is automatically committed after it is executed, false by default
            [--source.transaction.isolation=[transaction isolation]]    Sets transaction isolation level, it's a symbolic name or an integer constant of the required level from JDBC standard: none or 0, read.uncommitted or 1, read.committed or 2, repeatable.read or 4, serializable or 8. NuoDB does not support all of the levels, only read.committed or 2, serializable or 8 and also supports two additional levels that are not in the JDBC standard: write.committed or 5, consistent.read or 7
        [output specification, optional]
            [--output.type=[output type]]                               Output type (csv, xml, bson), default is csv
            [--output.path=[output path]]                               Path on the file system
            [--output.*=[attribute value]]                              Output format attributes
        [migration modes, optional]
            [--data=[true | false]]                                     Enables or disables data migration, true by default
            [--schema=[true | false]]                                   Enables or disables schema migration, true by default
        [data migration, optional]
            [table names]
                [--table=table [table ...]]                             Comma separated list of either simple table names or fully qualified names including catalog and schema or table name patterns using regex symbols, where * matches any number of characters and ? symbol to match any single character or mix of table names and table name patterns
                [--table.exclude=table [table ...]]                     Comma separated list of either excluded table names or excluded table name patterns using regex symbol * to match any number of characters and ? to match any single character
            [select statements, optional]
                [--query=query [query ...]]                             Select statement
            [--time.zone (-z)=time zone]                                Time zone enables date columns to be dumped and reloaded between servers in different time zones
            [--query.limit=[query limit]]                               Query limit is a maximum number of rows to split a table into chunks with LIMIT {limit} OFFSET {offset} syntax in a database specific way, where each chunk is written to a separate file. If a query limit is not given or is not supported by the migrator for a particular database queries are not split
        [schema migration, optional]
            [--table.type=[table type [table type ...]]]                Comma separated types of tables (TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, ALIAS, SYNONYM, etc) to process, by default only TABLE type is processed
            [--meta.data.*=[true | false]]                              Includes or excludes specific meta data type (catalog, schema, table, column, primary.key, index, foreign.key, check, sequence, column.trigger) from processing, by default all objects are included
        [executor options, optional]
            [--threads (-t)=[threads]]                                  Number of worker threads, defaults to a number of available processors

### Load schema & data to a target NuoDB database ###

    $ bin/nuodb-migrator load
        [target database connection, required]
           [--target.driver=driver]                                     JDBC driver class name, default is com.nuodb.jdbc.Driver
            --target.url=url                                            Target database connection URL in the format jdbc:com.nuodb://{broker1}:{port1},{broker2}:{port2},..,{brokerN}:{portN}/{database}?{params}
            [--target.username=[username]]                              Target database username
            [--target.password=[password]]                              Target database password (will prompt if this option is not provided)
            [--target.properties=[properties]]                          Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--target.schema=[schema]]                                  Default database schema name to use
        [input specification, required]
            --input.path=[input path]                                   Path on the file system
            [--input.*=[attribute value]]                               Input format attributes
        [migration modes, optional]
            [--data=[true | false]]                                     Enables or disables data migration, true by default
            [--schema=[true | false]]                                   Enables or disables schema migration, true by default
        [data migration, optional]
            [table names]
                [--table=table [table ...]]                             Comma separated list of either simple table names or fully qualified names including catalog and schema or table name patterns using regex symbols, where * matches any number of characters and ? symbol to match any single character or mix of table names and table name patterns
                [--table.exclude=table [table ...]]                     Comma separated list of either excluded table names or excluded table name patterns using regex symbol * to match any number of characters and ? to match any single character
            [commit strategy specification]
                [--commit.strategy=[single | batch | custom]]           Commit strategy name, either single or batch or fully classified class name of a custom strategy implementing com.nuodb.migrator.jdbc.commit.CommitStrategy, default is batch
                [--commit.*=[commit strategy attributes]]               Commit strategy attributes, such as commit.batch.size which is a number of updates to batch for commit point used with batch commit strategy, default is 1000
            [insert type specification]
                [--replace (-r)]                                        Writes REPLACE statements rather than INSERT statements
                [--table.*.replace]                                     Writes REPLACE statement for the specified table
                [--table.*.insert]                                      Writes INSERT statement for the specified
                table
            [--time.zone (-z)=time zone]                                Time zone enables date columns to be dumped and reloaded between servers in different time zones
        [schema migration, optional]
            [type declarations & translations, optional]
                [--use.nuodb.types=[true | false]]                      Instructs the migrator to transform source database types to the best matching NuoDB types, where CHAR, VARCHAR and CLOB source types will be rendered as STRING columns, nuodb-types.properties file is a source of type overrides, the option is false by default
                [--use.explicit.defaults=[true | false]]                Transforms source column implicit default values to NuoDB explicit defaults, the option is false by default
                [--type.name=type name]                                 SQL type name template, i.e. decimal({p},{s}) or varchar({n}), where {p} is a placeholder for a precision, {s} is a scale and {n} is a maximum size
                [--type.code=type code]                                 Integer code of declared SQL type
                [--type.size=[type size]]                               Maximum size of custom data type
                [--type.precision=[type precision]]                     The maximum total number of decimal digits that can be stored, both to the left and to the right of the decimal point. Typically, type precision is in the range of 1 through the maximum precision of 38.
                [--type.scale=[type scale]]                             The number of fractional digits for numeric data types
            [--table.type=[table type [table type ...]]]                Comma separated types of tables (TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, ALIAS, SYNONYM, etc) to process, by default only TABLE type is processed
            [--meta.data.*=[true | false]]                              Includes or excludes specific meta data type (catalog, schema, table, column, primary.key, index, foreign.key, check, sequence, column.trigger) from processing, by default all objects are included
            [--script.type=drop [create]]                               Comma separated types of statements to be generated, default is drop & create
            [--group.scripts.by=[table | meta.data]]                    Group generated DDL scripts, table by default
            [--naming.strategy=[naming strategy]]                       Naming strategy to use, either qualify, hash, auto or class name implementing com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy, default is auto
            [--identifier.quoting=[identifier quoting]]                 Identifier quoting policy name, minimal, always or fully qualified class name implementing com.nuodb.migrator.jdbc.dialect.IdentifierQuoting, default is always
            [--identifier.normalizer=[identifier normalizer]]           Identifier transformer to use, available normalizers are noop, standard, lower.case, upper.case or fully qualified class name implementing com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer, default is noop
        [executor options, optional]
            [--threads (-t)=[threads]]                                  Number of worker threads, defaults to a number of available processors
            [--parallelizer (-p)=[parallelizer]]                        Parallelization strategy name, either table.level (default), row.level or fully classified class name of a custom parallelizer implementing com.nuodb.migrator.backup.loader.Parallelizer. Table level parallelization activates 1 worker thread per table at max, while row level enables forking with more than 1 thread, where the number of worker threads is based on the weight of the loaded row set to the size of loaded tables. Notice row level forking may (and typically does) reorder the rows in the target table.
            [--parallelizer.*=[parallelizer attributes]]                Parallelizer attributes, such as min.rows.per.thread and max.rows.per.thread which are min possible and max allowed number of rows per thread, default are 100000 and 0 (unlimited) correspondingly

### Generate a schema for a target NuoDB database ###

    $ bin/nuodb-migrator schema
        [source database connection, required]
            --source.driver=driver                                      JDBC driver class name
            --source.url=url                                            Source database connection URL in the standard syntax jdbc:<subprotocol>:<subname>
            [--source.username=[username]]                              Source database username
            [--source.password=[password]]                              Source database password (will prompt if this option is not provided)
            [--source.properties=[properties]]                          Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--source.catalog=[catalog]]                                Default database catalog name to use
            [--source.schema=[schema]]                                  Default database schema name to use
            [--source.auto.commit=[true | false]]                       If set to true each individual statement is treated as a transaction and is automatically committed after it is executed, false by default
            [--source.transaction.isolation=[transaction isolation]]    Sets transaction isolation level, it's a symbolic name or an integer constant of the required level from JDBC standard: none or 0, read.uncommitted or 1, read.committed or 2, repeatable.read or 4, serializable or 8. NuoDB does not support all of the levels, only read.committed or 2, serializable or 8 and also supports two additional levels that are not in the JDBC standard: write.committed or 5, consistent.read or 7
        [target database connection, optional]
            [--target.url=url]                                          Target database connection URL in the format jdbc:com.nuodb://{broker}:{port}/{database}
            [--target.username=[username]]                              Target database username
            [--target.password=[password]]                              Target database password (will prompt if this option is not provided)
            [--target.properties=[properties]]                          Additional connection properties encoded as URL query string "property1=value1&property2=value2"
            [--target.schema=[schema]]                                  Default database schema name to use
        [script output, optional]
            --output.path=output path                                   Saves script to a file specified by path
        [table names]
            [--table=table [table ...]]                                 Comma separated list of either simple table names or fully qualified names including catalog and schema or table name patterns using regex symbols, where * matches any number of characters and ? symbol to match any single character or mix of table names and table name patterns
            [--table.exclude=table [table ...]]                         Comma separated list of either excluded table names or excluded table name patterns using regex symbol * to match any number of characters and ? to match any single character
        [type declarations & translations, optional]
            [--use.nuodb.types=[true | false]]                          Instructs the migrator to transform source database types to the best matching NuoDB types, where CHAR, VARCHAR and CLOB source types will be rendered as STRING columns, nuodb-types.properties file is a source of type overrides, the option is false by default
            [--use.explicit.defaults=[true | false]]                    Transforms source column implicit default values to NuoDB explicit defaults, the option is false by default
            [--type.name=type name]                                     SQL type name template, i.e. decimal({p},{s}) or varchar({n}), where {p} is a placeholder for a precision, {s} is a scale and {n} is a maximum size
            [--type.code=type code]                                     Integer code of declared SQL type
            [--type.size=[type size]]                                   Maximum size of custom data type
            [--type.precision=[type precision]]                         The maximum total number of decimal digits that can be stored, both to the left and to the right of the decimal point. Typically, type precision is in the range of 1 through the maximum precision of 38.
            [--type.scale=[type scale]]                                 The number of fractional digits for numeric data types
        [--table.type=[table type [table type ...]]]                    Comma separated types of tables (TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, ALIAS, SYNONYM, etc) to process, by default only TABLE type is processed
        [--meta.data.*=[true | false]]                                  Includes or excludes specific meta data type (catalog, schema, table, column, primary.key, index, foreign.key, check, sequence, column.trigger) from processing, by default all objects are included
        [--script.type=drop [create]]                                   Comma separated types of statements to be generated, default is drop & create
        [--group.scripts.by=[table | meta.data]]                        Group generated DDL scripts, table by default
        [--naming.strategy=[naming strategy]]                           Naming strategy to use, either qualify, hash, auto or class name implementing com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy, default is auto
        [--identifier.quoting=[identifier quoting]]                     Identifier quoting policy name, minimal, always or fully qualified class name implementing com.nuodb.migrator.jdbc.dialect.IdentifierQuoting, default is always
        [--identifier.normalizer=[identifier normalizer]]               Identifier transformer to use, available normalizers are noop, standard, lower.case, upper.case or fully qualified class name implementing com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer, default is noop
        [--fail.on.empty.database=[true | false]]                       If an empty source database is migrated an error will be raised or warn message will be printed to logs depending on the value of this switch. Default is true, which raises error

#### Override database types ####

The migrator allows to override any default database types from the command line or using config file. To override how source column types appear in a generated schema use *--type.name*, *--type.code*, *--type.size*, *--type.precision*, *--type.scale* command line parameters.

*--type.name* is a parameter for type name template with optional placeholders for precision, scale and size variables. For each overridden type *--type.name* template will be used for rendering resulting type name:
* --type.name=CLOB causes to generate CLOB
* --type.name=VARCHAR({N}) results in VARCHAR type where {N} placeholder is substituted by a source column size value
* --type.name=NUMERIC({P},{S}) produces NUMERIC where {P} placeholder stands for precision & {S} is a scale of a source database type

*--type.code* is a parameter, which declares a source database type and it should come in pairs with *--type.name* parameter, to have type template name for each type code. It accepts the following expressions for the type code:
* a fully qualified type name java.sql.Types.CLOB
* short type name, such as CLOB
* integer constant for a required type, such as 2005 for CLOB type
* vendor specific int constant for a required type

*--type.size*, *--type.precision*, *--type.scale* are optional parameters which define (if used) maximum (right) bound of size, precision, scale for a source type and allow to render different target type names depending on source type runtime attributes.

#### Use NuoDB types ####

*--use.nuodb.types* option loads set of type overrides from [conf/nuodb-types.config](https://github.com/nuodb/migration-tools/blob/master/assembly/src/main/assembly/conf/nuodb-types.config) and instructs the migrator to remap source CHAR, VARCHAR & CLOB column types to STRING for every matching source column from a source database during schema generation.

#### Override MySQL TINYINT type ####

MySQL JDBC Driver treats the TINYINT(1) as the BIT type by default, which sometimes is unwanted behaviour.
This is manipulated by *tinyInt1isBit* parameter, found more details at [Configuration Properties for Connector/J](http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-configuration-properties.html).

If you connect as *--source.url=jdbc:mysql://localhost/test?tinyInt1isBit=true* (which is equivalent to omitting *tinyInt1isBit=true*) and since TINYINT(1) is converted by MySQL J/Connector to BIT and NuoDBDialect transforms BIT to BOOLEAN the below table:
```sql
CREATE TABLE `t1`(`f1` TINYINT(1));
```
will be rendered as:
```sql
CREATE TABLE "t1" ("f1" BOOLEAN);
```
You can provide overrides on the command line to transform it to SMALLINT. The combination *--source.url=jdbc:mysql://localhost/test?tinyInt1isBit=true --type.code=java.sql.Types.BIT --type.size=0 --type.name=SMALLINT* will render:
```sql
CREATE TABLE "t1" ("f1" SMALLINT);
```
If you have *tinyInt1isBit=false* set in the url, use *--source.url=jdbc:mysql://localhost/test?tinyInt1isBit=false --type.code=java.sql.Types.TINYINT --type.size=3 --type.name=SMALLINT* to produce SMALLINT. MySQL always reports TINYINT(1) size as 3, notice bug open http://bugs.mysql.com/bug.php?id=38171:
```sql
CREATE TABLE "t1" ("f1" SMALLINT);
```

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
