# O&G Liability Tracker

This application is meant to be a stand-alone liability management tool to help companies track their LMR ratings, and forecast future ratios.

Initially only Alberta will be supported, but other provinces will be added eventually.

## Building instructions

The Oracle JDBC driver needs to be installed to a local Maven repository for this application to build and run:
 
```
mvn install:install-file -Dfile=.\lib\ojdbc7.jar -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.2 -Dpackaging=jar
```