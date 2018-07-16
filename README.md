
# notgroupb dataPreformatter

## Overview
Formatting Worker to Format Data from different Data Portals into POJOs. It drops all DataPoints older than 8 hours as they are not representative anymore. Furthermore it deduplicates Datapoints by dropping Points that were already ingested during the last 48 hours.

Architecture Overview:
 - Rectangle: Kafka Topic
 - Rhombus+Annotation: Operation
![Architecture Overview](https://user-images.githubusercontent.com/13851077/42758213-0f1be4c0-8903-11e8-826d-c80be2a00625.png)

The Output Queues have the following Schema:
 - HygonData
	 - Key: String (Stationname)
	 - Value: HygonDataPoint (from [dataFormats](https://github.com/CopernicusEOI2018studyproject/dataFormats))
- PegelOnlineData
	 - Key: String (Stationname)
	 - Value: PegelOnlineDataPoint (from [dataFormats](https://github.com/CopernicusEOI2018studyproject/dataFormats))

## Getting Started
### Prerequisites
 - [Apache Maven](https://maven.apache.org/) for packaging.
### Installing

```bash
mvn package
```
 A jar File will be created in the `/target` directory.

## Deployment
For simplicity and portability the jar File has all dependencies included and can simply be run as a jar executable.

```bash
java -jar target/dataFormatter-1.1.0-jar-with-dependencies.jar 
```


## Versioning

We use [Semamtic Versioning](http://semver.org/) for versioning.

## Authors

* **Jan Speckamp** - *Initial work* - [speckij](https://github.com/speckij)

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details
