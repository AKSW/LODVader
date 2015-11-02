LODVader: Visualization, Analytics and DiscovEry in Real-time
==============================================================

Source code of LODVader. For more datails, check our webpage: http://cirola2000.cloudapp.net


## Requirements
This project uses external tools that you must install before start using.
We use MongoDB to save relevant metadata for creation of linksets. Thus, for MongoDB the default installation is sufficient: `sudo apt-get install mongodb-server`. To compile and run the project, you need maven `sudo apt-get install maven` (version > 3.x).

Important!!! After cloning the project from this repository, please access the folder /resources and edit the properties configuration file.

## How to use

#### Instalation process

After cloning the project, open the project root folder and type: `mvn clean install`. Maven will then download all dependencies and compile the project.


#### Starting Jetty server

In order to run the project you need to start the Jetty server using the following command:
`mvn jetty:start`

 Now the server must be acessible at the address:
`http://localhost:9090/dataid/`.

 A good starting point is add a VoID, DCAT or DataID file to you customized cloud. After that, you can use the API:
`http://localhost:9090/dataid/api?addDataset=http://lod-cloud.net/data/void.ttl&rdfFormat=ttl`.

To check you datasets status, you can access: 
`http://localhost:9090/dataid/api?datasetStatus=http://lod-cloud.net/data/void.ttl`

Finaly, you can access the RDF data about the discovered linksets via:
`http://localhost:9090/dataid/api?retrieveDataset=http://lod-cloud.net/`


