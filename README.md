LODVader: Visualization, Analytics and DiscovEry in Real-time
==============================================================

This is the source code of LODVader API. For the front end repository, please access: https://github.com/cirola2000/LODVader-webpage

For more details and a working demo, check our webpage: http://lodvader.aksw.org

## Software Requirements
1. This project uses MongoDB to save relevant metadata for creation of linksets. The MongoDB default installation is sufficient: `sudo apt-get install mongodb-server`.  
2. To compile and run the project, you need maven. To install, please use apt-get: `sudo apt-get install maven` (version > 3.x).
3. After cloning the project `git clone https://github.com/AKSW/LODVader.git`, please access the folder ./resources/config.properties and:
3.1. edit the MongoDB authentication.
3.2. change the "BASE_PATH" for the directory that LODVader will store data. 
4. LODVader uses GridFS to store Bloom Filters. This means that your database might grow a lot! Ensure the variable "dbpath" in the /etc/mongodb.conf file is setted to the right place.

## Hardware Requirements
The minimum hardware requirement to have an acceptable performance is:
- Quad-core processor (4 real cores, not HT)
- 16GB RAM
- a super fast SSD drive

## How to use

#### Installation process

After cloning the project, open the project root folder and type: `mvn clean install`. Maven will then download all dependencies and compile the project.


#### Starting Jetty server

Before run LODVader, be sure that your JVM will have enough heap space to grow.  Here, we allow to use 28gb of RAM: `export MAVEN_OPTS="-Xmx28g"`

To run the API you need to start the Jetty server using `mvn jetty:start`, and the server must be accessible at the address:  `http://localhost:9090/LODVader/`.

 A good starting point is adding a VoID, DCAT or DataID file to LODVader using the API:
`http://localhost:9090/LODVader/api?addDataset=http://lod-cloud.net/data/void.ttl&rdfFormat=ttl`.

To check you datasets status, you can access: 
`http://localhost:9090/LODVader/api?datasetStatus=http://lod-cloud.net/data/void.ttl`

Finally, you can access the RDF data about the discovered linksets via:
`http://localhost:9090/LODVader/api?retrieveDataset=http://lod-cloud.net/`

To use visualize the LODVader diagram, you can use our front end available in the repository: https://github.com/cirola2000/LODVader-webpage.
