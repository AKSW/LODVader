package lodVader;

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.stereotype.Component;

import lodVader.bloomfilters.models.LoadedBloomFiltersCache;
import lodVader.configuration.Config;
import lodVader.configuration.LODVaderProperties;
import lodVader.enumerators.DistributionStatus;
import lodVader.enumerators.TuplePart;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.IndexesCreator;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LODVaderCounterDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.GeneralQueries;
import lodVader.utils.FileUtils;

/**
 * start service properly. This class checks whether the application have to
 * keep streaming files (means that app was killed before finish their work),
 * and whether have to create MongoDB indexes
 * 
 * @author Ciro Baron Neto
 *
 */
@Component
public class StartLODVader implements InitializingBean{
	
	final static Logger logger = LoggerFactory.getLogger(StartLODVader.class);
	
	@Autowired
	public Config conf;
	
	@Autowired 
	LODVaderProperties properties;
	
	@Autowired
	MongoDbFactory mongoClient;
	
	@Autowired
	DistributionQueries distributionQueries;
	
	@Autowired
	LODVaderCounterDB c;
	
	@Autowired
	IndexesCreator indexCreator;
	
	@Autowired
	Manager manager;

	@Autowired
	DistributionDB distributionDB;
	
    @Override
    public void afterPropertiesSet() throws Exception {


		try {

			logger.info("==========================================================");
			logger.info("");
			logger.info("");
			logger.info("====================================================");
			logger.info("============== LODVader " + LODVaderProperties.VERSION + " Started ===============");
			logger.info("====================================================");
			logger.info("");
			logger.info("");

			logger.info("Reading properties file.");


			if (LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH == null) {
				properties.loadProperties();
			}

			logger.info("Creating folders..");
			FileUtils.checkIfFolderExists();

			// creating indexes
			logger.info("Creating MongoDB indexes...");

			
			// checking counter
			try {
				c.incrementAndGetID();
			} catch (Exception e) {
				c.setCounterValue(1);
				c.db.insert(false); 
			}

			indexCreator.createIndexes();

			HashMap<Integer, DatasetDB> datasets = new HashMap<Integer, DatasetDB>();

			logger.info("Resuming Downloads...");
			
			if (LODVaderProperties.RESUME) {

				// re-download distributions with "Downloading" status
				ArrayList<String> q = new GeneralQueries().getMongoDBObject(DistributionDB.COLLECTION_NAME,
						DistributionDB.STATUS, DistributionStatus.STREAMING.toString());
				logger.info("re-download distributions with \"" + DistributionStatus.STREAMING + "\" status");

				for (String s : q) {
					DistributionDB dist = conf.getDistributionDB();
					dist.init(s);
					dist.setStatus(DistributionStatus.WAITING_TO_STREAM);
						dist.db.update(true);
				}

				// download distributions with "STATUS_WAITING_TO_STREAM" status
				q = new GeneralQueries().getMongoDBObject(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
						DistributionStatus.WAITING_TO_STREAM.toString());
				logger.info("download distributions with \"" + DistributionStatus.WAITING_TO_STREAM + "\" status");

				for (String s : q) {
					DistributionDB dist = conf.getDistributionDB();
					dist.init(s);
					dist.db.find(true);
        				//if(!dist.getFormat().equals("rdf"))
					DatasetDB dataset = conf.getDatasetDB();
					dataset.init(dist.getTopDatasetID());
					datasets.put(dist.getTopDatasetID(), dataset);
				}

			}

			if (LODVaderProperties.RESUME_ERRORS) {
				// download distributions with "ERROR"
				// status
				ArrayList<String> q = new GeneralQueries().getMongoDBObject(DistributionDB.COLLECTION_NAME,
						DistributionDB.STATUS, DistributionStatus.ERROR.toString());
				logger.info("download distributions with \"" + DistributionStatus.WAITING_TO_STREAM + "\" status");

				for (String s : q) {
					DistributionDB dist = conf.getDistributionDB();
					dist.init(s);
					dist.setStatus(DistributionStatus.WAITING_TO_STREAM);
					dist.db.update(true);
					DatasetDB dataset = conf.getDatasetDB();
					dataset.init(dist.getTopDatasetID());					
					datasets.put(dist.getTopDatasetID(), dataset);
				}
			}

			// load BF for namespaces
			logger.info("Loading nasmespaces... ");

			if (LoadedBloomFiltersCache.describedSubjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
					|| LoadedBloomFiltersCache.describedSubjectsNS == null)
				LoadedBloomFiltersCache.describedSubjectsNS = distributionQueries
						.getDescribedNS(TuplePart.SUBJECT);

			if (LoadedBloomFiltersCache.describedObjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
					|| LoadedBloomFiltersCache.describedObjectsNS == null)
				LoadedBloomFiltersCache.describedObjectsNS = distributionQueries
						.getDescribedNS(TuplePart.OBJECT);

			logger.info("We will resume: " + datasets.size() + " dataset(s).");

			manager.setDatasets(datasets.values());

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
