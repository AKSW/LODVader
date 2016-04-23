package lodVader;

import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.LoggerFactory;

import lodVader.bloomfilters.models.LoadedBloomFiltersCache;
import lodVader.enumerators.DistributionStatus;
import lodVader.enumerators.TuplePart;
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
public class StartLODVader {

	private static final long serialVersionUID = 9131804335500741880L;
	final static Logger logger = LoggerFactory.getLogger(StartLODVader.class);

	public StartLODVader() {

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

			LODVaderProperties properties = new LODVaderProperties();

			if (LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH == null) {
				properties.loadProperties();
			}

			logger.info("Creating folders..");
			FileUtils.checkIfFolderExists();

			// creating indexes
			logger.info("Creating MondoDB indexes...");

			// checking counter
			try {
				new LODVaderCounterDB().incrementAndGetID();
			} catch (Exception e) {
				LODVaderCounterDB c = new LODVaderCounterDB();
				c.setCounterValue(1);
				c.insert(false);
			}

			new IndexesCreator().createIndexes();

			HashMap<Integer, DatasetDB> datasets = new HashMap<Integer, DatasetDB>();

			logger.info("Resuming Downloads...");
			
			if (LODVaderProperties.RESUME) {

				// re-download distributions with "Downloading" status
				ArrayList<String> q = new GeneralQueries().getMongoDBObject(DistributionDB.COLLECTION_NAME,
						DistributionDB.STATUS, DistributionStatus.STREAMING.toString());
				logger.debug("re-download distributions with \"" + DistributionStatus.STREAMING + "\" status");

				for (String s : q) {
					DistributionDB dist = new DistributionDB(s);
					dist.setStatus(DistributionStatus.WAITING_TO_STREAM);
					dist.update(true);
				}

				// download distributions with "STATUS_WAITING_TO_STREAM" status
				q = new GeneralQueries().getMongoDBObject(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
						DistributionStatus.WAITING_TO_STREAM.toString());
				logger.debug("download distributions with \"" + DistributionStatus.WAITING_TO_STREAM + "\" status");

				for (String s : q) {
					DistributionDB dist = new DistributionDB(s);
					dist.update(true);
					datasets.put(dist.getTopDatasetID(), new DatasetDB(dist.getTopDatasetID()));
				}

			}

			if (LODVaderProperties.RESUME_ERRORS) {
				// download distributions with "ERROR"
				// status
				ArrayList<String> q = new GeneralQueries().getMongoDBObject(DistributionDB.COLLECTION_NAME,
						DistributionDB.STATUS, DistributionStatus.ERROR.toString());
				logger.debug("download distributions with \"" + DistributionStatus.WAITING_TO_STREAM + "\" status");

				for (String s : q) {
					DistributionDB dist = new DistributionDB(s);
					dist.setStatus(DistributionStatus.WAITING_TO_STREAM);
					dist.update(true); 
					datasets.put(dist.getTopDatasetID(),new DatasetDB(dist.getTopDatasetID()));
				}
			}

			// load BF for namespaces
			logger.info("Loading nasmespaces... ");

			if (LoadedBloomFiltersCache.describedSubjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
					|| LoadedBloomFiltersCache.describedSubjectsNS == null)
				LoadedBloomFiltersCache.describedSubjectsNS = new DistributionQueries()
						.getDescribedNS(TuplePart.SUBJECT);

			if (LoadedBloomFiltersCache.describedObjectsNSCurrentSize > LODVaderProperties.BF_BUFFER_RANGE
					|| LoadedBloomFiltersCache.describedObjectsNS == null)
				LoadedBloomFiltersCache.describedObjectsNS = new DistributionQueries()
						.getDescribedNS(TuplePart.OBJECT);

			logger.info("We will resume: " + datasets.size() + " dataset(s).");

			new Manager(datasets.values());

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
