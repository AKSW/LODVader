package lodVader;

import java.util.ArrayList;

import javax.servlet.http.HttpServlet;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import lodVader.mongodb.IndexesCreator;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.GeneralQueries;
import lodVader.utils.FileUtils;

/**
 * start service properly. This class checks whether the application have
 * to keep streaming files (means that app was killed before finish their work),
 * and whether have to create MongoDB indexes
 * @author ciro
 *
 */
public class StartLODVader extends HttpServlet {

	private static final long serialVersionUID = 9131804335500741880L;
	final static Logger logger = Logger.getLogger(StartLODVader.class);

	public StartLODVader() {

		new Thread(new Runnable() {

			public void run() {

				try {
					BasicConfigurator.configure();

					LODVaderProperties properties = new LODVaderProperties();

					if (LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH == null) {
						properties.loadProperties();
					}

					FileUtils.checkIfFolderExists();

					// creating indexes
					new IndexesCreator().createIndexes();

					ArrayList<DistributionDB> distributions = new ArrayList<DistributionDB>();

					if (LODVaderProperties.RESUME) {
						
						// re-download distributions with "Downloading" status
						ArrayList<String> q = new GeneralQueries().getMongoDBObject(
								DistributionDB.COLLECTION_NAME,
								DistributionDB.STATUS,
								DistributionDB.STATUS_STREAMING);
						logger.debug("re-download distributions with \""
								+ DistributionDB.STATUS_STREAMING
								+ "\" status");

						for (String s : q) {
							DistributionDB dist = new DistributionDB(
									s);
							dist.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
//							distributions.add(dist);
						}

						// new

						// download distributions with
						// "STATUS_WAITING_TO_STREAM"
						// status
						q = new GeneralQueries()
								.getMongoDBObject(
										DistributionDB.COLLECTION_NAME,
										DistributionDB.STATUS,
										DistributionDB.STATUS_WAITING_TO_STREAM);
						logger.debug("download distributions with \""
								+ DistributionDB.STATUS_WAITING_TO_STREAM
								+ "\" status");

						for (String s : q) {
							DistributionDB dist = new DistributionDB(
									s);
							dist.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
							distributions.add(dist);
						}

					}
					
					if(LODVaderProperties.RESUME_ERRORS){
						// download distributions with "ERROR"
						// status
						ArrayList<String> q = new GeneralQueries().getMongoDBObject(
								DistributionDB.COLLECTION_NAME,
								DistributionDB.STATUS,
								DistributionDB.STATUS_ERROR);
						logger.debug("download distributions with \""
								+ DistributionDB.STATUS_WAITING_TO_STREAM
								+ "\" status");

						for (String s : q) {
							DistributionDB dist = new DistributionDB(
									s);
							dist.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
							distributions.add(dist);
						}
					}

					new Manager(distributions);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
		;

	}

}
