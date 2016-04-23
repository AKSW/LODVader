package lodVader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.bloomfilters.models.DatasetLinksContainer;
import lodVader.enumerators.DistributionStatus;
import lodVader.lov.LOV;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.SystemPropertiesDB;
import lodVader.streaming.CheckWhetherToStream;
import lodVader.streaming.StreamAndProcess;
import lodVader.streaming.StreamAndSaveDump;
import lodVader.streaming.SuperStream;

public class Manager {
	final static Logger logger = LoggerFactory.getLogger(Manager.class);

	// list of subset and their distributions
	private static Queue<DatasetDB> datasets = new LinkedBlockingQueue<DatasetDB>();

	// tmp links between distribution -> dataset
	public static ArrayList<DatasetLinksContainer> datasetLinkContainer = new ArrayList<DatasetLinksContainer>(); 

	// list of datasets in the queue to be streamed

	boolean consumingQueue = false;

	public void streamAndCreateFilters() throws Exception {

		consumingQueue = true;

		// while we still have datasets in the queue, keep reading
		while (datasets.size() > 0) {

			logger.info("We still have " + datasets.size() + " datasets in the queue...");

			DatasetDB dataset = datasets.remove();

			// iterate on distributions of current dataset
			for (DistributionDB distribution : dataset.getDistributionsAsMongoDBObjects()) {

				logger.info("Processing distribution: " + distribution.getUri());

				distribution.setStatus(DistributionStatus.WAITING_TO_STREAM);

				// check is distribution need to be streamed
				boolean needDownload = checkDistributionStatus(distribution);
				// boolean needDownload = true;

				if (!needDownload) {
					logger.info("Distribution is already in the last version. No needs to stream again. ");
					distribution.setLastMsg("Distribution is already in the last version. No needs to stream again.");
					distribution.update(true);
				}

				// if distribution have not already been handled
				if (needDownload)
					try {

						// uptate status of distribution to streaming
						distribution.setStatus(DistributionStatus.STREAMING);
						distribution.update(true);

						// now we need to download the distribution
						SuperStream streamFile;
						if (!LODVaderProperties.ONLY_STREAM_DATASETS_AND_SAVE_NT_FORMAT)
							streamFile = new StreamAndProcess(distribution);
						else
							streamFile = new StreamAndSaveDump(distribution);

						logger.info("Streaming distribution.");

						streamFile.streamDistribution();

						// uptate status of distribution
						distribution.find(true);
						distribution.setStatus(DistributionStatus.STREAMED);
						distribution.update(true);

						logger.debug("Distribution streamed. ");

						logger.debug("Saving mongodb \"Distribution\" document.");

						distribution.setNumberOfObjectTriples(streamFile.objectLines);
						distribution.setNumberOfSubjectTriples(streamFile.subjectLines);
						distribution.setDownloadUrl(streamFile.downloadUrl.toString());
						distribution.setFormat(streamFile.extension.toString());
						distribution.setHttpByteSize(String.valueOf((int) streamFile.httpContentLength));
						distribution.setHttpFormat(streamFile.httpContentType);
						distribution.setHttpLastModified(streamFile.httpLastModified);
						distribution.setTriples(streamFile.totalTriples);

						distribution.setSuccessfullyDownloaded(true);
						distribution.update(true);

						// logger.debug("Checking Similarity among
						// distributions...");
						// distributionMongoDBObj.setStatus(DistributionDB.STATUS_CREATING_JACCARD_SIMILARITY);
						// distributionMongoDBObj.update(true);
						// Saving link similarities

						// logger.debug("Checking Jaccard Similarities...");
						// // Checking Jaccard Similarities...
						// LinkSimilarity linkSimilarity = new
						// JaccardSimilarity();
						// linkSimilarity.updateLinks(distributionMongoDBObj,
						// new AllPredicatesRelationDB());
						// linkSimilarity.updateLinks(distributionMongoDBObj,
						// new RDFTypeObjectRelationDB());
						// linkSimilarity.updateLinks(distributionMongoDBObj,
						// new RDFSubClassOfRelationDB());
						// linkSimilarity.updateLinks(distributionMongoDBObj,
						// new OwlClassRelationDB());

						logger.debug("Updating link strength among distributions...");
						distribution.setStatus(DistributionStatus.UPDATING_LINK_STRENGTH);
						distribution.update(true);
						// Saving link similarities
						// LinkStrength linkStrength = new LinkStrength();
						// linkStrength.updateLinks(distributionMongoDBObj);

						logger.debug("Done streaming mongodb distribution object.");

						// uptate status of distribution
						distribution.setStatus(DistributionStatus.DONE);

						DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss dd/MM/yyyy");
						// get current date time with Date()
						Date date = new Date();

						distribution.setLastTimeStreamed(dateFormat.format(date).toString());

						distribution.update(true);

						logger.info("Distribution " + distribution.getDownloadUrl() + " processed! ");

					} catch (Exception e) {
						// uptate status of distribution
						distribution.setStatus(DistributionStatus.ERROR);
						distribution.setLastMsg(e.getMessage());

						e.printStackTrace();
						distribution.setSuccessfullyDownloaded(false);
						distribution.update(true);

					}
			}			
			
		}
		consumingQueue = false;

		logger.info("We are done reading your distributions.");
	}

	/**
	 * Start the manager with a list of dataset to be analyzed
	 * 
	 * @param collection
	 */
	public Manager(Collection<DatasetDB> collection) {
		if (LODVaderProperties.CHECK_LOV)
			checkLOV();

		try {
			for (DatasetDB dataset : collection) {

				datasets.add(dataset);
				logger.info("Adding new dataset to the queue: " + dataset.getUri());

			}
			
			// start to process the distributions of datasets
			if (!consumingQueue) {
				streamAndCreateFilters();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void checkLOV() {
		// check if LOV have already been downloaded
		SystemPropertiesDB g = new SystemPropertiesDB();
		if (g.getDownloadedLOV() == null || !g.getDownloadedLOV()) {
			logger.info("LOV vocabularies still not lodaded! Loading now...");
			try {
				new LOV().loadLOVVocabularies();
				g.setDownloadedLOV(true);
				logger.info("LOV vocabularies loaded!");
			} catch (Exception e) {
				e.printStackTrace();
				g.setDownloadedLOV(false);
				logger.info("We got an error trying to load LOV vocabularies! " + e.getMessage());
			}
		}
	}

	private static boolean checkDistributionStatus(DistributionDB distributionMongoDBObj) throws Exception {
		boolean needDownload = false;

		if (distributionMongoDBObj.getStatus().equals(DistributionStatus.WAITING_TO_STREAM))
			needDownload = true;
		else if (distributionMongoDBObj.getStatus().equals(DistributionStatus.STREAMING))
			needDownload = false;
		else if (distributionMongoDBObj.getStatus().equals(DistributionStatus.ERROR))
			needDownload = true;
		else if (new CheckWhetherToStream().checkDistribution(distributionMongoDBObj))
			needDownload = true;

		return needDownload;
	}

}
