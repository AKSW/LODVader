package lodVader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.links.similarity.JaccardSimilarity;
import lodVader.links.similarity.LinkSimilarity;
import lodVader.lov.LOV;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.SystemPropertiesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.parsers.descriptionfileparser.DescriptionFileParser;
import lodVader.streaming.CheckWhetherToStream;
import lodVader.streaming.StreamAndProcess;
import lodVader.streaming.StreamAndSaveDump;
import lodVader.streaming.SuperStream;

public class Manager {
	final static Logger logger = LoggerFactory.getLogger(Manager.class);

	// list of subset and their distributions
	public static Queue<DistributionDB> distributionsLinks = new LinkedBlockingQueue<DistributionDB>();

	DescriptionFileParser fileInputParserModel = new DescriptionFileParser();

	static boolean consumingQueue = false;

	public static void streamAndCreateFilters() throws Exception {

		consumingQueue = true;

		while (distributionsLinks.size() > 0) {
			logger.info("We still have " + distributionsLinks.size() + " distributions in the queue...");

			DistributionDB distributionMongoDBObj = distributionsLinks.remove();

			logger.info("Processing distribution: " + distributionMongoDBObj.getUri());

			// case there is no such distribution, create one.
			if (distributionMongoDBObj.getStatus() == null) {
				distributionMongoDBObj.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
			}

			// check is distribution need to be streamed
			boolean needDownload = checkDistributionStatus(distributionMongoDBObj);
			// boolean needDownload = true;

			if (!needDownload) {
				logger.info("Distribution is already in the last version. No needs to stream again. ");
				distributionMongoDBObj
						.setLastMsg("Distribution is already in the last version. No needs to stream again.");
				distributionMongoDBObj.update(true);
			}

			// if distribution have not already been handled
			if (needDownload)
				try {

					// uptate status of distribution to streaming
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_STREAMING);
					distributionMongoDBObj.update(true);

					// now we need to download the distribution
					SuperStream streamFile;
					if(!LODVaderProperties.ONLY_STREAM_DATASETS_AND_SAVE_NT_FORMAT)
						streamFile = new StreamAndProcess(distributionMongoDBObj);
					else
						streamFile = new StreamAndSaveDump(distributionMongoDBObj);
						

					logger.info("Streaming distribution.");

					streamFile.streamDistribution();

					// uptate status of distribution
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_STREAMED);
					distributionMongoDBObj.update(true);

					logger.debug("Distribution streamed. ");

					logger.debug("Saving mongodb \"Distribution\" document.");

					distributionMongoDBObj.setNumberOfObjectTriples(streamFile.objectLines);
					distributionMongoDBObj.setNumberOfSubjectTriples(streamFile.subjectLines);
					distributionMongoDBObj.setDownloadUrl(streamFile.downloadUrl.toString());
					distributionMongoDBObj.setFormat(streamFile.extension.toString());
					distributionMongoDBObj.setHttpByteSize(String.valueOf((int) streamFile.httpContentLength));
					distributionMongoDBObj.setHttpFormat(streamFile.httpContentType);
					distributionMongoDBObj.setHttpLastModified(streamFile.httpLastModified);
					distributionMongoDBObj.setTriples(streamFile.totalTriples);

					distributionMongoDBObj.setSuccessfullyDownloaded(true);
					distributionMongoDBObj.update(true);

					logger.debug("Checking Similarity among distributions...");
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_CREATING_JACCARD_SIMILARITY);
					distributionMongoDBObj.update(true);
					// Saving link similarities

					logger.debug("Checking Jaccard Similarities...");
					// Checking Jaccard Similarities...
					LinkSimilarity linkSimilarity = new JaccardSimilarity();
					linkSimilarity.updateLinks(distributionMongoDBObj, new AllPredicatesRelationDB());
					linkSimilarity.updateLinks(distributionMongoDBObj, new RDFTypeObjectRelationDB());
					linkSimilarity.updateLinks(distributionMongoDBObj, new RDFSubClassOfRelationDB());
					linkSimilarity.updateLinks(distributionMongoDBObj, new OwlClassRelationDB());

					logger.debug("Updating link strength among distributions...");
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_UPDATING_LINK_STRENGTH);
					distributionMongoDBObj.update(true);
					// Saving link similarities
					// LinkStrength linkStrength = new LinkStrength();
					// linkStrength.updateLinks(distributionMongoDBObj);

					logger.debug("Done streaming mongodb distribution object.");

					// uptate status of distribution
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);

					DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss dd/MM/yyyy");
					// get current date time with Date()
					Date date = new Date();

					distributionMongoDBObj.setLastTimeStreamed(dateFormat.format(date).toString());

					distributionMongoDBObj.update(true);

					logger.info("Distribution " + distributionMongoDBObj.getDownloadUrl() + " processed! ");

				} catch (Exception e) {
					// uptate status of distribution
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_ERROR);
					distributionMongoDBObj.setLastMsg(e.getMessage());

					e.printStackTrace();
					distributionMongoDBObj.setSuccessfullyDownloaded(false);
					distributionMongoDBObj.update(true);

				}
		}
		consumingQueue = false;

		logger.info("We are done reading your distributions.");
	}

	public Manager(List<DistributionDB> distributionsLinksToBeAdded) {
		if (LODVaderProperties.CHECK_LOV)
			checkLOV();
		try {
			for (DistributionDB dist : distributionsLinksToBeAdded) {
				distributionsLinks.add(dist);
				logger.info("Adding new distribution to the queue: " + dist.getDownloadUrl());
			}
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
				g.updateObject(true);
				logger.info("LOV vocabularies loaded!");
			} catch (Exception e) {
				e.printStackTrace();
				g.setDownloadedLOV(false);
				g.updateObject(true);
				logger.info("We got an error trying to load LOV vocabularies! " + e.getMessage());
			}
		}
	}
 
	private static boolean checkDistributionStatus(DistributionDB distributionMongoDBObj) throws Exception {
		boolean needDownload = false;

		if (distributionMongoDBObj.getStatus().equals(DistributionDB.STATUS_WAITING_TO_STREAM))
			needDownload = true;
		else if (distributionMongoDBObj.getStatus().equals(DistributionDB.STATUS_STREAMING))
			needDownload = false;
		else if (distributionMongoDBObj.getStatus().equals(DistributionDB.STATUS_ERROR))
			needDownload = true;
		else if (new CheckWhetherToStream().checkDistribution(distributionMongoDBObj))
			needDownload = true;

		return needDownload;
	}

}
