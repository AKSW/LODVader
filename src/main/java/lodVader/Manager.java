package lodVader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Logger;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.exceptions.DynamicLODFileNotAcceptedException;
import lodVader.exceptions.DynamicLODNoDistributionFoundException;
import lodVader.exceptions.DynamicLODNoDownloadURLFoundException;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.links.similarity.JaccardSimilarity;
import lodVader.links.similarity.LinkSimilarity;
import lodVader.lov.LOV;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.SystemPropertiesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.parsers.InputRDFParser;
import lodVader.streaming.CheckWhetherToStream;
import lodVader.streaming.StreamDistribution;
import lodVader.utils.FileUtils;
import lodVader.utils.Timer;

public class Manager {
	final static Logger logger = Logger.getLogger(Manager.class);

	private String someDatasetURI = null;
 
	// list of subset and their distributions
	public static List<DistributionDB> distributionsLinks = new ArrayList<DistributionDB>();

	InputRDFParser fileInputParserModel = new InputRDFParser();

	public static void streamAndCreateFilters() throws Exception {
		// if there is at least one distribution, load them
		Iterator<DistributionDB> distributions = distributionsLinks.iterator();

		int counter = 0;

		logger.info("Loading " + distributionsLinks.size() + " distributions...");

		while (distributions.hasNext()) {
			counter++;

			DistributionDB distributionMongoDBObj = distributions.next();

			// case there is no such distribution, create one.
			if (distributionMongoDBObj.getStatus() == null) {
				distributionMongoDBObj.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
			}

			// check is distribution need to be streamed
			boolean needDownload = checkDistributionStatus(distributionMongoDBObj);
			// needDownload = true;

			logger.info("Distribution n. " + counter + ": " + distributionMongoDBObj.getUri());

			if (!needDownload) {
				logger.info("Distribution is already in the last version. No needs to stream again. ");
				distributionMongoDBObj
						.setLastMsg("Distribution is already in the last version. No needs to stream again.");
				distributionMongoDBObj.updateObject(true);
			}

			// if distribution have not already been handled
			if (needDownload)
				try {

					// uptate status of distribution to streaming
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_STREAMING);
					distributionMongoDBObj.updateObject(true);

					// now we need to download the distribution
					StreamDistribution streamFile = new StreamDistribution(distributionMongoDBObj);

					logger.info("Streaming distribution.");

					streamFile.streamDistribution();

					// uptate status of distribution
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_STREAMED);
					distributionMongoDBObj.updateObject(true);

					if (!LODVaderProperties.ONLY_STREAM_DATASETS) {

						logger.info("Distribution streamed. ");

						// uptate status of distribution
						distributionMongoDBObj.setStatus(DistributionDB.STATUS_CREATING_BLOOM_FILTER);
						distributionMongoDBObj.updateObject(true);

						logger.info("Creating bloom filter.");

						// createBloomFilters(downloadedFile,
						// distributionMongoDBObj);

						// save distribution in a mongodb object

						logger.info("Saving mongodb \"Distribution\" document.");

						distributionMongoDBObj.setNumberOfObjectTriples(String.valueOf(streamFile.objectLines));
						distributionMongoDBObj.setDownloadUrl(streamFile.url.toString());
						distributionMongoDBObj.setFormat(streamFile.extension.toString());
						distributionMongoDBObj.setHttpByteSize(String.valueOf((int) streamFile.httpContentLength));
						distributionMongoDBObj.setHttpFormat(streamFile.httpContentType);
						distributionMongoDBObj.setHttpLastModified(streamFile.httpLastModified);
						distributionMongoDBObj.setObjectPath(streamFile.objectFilePath);
						distributionMongoDBObj.setTriples(streamFile.totalTriples);

						distributionMongoDBObj.setSuccessfullyDownloaded(true);
						distributionMongoDBObj.updateObject(true);

						logger.info("Checking Similarity among distributions...");
						distributionMongoDBObj.setStatus(DistributionDB.STATUS_CREATING_JACCARD_SIMILARITY);
						distributionMongoDBObj.updateObject(true);
						// Saving link similarities

						logger.info("Checking Jaccard Similarities...");
						// Checking Jaccard Similarities...
						LinkSimilarity linkSimilarity = new JaccardSimilarity();
						linkSimilarity.updateLinks(distributionMongoDBObj, new AllPredicatesRelationDB());
						linkSimilarity.updateLinks(distributionMongoDBObj, new RDFTypeObjectRelationDB());
						linkSimilarity.updateLinks(distributionMongoDBObj, new RDFSubClassOfRelationDB());
						linkSimilarity.updateLinks(distributionMongoDBObj, new OwlClassRelationDB());

						logger.info("Updating link strength among distributions...");
						distributionMongoDBObj.setStatus(DistributionDB.STATUS_UPDATING_LINK_STRENGTH);
						distributionMongoDBObj.updateObject(true);
						// Saving link similarities
						// LinkStrength linkStrength = new LinkStrength();
						// linkStrength.updateLinks(distributionMongoDBObj);

						logger.info("Done streaming mongodb distribution object.");
					}

					// uptate status of distribution
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_DONE);
					
					DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss dd/MM/yyyy");
					// get current date time with Date()
					Date date = new Date();

					distributionMongoDBObj.setLastTimeStreamed(dateFormat.format(date).toString());

					distributionMongoDBObj.updateObject(true);

					logger.info("Distribution saved! ");

				} catch (Exception e) {
					// uptate status of distribution
					distributionMongoDBObj.setStatus(DistributionDB.STATUS_ERROR);
					distributionMongoDBObj.setLastMsg(e.getMessage());

					e.printStackTrace();
					distributionMongoDBObj.setSuccessfullyDownloaded(false);
					distributionMongoDBObj.updateObject(true);

				}

		}
		logger.info("We are done reading your distributions.");
	}

	public Manager(List<DistributionDB> distributionsLinksToBeAdded) {		
		checkLOV();
		try {
			if(distributionsLinks.size() == 0){
				for(DistributionDB dist:distributionsLinksToBeAdded ){
					distributionsLinks.add(dist);
				}
				streamAndCreateFilters();
			}
			else{
				for(DistributionDB dist:distributionsLinksToBeAdded ){
					distributionsLinks.add(dist);
				}
			}
		}
		catch (Exception e) {
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
