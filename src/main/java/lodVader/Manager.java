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
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.exceptions.DynamicLODNoDistributionFoundException;
import lodVader.exceptions.DynamicLODNoDownloadURLFoundException;
import lodVader.links.similarity.JaccardSimilarity;
import lodVader.links.similarity.LinkSimilarity;
import lodVader.links.strength.LinkStrength;
import lodVader.lov.LOV;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.SystemPropertiesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.parsers.InputRDFParser;
import lodVader.streaming.CheckWhetherToStream;
import lodVader.streaming.StreamAndCompareDistribution;
import lodVader.utils.FileUtils;
import lodVader.utils.Timer;

public class Manager {
	final static Logger logger = Logger.getLogger(Manager.class);

	private String someDatasetURI = null;

	// list of subset and their distributions
	public List<DistributionDB> distributionsLinks = new ArrayList<DistributionDB>();

	InputRDFParser fileInputParserModel = new InputRDFParser();

	public void streamAndCreateFilters() throws Exception {
		// if there is at least one distribution, load them
		Iterator<DistributionDB> distributions = distributionsLinks
				.iterator();

		int counter = 0;

		logger.info("Loading " + distributionsLinks.size()
				+ " distributions...");

		while (distributions.hasNext()) {
			counter++;

			DistributionDB distributionMongoDBObj = distributions
					.next();

			// case there is no such distribution, create one.
			if (distributionMongoDBObj.getStatus() == null) {
				distributionMongoDBObj
						.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
			}

			// check is distribution need to be streamed
			boolean needDownload = checkDistributionStatus(distributionMongoDBObj);
			// needDownload = true;

			logger.info("Distribution n. " + counter + ": "
					+ distributionMongoDBObj.getUri());

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
					distributionMongoDBObj
							.setStatus(DistributionDB.STATUS_STREAMING);
					distributionMongoDBObj.updateObject(true);

					// now we need to download the distribution
					StreamAndCompareDistribution downloadedFile = new StreamAndCompareDistribution(
							distributionMongoDBObj);

					logger.info("Streaming distribution.");

						downloadedFile.streamDistribution();
					

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionDB.STATUS_STREAMED);
					distributionMongoDBObj.updateObject(true);

					logger.info("Distribution streamed. ");

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionDB.STATUS_CREATING_BLOOM_FILTER);
					distributionMongoDBObj.updateObject(true);

					logger.info("Creating bloom filter.");

					createBloomFilters(downloadedFile, distributionMongoDBObj);

					// save distribution in a mongodb object

					logger.info("Saving mongodb \"Distribution\" document.");

					distributionMongoDBObj.setNumberOfObjectTriples(String
							.valueOf(downloadedFile.objectLines));
					distributionMongoDBObj.setDownloadUrl(downloadedFile.url
							.toString());
					distributionMongoDBObj.setFormat(downloadedFile.extension
							.toString());
					distributionMongoDBObj.setHttpByteSize(String
							.valueOf((int) downloadedFile.httpContentLength));
					distributionMongoDBObj
							.setHttpFormat(downloadedFile.httpContentType);
					distributionMongoDBObj
							.setHttpLastModified(downloadedFile.httpLastModified);
					distributionMongoDBObj
							.setObjectPath(downloadedFile.objectFilePath);
					distributionMongoDBObj
							.setTriples(downloadedFile.totalTriples);


					distributionMongoDBObj.setSuccessfullyDownloaded(true);
					distributionMongoDBObj.updateObject(true);
					
					logger.info("Checking Similarity among distributions...");
					distributionMongoDBObj
						.setStatus(DistributionDB.STATUS_CREATING_JACCARD_SIMILARITY);
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
					distributionMongoDBObj
						.setStatus(DistributionDB.STATUS_UPDATING_LINK_STRENGTH);
					distributionMongoDBObj.updateObject(true);
					// Saving link similarities
					LinkStrength linkStrength = new LinkStrength();
					linkStrength.updateLinks(distributionMongoDBObj);


					logger.info("Done streaming mongodb distribution object.");

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionDB.STATUS_DONE);
					DateFormat dateFormat = new SimpleDateFormat(
							"HH:mm:ss dd/MM/yyyy");
					// get current date time with Date()
					Date date = new Date();

					distributionMongoDBObj.setLastTimeStreamed(dateFormat
							.format(date).toString());

					distributionMongoDBObj.updateObject(true);

					logger.info("Distribution saved! ");

				} catch (Exception e) {
					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionDB.STATUS_ERROR);
					distributionMongoDBObj.setLastMsg(e.getMessage());

					e.printStackTrace();
					distributionMongoDBObj.setSuccessfullyDownloaded(false);
					distributionMongoDBObj.updateObject(true);
					
					
					
				}

		}
		logger.info("We are done reading your distributions.");
	}

	public Manager(List<DistributionDB> distributionsLinks) {
		this.distributionsLinks = distributionsLinks;
		checkLOV();
		try {
			streamAndCreateFilters();
		}

		catch (Exception e) {
			e.printStackTrace();
		}

	}

	public Manager(String URL) {
		try {
			logger.debug("Loading VoID/DCAT/DataID file. URL: " + URL);

			// check file extension
			FileUtils.acceptedFormats(URL.toString());

			// create jena model
			someDatasetURI = fileInputParserModel.readModel(URL, "ttl");

			logger.info("We found at least one dataset: " + someDatasetURI);

			logger.info("Parsing model in order to find distributions...");

			// parse model in order to find distributions
			List<DistributionDB> listOfSubsets = fileInputParserModel
					.parseDistributions();
			int numberOfDistributions = listOfSubsets.size();

			if (!fileInputParserModel.someDownloadURLFound)
				throw new DynamicLODNoDownloadURLFoundException(
						"No DownloadURL property found!");
			else if (numberOfDistributions == 0)
				throw new DynamicLODNoDistributionFoundException(
						"### 0 distribution found! ###");

			checkLOV();

			// try to load distributions and make filters
			streamAndCreateFilters();

		} catch (DynamicLODFileNotAcceptedException | LODVaderNoDatasetFoundException | DynamicLODNoDownloadURLFoundException
				| DynamicLODNoDistributionFoundException e) {
			logger.error(e.getMessage());
		} catch (Exception e1) {
			e1.printStackTrace();
			logger.error(e1.getMessage());
		}

		logger.info("END");
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
				logger.info("We got an error trying to load LOV vocabularies! "
						+ e.getMessage());
			}
		}
	}

	private boolean checkDistributionStatus(
			DistributionDB distributionMongoDBObj) throws Exception {
		boolean needDownload = false;

		if (distributionMongoDBObj.getStatus().equals(
				DistributionDB.STATUS_WAITING_TO_STREAM))
			needDownload = true;
		else if (distributionMongoDBObj.getStatus().equals(
				DistributionDB.STATUS_STREAMING))
			needDownload = false;
		else if (distributionMongoDBObj.getStatus().equals(
				DistributionDB.STATUS_ERROR))
			needDownload = true;
		else if (new CheckWhetherToStream()
				.checkDistribution(distributionMongoDBObj))
			needDownload = true;

		return needDownload;
	}

	public boolean createBloomFilters(
			StreamAndCompareDistribution downloadedFile,
			DistributionDB distributionMongoDBObj) {
		
		GoogleBloomFilter filterSubject;
		GoogleBloomFilter filterObject;
		if (downloadedFile.subjectLines != 0) {

			// get customized equation from properties file
			if (LODVaderProperties.FPP_EQUATION != null) {

				// equation parser
				JexlEngine jexl = new JexlEngine();
				jexl.setCache(512);
				jexl.setLenient(false);
				jexl.setSilent(false);

				String calc = LODVaderProperties.FPP_EQUATION;
				Expression e = jexl.createExpression(calc);

				// populate the context
				JexlContext context = new MapContext();
				context.set("distributionSize", downloadedFile.subjectLines);

				// create filter for subjects
				Double result = (Double) e.evaluate(context);

				filterSubject = new GoogleBloomFilter(
						(int) downloadedFile.subjectLines, result);

				logger.info("Created bloom filter with customized equation: "
						+ LODVaderProperties.FPP_EQUATION + " and value: "
						+ result);

				// create filter for objects
				filterObject = new GoogleBloomFilter(
						(int) downloadedFile.objectLines, result);

			} else {

				if (downloadedFile.subjectLines > 1000000) {
					filterSubject = new GoogleBloomFilter(
							(int) downloadedFile.subjectLines,
							0.9 / downloadedFile.subjectLines);
					filterObject = new GoogleBloomFilter(
							(int) downloadedFile.objectLines,
							0.9 / downloadedFile.objectLines);

				} else {
					filterSubject = new GoogleBloomFilter(
							(int) downloadedFile.subjectLines, 0.0000001);
					filterObject = new GoogleBloomFilter(
							(int) downloadedFile.objectLines, 0.0000001);
				}
			}
		} else {
			filterSubject = new GoogleBloomFilter(
					(int) downloadedFile.contentLengthAfterDownloaded / 40,
					0.000001);
			filterObject = new GoogleBloomFilter(
					(int) downloadedFile.contentLengthAfterDownloaded / 40,
					0.000001);
		}

		// load file to filter and take the process time
//		FileToFilter f = new FileToFilter();

		Timer timer = new Timer();
		timer.startTimer();

		// Loading subject file to filter
		filterSubject.loadFileToFilter(
				LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setTimeToCreateSubjectFilter(String
				.valueOf(timer.stopTimer()));

		filterSubject
				.saveFilter(LODVaderProperties.SUBJECT_FILE_FILTER_PATH
						+ downloadedFile.hashFileName);
		// save filter

		distributionMongoDBObj
				.setSubjectFilterPath(LODVaderProperties.SUBJECT_FILE_FILTER_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setNumberOfSubjectTriples(String
				.valueOf(filterSubject.elementsLoadedIntoFilter));

		timer = new Timer();
		timer.startTimer();
		// Loading object file to filter
		filterObject.loadFileToFilter(
				LODVaderProperties.OBJECT_FILE_DISTRIBUTION_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setTimeToCreateObjectFilter(String.valueOf(timer
				.stopTimer()));

		filterObject.saveFilter(LODVaderProperties.OBJECT_FILE_FILTER_PATH
				+ downloadedFile.hashFileName);
		// save filter

		distributionMongoDBObj
				.setObjectFilterPath(LODVaderProperties.OBJECT_FILE_FILTER_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setNumberOfObjectTriples(String
				.valueOf(filterObject.elementsLoadedIntoFilter));
		
		// remove temp files
		FileUtils.removeFile(LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH
						+ downloadedFile.hashFileName);
		
		FileUtils.removeFile(LODVaderProperties.OBJECT_FILE_DISTRIBUTION_PATH
				+ downloadedFile.hashFileName);
		

		return false;
	}

}
