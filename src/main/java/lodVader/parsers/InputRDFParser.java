package lodVader.parsers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.riot.RiotException;
import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import lodVader.LODVaderProperties;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.ontology.NS;
import lodVader.ontology.RDFProperties;
import lodVader.utils.FileUtils;
import lodVader.utils.Formats;

/**
 * @author ciro
 * parser for DataID, VoID and DCAT files
 */
public class InputRDFParser {

	final static Logger logger = Logger.getLogger(InputRDFParser.class);

	private Model inModel = ModelFactory.createDefaultModel();
	public List<DistributionDB> distributionsLinks = new ArrayList<DistributionDB>();
	int numberOfDistributions = 0;
	public boolean someDownloadURLFound = false;
	private String fileURLHash;
	private String access_url;

//	APIStatusMongoDBObject apiStatus = null;

	boolean isVoid = false;
	boolean isDataid = false;

	/**
	 * Get the first statement (described as primary topic) of an RDF file
	 * @return
	 */
	public StmtIterator getFirstStmt() {
		// select dataset
		StmtIterator datasetsStmt = null;

		// find primaryTopic
		datasetsStmt = inModel.listStatements(null, RDFProperties.primaryTopic,
				(RDFNode) null);

		Resource topic = null;

		if (datasetsStmt.hasNext())
			topic = datasetsStmt.next().getObject().asResource();

		for (Resource datasetResource : RDFProperties.Dataset) {
			if (topic == null)
				datasetsStmt = inModel.listStatements(null, RDFProperties.type,
						datasetResource);
			else
				datasetsStmt = inModel.listStatements(topic,
						RDFProperties.type, datasetResource);
			if (datasetsStmt.hasNext()) {
				if (datasetResource.equals(RDFProperties.dataIdDataset))
					isDataid = true;
				break;
			}
		}

		return datasetsStmt;
	}

	/**
	 * Method that will parse all distributions from a description file
	 * @return list of distribution objects
	 * @throws LODVaderNoDatasetFoundException
	 * @throws LODVaderFormatNotAcceptedException
	 * @throws LODVaderLODGeneralException
	 */
	public List<DistributionDB> parseDistributions()
			throws LODVaderNoDatasetFoundException,
			LODVaderFormatNotAcceptedException, LODVaderLODGeneralException {
		 
		// select dataset
		StmtIterator datasetsStmt = getFirstStmt();

		if (datasetsStmt.hasNext())
			iterateSubsetsNew(datasetsStmt, 0, 0,null, true);
		else
			throw new LODVaderNoDatasetFoundException(
					"We could not parse any datasets.");

		return distributionsLinks;
	}

	/**
	 * iterating over the subsets (recursive method)
	 * 
	 * @param stmtDatasets
	 * @param parentDataset
	 * @param topDatasetID
	 * @param isTopDataset
	 * @throws LODVaderLODGeneralException
	 * @throws LODVaderFormatNotAcceptedException
	 */
	private void iterateSubsetsNew(StmtIterator stmtDatasets,
			int parentDataset, int topDatasetID, String topDatasetTitle, boolean isTopDataset)
			throws LODVaderLODGeneralException,
			LODVaderFormatNotAcceptedException {

		// iterate over subsets
		while (stmtDatasets.hasNext()) {

			// get subset
			Statement dataset = stmtDatasets.next();

			String datasetURI = dataset.getSubject().toString();
			logger.info("Found dataset: " + datasetURI);


			// create a mongodb dataset object
			DatasetDB datasetMongoDBObj = new DatasetDB(
					datasetURI);
			
			// do not overlap LOV datasets
			if(datasetMongoDBObj.getIsVocabulary())
				break;
			
			

			datasetMongoDBObj.setAccess_url(access_url);

			// add description file path
			datasetMongoDBObj.setDescriptionFileName(fileURLHash);

			// case there is title property
			if (dataset.getSubject().getProperty(RDFProperties.title) != null) {
				datasetMongoDBObj.setTitle(dataset.getSubject()
						.getProperty(RDFProperties.title).getObject()
						.toString());
			} else
				datasetMongoDBObj.setTitle(datasetURI);

			// case there is label property
			if (dataset.getSubject().getProperty(RDFProperties.label) != null) {
				datasetMongoDBObj.setLabel(dataset.getSubject()
						.getProperty(RDFProperties.label).getObject()
						.toString());
			} else
				datasetMongoDBObj.setLabel(datasetURI);

			// setting TOP dataset
			if (isTopDataset) {
				topDatasetID = datasetMongoDBObj.getLODVaderID();
				topDatasetTitle = datasetMongoDBObj.getTitle();
			}

			datasetMongoDBObj.updateObject(true);
			datasetMongoDBObj.addParentDatasetID(parentDataset);

			// find subset within subset
			StmtIterator stmtDatasets2 = inModel.listStatements(dataset
					.getSubject().asResource(), RDFProperties.subset,
					(RDFNode) null);

			// case there is a subset, call method recursively
			while (stmtDatasets2.hasNext()) {

				// get subset
				Statement subset = stmtDatasets2.next();

				// case is a Linkset subset, leave.
				StmtIterator stmtLinkset;
				stmtLinkset = inModel.listStatements(subset.getObject()
						.asResource(), RDFProperties.type,
						RDFProperties.linkset);
				if (!stmtLinkset.hasNext()) {

					StmtIterator stmtDatasets3 = null;

					for (Resource datasetsp : RDFProperties.Dataset) {
						stmtDatasets3 = inModel.listStatements(subset
								.getObject().asResource(), RDFProperties.type,
								datasetsp);
						if (stmtDatasets3.hasNext())
							break;
					}

					if (stmtDatasets3.hasNext()) {
						iterateSubsetsNew(stmtDatasets3, datasetMongoDBObj.getLODVaderID(), topDatasetID, topDatasetTitle, false);
//						datasetMongoDBObj.addSubsetID(subset.getObject()
//								.toString());
						datasetMongoDBObj.addSubsetID(new DatasetDB(subset.getObject()
								.toString()).getLODVaderID());
						datasetMongoDBObj.updateObject(true);
					}
				}
			}

			// find a distribution within subset
			StmtIterator stmtDistribution = null;

			for (Property distributionProperty : RDFProperties.distribution) {
				stmtDistribution = inModel.listStatements(dataset.getSubject()
						.asResource(), distributionProperty, (RDFNode) null);

				// special treatment for VOID file
				if (stmtDistribution.hasNext()
						&& distributionProperty.equals(ResourceFactory
								.createProperty(NS.VOID_URI, "dataDump"))) {
					Statement stmtDistribution2 = stmtDistribution.next();
					addDistribution(stmtDistribution2, stmtDistribution2,
							datasetMongoDBObj,topDatasetTitle, topDatasetID);
				} else if (stmtDistribution.hasNext()) {
					break;
				}
			}

			// case there's an distribution take the fist that has
			// downloadURL
			boolean downloadURLFound = false;
			while (stmtDistribution.hasNext()) {
				// store distribution
				Statement distributionStmt = stmtDistribution.next();

				// give priority for nt files (case it's a dataid file)
				if (isDataid) {
					if (downloadURLFound == false)
						if (!stmtDistribution.hasNext()
								|| distributionStmt.getObject().toString()
										.contains(".nt")) {
							// find downloadURL property
							StmtIterator stmtDownloadURL = null;

							for (Property downloadProperty : RDFProperties.downloadURL) {
								stmtDownloadURL = inModel.listStatements(
										distributionStmt.getObject()
												.asResource(),
										downloadProperty, (RDFNode) null);
								if (stmtDownloadURL.hasNext())
									break;
							}

							// case there is an downloadURL property
							while (stmtDownloadURL.hasNext()) {
								// store downloadURL statement
								Statement downloadURLStmt = stmtDownloadURL
										.next();

								try {
									if (FileUtils
											.acceptedFormats(downloadURLStmt
													.getObject().toString())) {

										downloadURLFound = true;
										addDistribution(downloadURLStmt,
												distributionStmt,
												datasetMongoDBObj,topDatasetTitle, topDatasetID);

									}
								} catch (Exception ex) {
									ex.printStackTrace();
//									apiStatus.setHasError(true);
//									apiStatus.setMessage(ex.getMessage());
								}
							}
							break;
						}
				}

				else {

					// find downloadURL property
					StmtIterator stmtDownloadURL = null;

					for (Property downloadProperty : RDFProperties.downloadURL) {
						stmtDownloadURL = inModel.listStatements(
								distributionStmt.getObject().asResource(),
								downloadProperty, (RDFNode) null);
						if (stmtDownloadURL.hasNext())
							break;
					}

					// case there is an downloadURL property
					while (stmtDownloadURL.hasNext()) {
						// store downloadURL statement
						Statement downloadURLStmt = stmtDownloadURL.next();

						try {
							if (FileUtils.acceptedFormats(downloadURLStmt
									.getObject().toString())) {

								downloadURLFound = true;
								addDistribution(downloadURLStmt,
										distributionStmt, datasetMongoDBObj, topDatasetTitle,
										topDatasetID);

							}
						} catch (LODVaderFormatNotAcceptedException ex) {
							ex.printStackTrace();
						}
					}
				}
			}
		}
	}

	public void addDistribution(Statement downloadURLStmt,
			Statement stmtDistribution, DatasetDB subsetMongoDBObj,
			String topDatasetTitle, int topDatasetID) {

		logger.info("Distribution found: downloadURL: "
				+ downloadURLStmt.getObject().toString());

		// save distribution with downloadURL to list
		numberOfDistributions++;
		someDownloadURLFound = true;

		// creating mongodb distribution object
		DistributionDB distributionMongoDBObj = new DistributionDB(
				downloadURLStmt.getObject().toString());

		// do not overlap LOV datasets
		if(distributionMongoDBObj.getIsVocabulary())
			return;
		
		distributionMongoDBObj.setResourceUri(stmtDistribution.getSubject()
				.toString());
 
		distributionMongoDBObj.addDefaultDataset(subsetMongoDBObj.getLODVaderID());

		distributionMongoDBObj.setTopDataset(topDatasetID); 

		distributionMongoDBObj.setTopDatasetTitle(topDatasetTitle); 

		distributionMongoDBObj.setDownloadUrl(downloadURLStmt.getObject()
				.toString());

		// case there is title property
		try {
			if (stmtDistribution.getSubject().getProperty(RDFProperties.title) != null) {
				distributionMongoDBObj.setTitle(stmtDistribution
						.getProperty(RDFProperties.title).getObject()
						.toString());

			}
		} catch (Exception e) {
			if (stmtDistribution.getSubject().getProperty(RDFProperties.title) != null) {
				distributionMongoDBObj.setTitle(stmtDistribution.getSubject()
						.getProperty(RDFProperties.title).getObject()
						.toString());

			}
		}

		// case there is format property
		if (stmtDistribution.getSubject().getProperty(RDFProperties.format) != null) {
			distributionMongoDBObj.setFormat(Formats
					.getEquivalentFormat(stmtDistribution.getSubject()
							.getProperty(RDFProperties.format).getObject()
							.toString()));
		}
		if (stmtDistribution.getObject().asResource()
				.getProperty(RDFProperties.format) != null) {

			// try to get format like CKAN's provides:
			// dct:format [
			// a dct:IMT ;
			// rdf:value "application/rdf+xml" ;
			// rdfs:label "application/rdf+xml"
			// ] ;
			// a dcat:Distribution ;
			// dcat:accessURL
			// <http://download.geonames.org/all-geonames-rdf.zip>

			try {
				if (stmtDistribution.getObject().asResource()
						.getProperty(RDFProperties.format).getObject()
						.asResource().getProperty(RDFProperties.rdfValue)
						.getObject() != null) {
					distributionMongoDBObj.setFormat(Formats
							.getEquivalentFormat(stmtDistribution.getObject()
									.asResource()
									.getProperty(RDFProperties.format)
									.getObject().asResource()
									.getProperty(RDFProperties.rdfValue)
									.getObject().toString()));
					
					
					
				}
			} catch (Exception e) {

				// else
				distributionMongoDBObj.setFormat(Formats
						.getEquivalentFormat(stmtDistribution.getObject()
								.asResource().getProperty(RDFProperties.format)
								.getObject().toString()));
			}

		}

		if (distributionMongoDBObj.getStatus() == null) {
			distributionMongoDBObj
					.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
		}
		distributionMongoDBObj.updateObject(true);
		distributionsLinks.add(distributionMongoDBObj);

		if (subsetMongoDBObj != null) {
			// update dataset or subset on mongodb with distribution
			subsetMongoDBObj.addDistributionID(distributionMongoDBObj.getLODVaderID());
			subsetMongoDBObj.updateObject(true);
		}

	}

	private ResIterator findDataset() {
		ResIterator hasSomeDataset = null;
		for (Resource datasetResource : RDFProperties.Dataset) {
			hasSomeDataset = inModel.listResourcesWithProperty(
					RDFProperties.type, datasetResource);
			if (hasSomeDataset.hasNext())
				break;
		}
		return hasSomeDataset;
	}

	// read dataID file and return the dataset uri
	public String readModel(String URL, String format)
			throws MalformedURLException, IOException,
			LODVaderNoDatasetFoundException, RiotException {
//		apiStatus = new APIStatusMongoDBObject(URL);
		access_url = URL;
		String someDatasetURI = null;
		format = getJenaFormat(format);
		logger.info("Trying to read dataset: " + URL.toString());

		HttpURLConnection URLConnection = (HttpURLConnection) new URL(URL)
				.openConnection();
		URLConnection.setRequestProperty("Accept", "application/rdf+xml");

		inModel.read(URLConnection.getInputStream(), null, format);

		ResIterator hasSomeDataset = findDataset();

		if (hasSomeDataset.hasNext()) {
			someDatasetURI = hasSomeDataset.next().getURI().toString();
			logger.info("Jena model created. ");
			logger.info("Looks that this is a valid VoID/DCAT/DataID file! "
					+ someDatasetURI);

			fileURLHash = FileUtils.stringToHash(URL);
			inModel.write(new FileOutputStream(new File(
					LODVaderProperties.FILE_URL_PATH + fileURLHash)));
		} else {
			throw new LODVaderNoDatasetFoundException(
					"It's not possible to find a dataset.  Perhaps that's not a valid VoID, DCAT or DataID file.");
		}

		return someDatasetURI;
	}

	/**
	 * Get serialization format for Jena processing
	 * @param format
	 * @return
	 */
	public String getJenaFormat(String format) {
		format = Formats.getEquivalentFormat(format);
		if (format.equals(Formats.DEFAULT_NTRIPLES))
			return "N-TRIPLES";
		else if (format.equals(Formats.DEFAULT_TURTLE))
			return "TTL";
		else if(format.equals(Formats.DEFAULT_JSONLD))
			return "JSON-LD";
		else
			return "RDF/XML";

	}

}
