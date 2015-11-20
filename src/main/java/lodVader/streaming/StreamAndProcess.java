package lodVader.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.jsonld.JSONLDParser;
import org.openrdf.rio.n3.N3ParserFactory;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.turtle.TurtleParser;

import lodVader.LODVaderProperties;
import lodVader.TuplePart;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.linksets.MakeLinksetsMasterThread;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.parsers.tripleParsers.NTriplesLODVaderParser;
import lodVader.tupleManager.SplitAndProcess;
import lodVader.tupleManager.SplitAndStoreNT;
import lodVader.tupleManager.SuperTupleManager;
import lodVader.utils.FileUtils;
import lodVader.utils.Formats;

public class StreamAndProcess extends SuperStream {

	final static Logger logger = LoggerFactory.getLogger(StreamAndProcess.class);

	// Paths

	ConcurrentLinkedQueue<String> bufferQueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> objectQueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> subjectQueue = new ConcurrentLinkedQueue<String>();

	boolean doneReadingFile = false;
	boolean doneSplittingString = false;

	public MakeLinksetsMasterThread makeLinksetFromObjectsThread = null;
	public MakeLinksetsMasterThread makeLinksetFromSubjectsThread = null;

	private DistributionDB distribution = null;

	public StreamAndProcess(DistributionDB distributionMongoDBObj) throws MalformedURLException {
		this.distribution = distributionMongoDBObj;
		this.url = new URL(distributionMongoDBObj.getDownloadUrl());
		this.RDFFormat = distributionMongoDBObj.getFormat();
		this.uri = distributionMongoDBObj.getUri();
	}

	public void streamDistribution() throws IOException, LODVaderLODGeneralException, InterruptedException,
			RDFHandlerException, RDFParseException, LODVaderFormatNotAcceptedException {

		openStream();

		// allowing bzip2 format
		checkBZip2InputStream();

		// allowing gzip format
		checkGZipInputStream();

		// transform the URI to a hash
		hashFileName = FileUtils.stringToHash(url.toString());

		// get the path where the objects should be stored
		objectFilePath = LODVaderProperties.OBJECT_FILE_DISTRIBUTION_PATH + hashFileName;

		// check format and extension
		if (RDFFormat == null || RDFFormat.equals("")) {
			DistributionDB dist = new DistributionDB(url.toString());
			if (dist.getFormat() == null || dist.getFormat() == "" || dist.getFormat().equals(""))
				RDFFormat = getExtension();
			else
				RDFFormat = dist.getFormat();
		}

		// start streaming the distribution
		streamAndProcess();

		// setExtension(Formats.getEquivalentFormat(getExtension()));

		// after finishing stream, set the status
		doneReadingFile = true;

		// update file length
		File f = new File(LODVaderProperties.DUMP_PATH + hashFileName);
		if (httpContentLength < 1) {
			httpContentLength = f.length();
		}

		httpConn.disconnect();
		inputStream.close();
	}

	private void streamAndProcess()
			throws InterruptedException, LODVaderLODGeneralException, LODVaderFormatNotAcceptedException, IOException {

		SuperTupleManager splitThread;

		splitThread = new SplitAndProcess(subjectQueue, objectQueue, FileUtils.stringToHash(url.toString()),
				distribution.getLODVaderID());

		makeLinksetFromObjectsThread = new MakeLinksetsMasterThread(objectQueue, uri);
		makeLinksetFromSubjectsThread = new MakeLinksetsMasterThread(subjectQueue, uri);

		// setting thread names
		makeLinksetFromObjectsThread.setName("getNSFromObjectsThread");
		makeLinksetFromSubjectsThread.setName("getNSFromSubjectsThread");

		// setting part of the tuple being processed
		makeLinksetFromSubjectsThread.tuplePart = TuplePart.SUBJECT;
		makeLinksetFromObjectsThread.tuplePart = TuplePart.OBJECT;

		try {

			// instance of rdf parser
			RDFParser rdfParser = null;

			// checking whether to use turtle parser
			if (RDFFormat.equals(Formats.DEFAULT_TURTLE)) {
				rdfParser = new TurtleParser();
				logger.info("==== Turtle Parser loaded ====");
			}

			// checking ntriples to use turtle parser
			else if (RDFFormat.equals(Formats.DEFAULT_NTRIPLES)) {
				// rdfParser = new NTriplesParser();
				rdfParser = new NTriplesLODVaderParser();
				logger.info("==== NTriples Parser loaded ====");
			}

			// checking rdf/xml to use turtle parser
			else if (RDFFormat.equals(Formats.DEFAULT_RDFXML)) {
				rdfParser = new RDFXMLParser();
				logger.info("==== RDF/XML Parser loaded ====");
			}

			// checking jsonld to use turtle parser
			else if (RDFFormat.equals(Formats.DEFAULT_JSONLD)) {
				rdfParser = new JSONLDParser();
				logger.info("==== JSON-LD Parser loaded ====");
			}

			// checking n3 to use turtle parser
			else if (RDFFormat.equals(Formats.DEFAULT_N3)) {
				rdfParser = new N3ParserFactory().getParser();
				logger.info("==== N3Parser loaded ====");
			}

			// if the format is not supported, throw an exception
			else {
				httpConn.disconnect();
				inputStream.close();
				logger.info("RDF format not supported: " + RDFFormat);
				throw new LODVaderFormatNotAcceptedException("RDF format not supported: " + RDFFormat);
			}

			// start threads
			makeLinksetFromSubjectsThread.start();
			makeLinksetFromObjectsThread.start();

			// set RDF handler
			rdfParser.setRDFHandler(splitThread);

			// set OpenRDF parset config
			ParserConfig config = new ParserConfig();
			config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
			config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
			config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
			config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
			config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
			rdfParser.setParserConfig(config);

			// check whether file is tar/zip type
			if (getExtension().equals("zip")) {
				InputStream data = new BufferedInputStream(inputStream);
				logger.debug("File extension is zip, creating ZipInputStream and checking compressed files...");

				ZipInputStream zip = new ZipInputStream(data);
				int nf = 0;
				ZipEntry entry = zip.getNextEntry();
				while (entry != null) {
					if (!entry.isDirectory()) {
						logger.debug(++nf + " zip file uncompressed.");
						logger.debug("File name: " + entry.getName());
						rdfParser.parse(zip, url.toString());
					}

					entry = zip.getNextEntry();
				}

				setExtension(FilenameUtils.getExtension(getFileName()));
			}

			else if (getExtension().equals("tar")) {
				InputStream data = new BufferedInputStream(inputStream);
				logger.debug("File extension is tar, creating TarArchiveInputStream and checking compressed files...");

				TarArchiveInputStream tar = new TarArchiveInputStream(data);
				int nf = 0;
				TarArchiveEntry entry = (TarArchiveEntry) tar.getNextEntry();
				while (entry != null) {
					if (entry.isFile() && !entry.isDirectory()) {
						logger.debug(++nf + " tar file uncompressed.");
						logger.debug("File name: " + entry.getName());

						byte[] content = new byte[(int) entry.getSize()];

						tar.read(content, 0, (int) entry.getSize());

						rdfParser.parse(tar, url.toString());
					}
					entry = (TarArchiveEntry) tar.getNextEntry();
				}
				setExtension(FilenameUtils.getExtension(getFileName()));
			}

			else {
				rdfParser.parse(inputStream, url.toString());
			}

		} catch (RDFHandlerException | IOException | RDFParseException e) {

		}

		doneReadingFile = true;

		// fileName = splitThread.getFileName();
		objectLines = splitThread.getObjectLines();
		subjectLines = splitThread.getSubjectLines();
		totalTriples = splitThread.getTotalTriples();
		makeLinksetFromObjectsThread.setDoneSplittingString(true);
		makeLinksetFromObjectsThread.join();

		makeLinksetFromSubjectsThread.setDoneSplittingString(true);
		makeLinksetFromSubjectsThread.join();

		splitThread.closeFiles();

		logger.info("Saving predicates...");
		// save predicates
		new AllPredicatesDB().insertSet(splitThread.allPredicates.keySet());
		new AllPredicatesRelationDB().insertSet(splitThread.allPredicates, distribution.getLODVaderID(),
				distribution.getTopDatasetID());

		logger.info("Saving rdf:type objects...");
		// Saving RDF Type classes
		new RDFTypeObjectDB().insertSet(splitThread.rdfTypeObjects.keySet());
		new RDFTypeObjectRelationDB().insertSet(splitThread.rdfTypeObjects, distribution.getLODVaderID(),
				distribution.getTopDatasetID());

		logger.info("Saving rdfs:subclass objects...");
		new RDFSubClassOfDB().insertSet(splitThread.rdfSubClassOf.keySet());
		new RDFSubClassOfRelationDB().insertSet(splitThread.rdfSubClassOf, distribution.getLODVaderID(),
				distribution.getTopDatasetID());

		logger.info("Saving owl:Class objects...");
		new OwlClassDB().insertSet(splitThread.owlClasses.keySet());
		new OwlClassRelationDB().insertSet(splitThread.owlClasses, distribution.getLODVaderID(),
				distribution.getTopDatasetID());

		// create BFs
		logger.info("Creating subjects BF");
		logger.debug("Reading resources from file system...");
		SubjectsBucket subjectBucket = new SubjectsBucket(
				getUniqueItemsFromFile(LODVaderProperties.SUBJECT_FILE_DISTRIBUTION_PATH + hashFileName),
				distribution.getLODVaderID());
		logger.debug("Creating bucket.");
		subjectBucket.makeBucket();

		logger.debug("Creating objects BF");
		logger.debug("Reading resources from file system...");
		ObjectsBucket objectBucket = new ObjectsBucket(
				getUniqueItemsFromFile(LODVaderProperties.OBJECT_FILE_DISTRIBUTION_PATH + hashFileName),
				distribution.getLODVaderID());
		logger.debug("Creating bucket.");
		objectBucket.makeBucket();

	}

	public TreeSet<String> getUniqueItemsFromFile(String fileName) {
		BufferedReader br;
		String sCurrentLine;
		TreeSet<String> items = new TreeSet<String>();

		try {
			br = new BufferedReader(new FileReader(fileName));

			while ((sCurrentLine = br.readLine()) != null) {
				items.add(sCurrentLine);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return items;
	}

}
