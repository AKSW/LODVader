package lodVader.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.jsonld.JSONLDParser;
import org.openrdf.rio.n3.N3ParserFactory;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.LODVaderProperties;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.tripleParsers.NTriplesLODVaderParser;
import lodVader.tupleManager.SplitAndStoreNT;
import lodVader.tupleManager.SuperTupleManager;
import lodVader.utils.FileUtils;
import lodVader.utils.Formats;

public class StreamAndSaveDump extends SuperStream {

	final static Logger logger = LoggerFactory.getLogger(StreamAndSaveDump.class);

	// Paths
	public String uri;

	public double contentLengthAfterDownloaded = 0;

	BlockingQueue<String> bufferQueue = new ArrayBlockingQueue<String>(200000);
	BlockingQueue<String> objectQueue = new ArrayBlockingQueue<String>(200000);
	BlockingQueue<String> subjectQueue = new ArrayBlockingQueue<String>(200000);

	boolean doneReadingFile = false;
	boolean doneSplittingString = false;

	private DistributionDB distribution = null; 

	public StreamAndSaveDump(DistributionDB distributionMongoDBObj) throws MalformedURLException {
		this.distribution = distributionMongoDBObj;
		this.downloadUrl = new URL(distributionMongoDBObj.getDownloadUrl());
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

		// check format and extension
		if (RDFFormat == null || RDFFormat.equals("")) {
			DistributionDB dist = new DistributionDB(downloadUrl.toString());
			if (dist.getFormat() == null || dist.getFormat() == "" || dist.getFormat().equals(""))
				RDFFormat = getExtension();
			else
				RDFFormat = dist.getFormat();
		}

		// start streaming the distribution
		streamAndProcess();

		doneReadingFile = true;

		httpConn.disconnect();
		inputStream.close();
	}

	private void streamAndProcess()
			throws InterruptedException, LODVaderLODGeneralException, LODVaderFormatNotAcceptedException, IOException {

		SuperTupleManager splitThread;
		String directoryPath = LODVaderProperties.DUMP_PATH + distribution.getTopDatasetTitle() + "_"
				+ distribution.getTopDatasetID() + "/";
		String filePath = directoryPath + distribution.getTitle() + "_" + distribution.getLODVaderID();

		// check whether the folder exists
		File f = new File(directoryPath);
		if (!f.exists())
			f.mkdir();

		String metaFileName = filePath + ".meta";

		// creating some metadata to help identify the file
		FileWriter metaWriter = new FileWriter(new File(metaFileName));
		metaWriter.write(DistributionDB.LOD_VADER_ID + "=\"" + distribution.getLODVaderID() + "\"\n");
		metaWriter.write(DistributionDB.DOWNLOAD_URL + "=\"" + distribution.getDownloadUrl() + "\"\n");
		metaWriter.write(DistributionDB.TITLE + "=\"" + distribution.getTitle() + "\"\n");
		metaWriter.write(DistributionDB.TOP_DATASET + "=\"" + distribution.getTopDatasetID() + "\"\n");
		metaWriter.write(DistributionDB.TOP_DATASET_TITLE + "=\"" + distribution.getTopDatasetTitle() + "\"\n");

		metaWriter.close();

		splitThread = new SplitAndStoreNT(subjectQueue, objectQueue, filePath+ ".nt", distribution);

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
						
						rdfParser.parse(zip, downloadUrl.toString());

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

						rdfParser.parse(tar, downloadUrl.toString());
					}
					entry = (TarArchiveEntry) tar.getNextEntry();
				}
				setExtension(FilenameUtils.getExtension(getFileName()));
			}

			else {
				rdfParser.parse(inputStream, downloadUrl.toString());
			}

		} catch (RDFHandlerException | IOException | RDFParseException e) {

		}

		doneReadingFile = true;
		
		objectLines = splitThread.getObjectLines();
		subjectLines = splitThread.getSubjectLines();
		totalTriples = splitThread.getTotalTriples();
		
		splitThread.closeFiles();

		logger.debug("Saving predicates NS ...");
		
		String typeFileName = filePath + ".ns";
		BufferedWriter b = new BufferedWriter(new FileWriter(new File(typeFileName)));
		for(String predicate:splitThread.allPredicates.keySet()){
			b.write(predicate+ "\n");
		}
		b.close();
//		
//		logger.debug("Saving rdfs:subclass objects...");
//		String subclassFileName = filePath + ".subclass";
//		b = new BufferedWriter(new FileWriter(new File(subclassFileName)));
//		for(String subclass:splitThread.rdfSubClassOf.keySet()){
//			b.write(subclass+ "\n");
//		}
//		b.close();
//		
//		logger.debug("Saving owl:Class objects...");
//		String owlClassFileName = filePath + ".owlClass";
//		b = new BufferedWriter(new FileWriter(new File(owlClassFileName)));
//		for(String owlClass:splitThread.owlClasses.keySet()){
//			b.write(owlClass+ "\n");
//		}
//		b.close();
		
		String graphFileName = directoryPath + "global.graph";
		File file = new File(graphFileName);
		if(!file.exists()){
			b = new BufferedWriter(new FileWriter(file));		
			b.write(FileUtils.getASCIIFormat(distribution.getTopDatasetTitle() + "_"
					+ distribution.getTopDatasetID()));
			b.close();			
		}
		

	}

}
