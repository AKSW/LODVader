package lodVader.parsers.descriptionfileparser;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.riot.RiotException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.Formats;

public class CLODFileParser implements FileParserInterface {

	final static Logger logger = LoggerFactory.getLogger(CLODFileParser.class);

	private Model inModel = ModelFactory.createDefaultModel();

	Property urlProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/url");
	Property formatProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/fileExtension");
	
	public List<DistributionDB> distributionsLinks = new ArrayList<DistributionDB>();
	
	@Test
	public void oi (){
		try {
			readModel("http://localhost/dbpedia/urls", "nt");
		} catch (RiotException | IOException | LODVaderNoDatasetFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// read dataID file and return the dataset uri
	public String readModel(String URL, String format)
			throws MalformedURLException, IOException, LODVaderNoDatasetFoundException, RiotException {
		// apiStatus = new APIStatusMongoDBObject(URL);
		String someDatasetURI = null;
		format = getJenaFormat(format);
		logger.info("Trying to read dataset: " + URL.toString());

		HttpURLConnection URLConnection = (HttpURLConnection) new URL(URL).openConnection();
		URLConnection.setRequestProperty("Accept", "application/rdf+xml");

		inModel.read(URLConnection.getInputStream(), null, format);

		StmtIterator someIterator = inModel.listStatements(null, urlProp, (RDFNode) null);

		while(someIterator.hasNext()){
			Statement stmt = someIterator.next();
			
			try {
			DatasetDB dataset = new DatasetDB(stmt.getObject().toString());
			dataset.setIsVocabulary(false);
			dataset.setDescriptionFileURL(URL);
			dataset.setTitle(stmt.getObject().toString());
			dataset.update(true);
			
			DistributionDB distribution = new DistributionDB();
			distribution.setUri(stmt.getObject().toString());
			distribution.setDownloadUrl(stmt.getObject().toString());
			distribution.setTitle(stmt.getObject().toString());
			distribution.setTopDatasetTitle(stmt.getObject().toString());
			distribution.setTopDataset(dataset.getLODVaderID());
			distribution.setIsVocabulary(false);
			distribution.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);

			StmtIterator otherIterator = inModel.listStatements(stmt.getSubject().asResource(), formatProp, (RDFNode) null);
			distribution.setFormat(Formats
					.getEquivalentFormat(otherIterator.next().getObject().asLiteral().getString()));

			System.out.println(stmt.getObject().toString());

			distribution.update(true);
			distributionsLinks.add(distribution);
			
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}	
		

		return someDatasetURI;
	}

	/**
	 * Get serialization format for Jena processing
	 * 
	 * @param format
	 * @return
	 */
	public String getJenaFormat(String format) {
		format = Formats.getEquivalentFormat(format);
		if (format.equals(Formats.DEFAULT_NTRIPLES))
			return "N-TRIPLES";
		else if (format.equals(Formats.DEFAULT_TURTLE))
			return "TTL";
		else if (format.equals(Formats.DEFAULT_JSONLD))
			return "JSON-LD";
		else
			return "RDF/XML";

	}

}
