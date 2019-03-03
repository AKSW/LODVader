package lodVader.lov;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

import lodVader.configuration.LODVaderProperties;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.collections.DatasetCoesionValues;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.GeneralQueries;
import lodVader.streaming.SuperStream;

public class LOV2 extends SuperStream {
	final static Logger logger = LoggerFactory.getLogger(LOV2.class);

	DistributionDB distribution = null;

	// saving all predicates
	HashMap<String, Integer> allPredicates = new HashMap<String, Integer>();

	HashMap<String, Integer> owlClasses = new HashMap<String, Integer>();

	// saving all rdf type
	// HashMap<String, Integer> rdfTypeSubjects = new HashMap<String,
	// Integer>();
	HashMap<String, Integer> rdfTypeObjects = new HashMap<String, Integer>();

	HashMap<String, Integer> rdfSubClassOf = new HashMap<String, Integer>();

	public void loadLOVVocabularies() throws Exception {

		logger.info("Loading LOV vocabulary.");
 
		Model m = ModelFactory.createDefaultModel();
		Model tmpModel = ModelFactory.createDefaultModel();

		new LODVaderProperties().loadProperties();

		setUrl(new URL(LODVaderProperties.LOV_URL));
		// setUrl(new
		// URL("http://data.pokepedia.fr/dumps/pokepedia-fr_rdfdump.tar.gz"));
		//
		// download lov file
		openStream();

		// allowing gzip format
		checkGZipInputStream();

		simpleDownload(LODVaderProperties.BASE_PATH + "lov.tmp", inputStream);

		DatasetGraph dg = RDFDataMgr.loadDatasetGraph(LODVaderProperties.BASE_PATH + "lov.tmp", Lang.NQUADS);
		// DatasetGraph dg = RDFDataMgr.loadDatasetGraph(
		// LODVaderProperties.BASE_PATH + "lov", Lang.NQUADS);

		Iterator<Node> tmpNodeIt = dg.listGraphNodes();

		int a = 0;

		Node tmpNode = null;
		while (tmpNodeIt.hasNext()) {
			tmpNode = tmpNodeIt.next();
			Graph tmpGraph = dg.getGraph(tmpNode);

			tmpModel = ModelFactory.createModelForGraph(tmpGraph);

			if (tmpNode.getURI().equals("http://lov.okfn.org/dataset/lov")) {
				break;
			}

		}

		Iterator<Node> nodeIt = dg.listGraphNodes();

		int i = 0;

		while (nodeIt.hasNext()) {
			Node node = nodeIt.next();
			Graph graph = dg.getGraph(node);

			int numberOfTriples = 0;
			int numberOfLiterals = 0;
			int numberCohesion = 0;

			m = ModelFactory.createModelForGraph(graph);

			Property p = ResourceFactory.createProperty("http://purl.org/dc/terms/title");

			Property p2 = ResourceFactory.createProperty("http://www.w3.org/2000/01/rdf-schema#label");

			Resource r = ResourceFactory.createResource(node.getURI());

			// new dataset at mongodb
			StmtIterator stmt = tmpModel.listStatements(r, p, (RDFNode) null);
			String label = null;

			if (stmt.hasNext()) {
				label = stmt.next().getObject().toString();

				stmt = tmpModel.listStatements(r, p2, (RDFNode) null);
			} else if (stmt.hasNext()) {
				label = stmt.next().getObject().toString();

			}
			System.out.println("Dataset: "+label);
			try{
			DatasetDB dataset = new DatasetDB(
					new GeneralQueries().getMongoDBObject(DatasetDB.COLLECTION_NAME, DatasetDB.TITLE, label).get(0));

			StmtIterator triples = m.listStatements(null, null, (RDFNode) null);

			ConcurrentLinkedQueue<String> subjectsQueue = new ConcurrentLinkedQueue<String>();
			ConcurrentLinkedQueue<String> objectsQueue = new ConcurrentLinkedQueue<String>();
			TreeSet<String> subjects = new TreeSet<String>();
			TreeSet<String> objects = new TreeSet<String>();

			// saving all predicates
			allPredicates = new HashMap<String, Integer>();

			// saving all rdf type
			// rdfTypeSubjects = new HashMap<String, Integer>();
			rdfTypeObjects = new HashMap<String, Integer>();
			rdfSubClassOf = new HashMap<String, Integer>();
			owlClasses = new HashMap<String, Integer>();

			while (triples.hasNext()) {

				Statement triple = triples.next();

				if (triple.getSubject().toString().contains(node.getNameSpace())) {

					subjects.add(triple.getSubject().toString());
					objects.add(triple.getObject().toString());
					if (triple.getObject().isLiteral()) {
						numberOfLiterals++;
					}
					numberOfTriples++;

				}
//				System.out.println(triple.getSubject().toString());
				
			}
			
			Iterator<String> it = objects.iterator();
			while(it.hasNext()){
				if(subjects.contains(it.next()))
					numberCohesion++;
			}
			
			System.out.println("vocabulary: "+dataset.getTitle());
			System.out.println("total triples: "+numberOfTriples);
			System.out.println("total literals: "+numberOfLiterals);
			System.out.println("total cohesion: "+numberCohesion);
			
			DatasetCoesionValues cohesion = new DatasetCoesionValues();
			cohesion.setCohesion(numberCohesion);
			cohesion.setDatasetID(dataset.getLODVaderID().toString());
			cohesion.setIsVocab(true);
			cohesion.setLiterals(numberOfLiterals);
			cohesion.setTriples(numberOfTriples);
			
			cohesion.update(true);
			
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			}
			catch(Exception e){
				
			}

		}
	}

	@Override
	public void streamDistribution() throws IOException, LODVaderLODGeneralException, InterruptedException,
			RDFHandlerException, RDFParseException, LODVaderFormatNotAcceptedException {
		// TODO Auto-generated method stub

	}

}
