package lodVader.lov;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;
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

import lodVader.LODVaderProperties;
import lodVader.TuplePart;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.links.similarity.JaccardSimilarity;
import lodVader.links.similarity.LinkSimilarity;
import lodVader.links.strength.LinkStrength;
import lodVader.linksets.MakeLinksetsMasterThread;
import lodVader.mongodb.collections.DatasetDB;
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
import lodVader.streaming.SuperStream;
import lodVader.utils.FileUtils;
import lodVader.utils.Timer;

public class LOV extends SuperStream {
	final static Logger logger = LoggerFactory.getLogger(LOV.class);

	DistributionDB distribution = null;

	int numberOfTriples = 0;

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

			m = ModelFactory.createModelForGraph(graph);

			Property p = ResourceFactory.createProperty("http://purl.org/dc/terms/title");

			Property p2 = ResourceFactory.createProperty("http://www.w3.org/2000/01/rdf-schema#label");

			Resource r = ResourceFactory.createResource(node.getURI());

			// new dataset at mongodb
			DatasetDB dataset = new DatasetDB(node.getNameSpace());
			StmtIterator stmt = tmpModel.listStatements(r, p, (RDFNode) null);

			if (stmt.hasNext())
				dataset.setTitle(stmt.next().getObject().toString());

			stmt = tmpModel.listStatements(r, p2, (RDFNode) null);
			if (stmt.hasNext()) {
				String label = stmt.next().getObject().toString();
				dataset.setLabel(label);

				if (dataset.getTitle() == null)
					dataset.setTitle(label);
			}

			dataset.setIsVocabulary(true);
			dataset.setDescriptionFileURL(LODVaderProperties.LOV_URL);
			dataset.setSubsetIds(new ArrayList<Integer>());
			dataset.setDistributionsIds(new ArrayList<Integer>());
			dataset.update(true);

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
					if (triple.getSubject().toString().startsWith("http")) {
						subjects.add(triple.getSubject().toString());
						subjectsQueue.add(triple.getSubject().toString());
					}
					if (triple.getObject().isResource()) {
						if (triple.getObject().toString().startsWith("http")) {
							objects.add(triple.getObject().toString());
							objectsQueue.add(triple.getObject().toString());
						}

						if (triple.getObject().toString().equals("http://www.w3.org/2002/07/owl#Class")) {
							addToMap(owlClasses, triple.getSubject().toString());
						} else if (triple.getPredicate().toString()
								.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
							addToMap(rdfSubClassOf, triple.getObject().toString());
						} else if (triple.getPredicate().toString()
								.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
							// addToMap(rdfTypeSubjects,
							// triple.getSubject().toString());
							addToMap(rdfTypeObjects, triple.getObject().toString());
						}
					}
					addToMap(allPredicates, triple.getPredicate().toString());
					// predicates.add(triple.getPredicate().toString());
					numberOfTriples++;

				}
			}
			if (node.getNameSpace().startsWith("http")) {
				distribution = new DistributionDB(node.getNameSpace());
				distribution.setTopDataset(dataset.getLODVaderID());
				distribution.setUri(node.getNameSpace());
				if (dataset.getTitle() != null)
					distribution.setTitle(dataset.getTitle());
				else if (dataset.getLabel() != null)
					distribution.setTitle(dataset.getLabel());
				
				distribution.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
				distribution.update(true);

				MakeLinksetsMasterThread makeLinksets = new MakeLinksetsMasterThread(subjectsQueue,
						node.getNameSpace());
				MakeLinksetsMasterThread makeLinksets2 = new MakeLinksetsMasterThread(objectsQueue,
						node.getNameSpace());
				makeLinksets2.tuplePart = TuplePart.OBJECT;
				makeLinksets.tuplePart = TuplePart.SUBJECT;
				makeLinksets.start();
				makeLinksets2.start();

				Thread.sleep(50);

				makeLinksets.setDoneSplittingString(true);
				makeLinksets2.setDoneSplittingString(true);
				makeLinksets.join();
				makeLinksets2.join();

				// distribution.update(true);

				SaveDist(node.getNameSpace(), subjects, objects, dataset.getLODVaderID(), dataset.getTitle());
			}
		}

	}

	public void SaveDist(String nameSpace, TreeSet<String> subjects, TreeSet<String> objects, int parentDynID,
			String parentTitle) throws Exception {

		Timer t = new Timer();
		t.startTimer();
		SubjectsBucket s = new SubjectsBucket(new TreeSet<String>(subjects), distribution.getLODVaderID());
		s.makeBucket();
		String timer1 = t.stopTimer();

		Timer t2 = new Timer();
		t.startTimer();
		ObjectsBucket o = new ObjectsBucket(new TreeSet<String>(objects), distribution.getLODVaderID());
		o.makeBucket();
		String timer2 = t2.stopTimer();

		ArrayList<Integer> parentDataset = new ArrayList<Integer>();
		parentDataset.add(parentDynID);

		distribution.setDownloadUrl(nameSpace);
		distribution.setUri(nameSpace);
		distribution.setDefaultDatasets(parentDataset);
		distribution.setTopDataset(parentDynID);
		distribution.setTriples(numberOfTriples);
		distribution.setFormat("nq");
		distribution.setTopDatasetTitle(parentTitle);
		distribution.setIsVocabulary(true);
		distribution.setNumberOfObjectTriples(objects.size());
		distribution.setNumberOfSubjectTriples(subjects.size());
		distribution.setSuccessfullyDownloaded(true);
		// distribution.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_CREATE_LINKSETS);
		distribution.setStatus(DistributionDB.STATUS_DONE);

		DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss dd/MM/yyyy");
		// get current date time with Date()
		Date date = new Date();

		distribution.setLastTimeStreamed(dateFormat.format(date).toString());

		distribution.update(true);

		numberOfTriples = 0;

		logger.info("Saving predicates...");

		new AllPredicatesDB().insertSet(allPredicates.keySet());
		new AllPredicatesRelationDB().insertSet(allPredicates, distribution.getLODVaderID(),
				distribution.getTopDatasetID());

		new RDFTypeObjectDB().insertSet(rdfTypeObjects.keySet());
		new RDFSubClassOfDB().insertSet(rdfSubClassOf.keySet());
		// new RDFTypeSubjectDB().insertSet(rdfTypeSubjects.keySet());

		new RDFTypeObjectRelationDB().insertSet(rdfTypeObjects, distribution.getLODVaderID(),
				distribution.getTopDatasetID());
		new RDFSubClassOfRelationDB().insertSet(rdfSubClassOf, distribution.getLODVaderID(),
				distribution.getTopDatasetID());
		// new RDFTypeSubjectRelationDB().insertSet(rdfTypeSubjects,
		// distribution.getDynLodID(), distribution.getTopDataset());

		logger.info("Saving OWL classes...");
		// Saving OWL classes
		new OwlClassDB().insertSet(owlClasses.keySet());
		new OwlClassRelationDB().insertSet(owlClasses, distribution.getLODVaderID(), distribution.getTopDatasetID());

		// new OWLClassQueries().insertOWLClasses(owlClasses,
		// distribution.getDynLodID(), distribution.getTopDataset());

		logger.info("Checking Jaccard Similarities...");
		// Checking Jaccard Similarities...
		LinkSimilarity linkSimilarity = new JaccardSimilarity();
		linkSimilarity.updateLinks(distribution, new AllPredicatesRelationDB());
		linkSimilarity.updateLinks(distribution, new RDFTypeObjectRelationDB());
		linkSimilarity.updateLinks(distribution, new RDFSubClassOfRelationDB());
		linkSimilarity.updateLinks(distribution, new OwlClassRelationDB());

		logger.info("Updating link strength among distributions...");
		// Saving link similarities
		LinkStrength linkStrength = new LinkStrength();
		linkStrength.updateLinks(distribution);

	}

	private void addToMap(HashMap<String, Integer> map, String value) {
		int n = 0;
		if (map.get(value) != null)
			n = map.get(value);
		map.put(value, n + 1);
	}

	@Override
	public void streamDistribution() throws IOException, LODVaderLODGeneralException, InterruptedException,
			RDFHandlerException, RDFParseException, LODVaderFormatNotAcceptedException {
		// TODO Auto-generated method stub

	}

}
