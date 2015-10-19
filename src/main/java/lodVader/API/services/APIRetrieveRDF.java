package lodVader.API.services;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import lodVader.LODVaderProperties;
import lodVader.API.core.API;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.exceptions.api.DynamicLODAPINoLinksFoundException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.LinksetQueries;
import lodVader.ontology.NS;
import lodVader.ontology.RDFProperties;

public class APIRetrieveRDF extends API {

	public Model outModel = null;
	
	final static Logger logger = Logger.getLogger(APIRetrieveRDF.class);

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public APIRetrieveRDF() {
		// TODO Auto-generated constructor stub
	}

//	@Test
//	public void t() throws DynamicLODNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
//		outModelInit();
//		getDatasetChildren(new DatasetMongoDBObject(
//				"http://gerbil.aksw.org/gerbil/dataId/corpora/N3-RSS-500#dataset"));
//		printModel();
//	}
	
	

	public APIRetrieveRDF(String source, String target) throws LODVaderNoDatasetFoundException,
			DynamicLODAPINoLinksFoundException{
		retrieveRDF(source, target);
	}
	public APIRetrieveRDF(String URI) throws LODVaderNoDatasetFoundException,
	DynamicLODAPINoLinksFoundException {
		retrieveRDF(URI, (String) null);
	}
	
	public void retrieveRDF(String source, String target) throws LODVaderNoDatasetFoundException,
	DynamicLODAPINoLinksFoundException{
		
		outModelInit();
		
		// try to find by distribution
		DistributionDB dist = new DistributionDB(source);
		
		if(dist.getDefaultDatasets().size()>0){
			retrieveByDistribution(dist.getUri());
			logger.debug("APIRetrieve found a distribution to retrieve RDF: "+ dist.getUri());
		}
		else{
			DatasetDB d = new DatasetDB(source, true);
			getDatasetChildren(d);
		}
//		printModel();
	}
	



	private void outModelInit() {
		outModel = ModelFactory.createDefaultModel();
		outModel.setNsPrefix("rdfs", NS.RDFS_URI);
		outModel.setNsPrefix("dcat", NS.DCAT_URI);
		outModel.setNsPrefix("void", NS.VOID_URI);
		outModel.setNsPrefix("sd", NS.SD_URI);
		outModel.setNsPrefix("prov", NS.PROV_URI);
		outModel.setNsPrefix("dct", NS.DCT_URI);
		outModel.setNsPrefix("xsd", NS.XSD_URI);
		outModel.setNsPrefix("foaf", NS.FOAF_URI);
	}

	private void printModel() {
		outModel.write(System.out, "TURTLE");
	}

	public void retrieveByDistribution(String distributionURI) throws DynamicLODAPINoLinksFoundException {
		// get indegree and outdegree for a distribution
		DistributionDB dis = new DistributionDB(distributionURI);
		
		ArrayList<LinksetDB> in = new LinksetQueries()
				.getLinksetsInDegreeByDistribution(dis.getLODVaderID(), LinksetDB.LINK_NUMBER_LINKS, 50,-1);
		ArrayList<LinksetDB> out = new LinksetQueries()
				.getLinksetsOutDegreeByDistribution(dis.getLODVaderID(), LinksetDB.LINK_NUMBER_LINKS,50,-1);

		// add choosen distribution to jena
		// addDistributionToModel(new
		// DistributionMongoDBObject(distributionURI));
		
		boolean linksetsFound = false;

		// add linksets to jena model
		for (LinksetDB linkset : in) {
			DistributionDB distributionSubject = new DistributionDB(
					linkset.getDistributionTarget());

			DistributionDB distributionObject =  new DistributionDB(
					linkset.getDistributionSource());

			for (int d1 : distributionSubject.getDefaultDatasets()) {
				for (int d2 : distributionObject.getDefaultDatasets()) {
					if(addLinksetToModel(d2, d1, linkset.getLinks()))
						linksetsFound = true;
				}
			}
		}
		// add linksets to jena model
		for (LinksetDB linkset : out) {
			DistributionDB distributionSubject =  new DistributionDB(
					linkset.getDistributionTarget());

			DistributionDB distributionObject = new DistributionDB(
					linkset.getDistributionSource());

			for (int d1 : distributionSubject.getDefaultDatasets()) {
				for (int d2 : distributionObject.getDefaultDatasets()) {
					if(addLinksetToModel(d2, d1, linkset.getLinks()))
						linksetsFound = true;
				}
			}

		}
		
//		if(!linksetsFound)
//			throw new DynamicLODAPINoLinksFoundException ("Your dataset still doesn't not contains links with our stored datasets.");

	}

	private void addDistributionToModel(DistributionDB distribution) {
		// add distribution to jena model
		Resource r = outModel.createResource(distribution.getDownloadUrl());
		r.addProperty(RDFProperties.type,
				ResourceFactory.createResource(NS.DCAT_URI + "distribution"));

		String name;

		if (distribution.getTitle() == null)
			name = distribution.getUri();
		else
			name = distribution.getTitle();

		r.addProperty(RDFProperties.title, name);
	}

	private void addDatasetToModel(DatasetDB dataset, String subset) {
		// add distribution to jena model
		Resource r = outModel.createResource(dataset.getUri());
		r.addProperty(RDFProperties.type,
				ResourceFactory.createResource(NS.VOID_URI + "Dataset"));

		String name;

		if (dataset.getTitle() == null)
			name = dataset.getUri();
		else
			name = dataset.getTitle();

		r.addProperty(RDFProperties.title, name);
		r.addProperty(RDFProperties.triples,
				String.valueOf(new DatasetQueries().getNumberOfTriples(dataset)));
		r.addProperty(RDFProperties.subset, outModel.createResource(subset));
	}

	private boolean addLinksetToModel(int source, int target, int links) {
		DatasetDB datasetSource = new DatasetDB(source);
		DatasetDB datasetTarget =new DatasetDB(target);

		if (!datasetSource.getIsVocabulary()
				&& !datasetTarget.getIsVocabulary()) {
//			 add linksets
//			String linksetURI = target + "_" + source;
			String linksetURI = lodVader.API.server.ServiceAPI.getServerURL() + "?retrieveDataset&source="+
					datasetSource.getUri() + "&target="+ datasetTarget.getUri();
			Resource r = outModel.createResource(linksetURI);
			Resource wasDerivedFrom = outModel
					.createResource(lodVader.API.server.ServiceAPI.getServerURL());

			r.addProperty(RDFProperties.type,
					ResourceFactory.createResource(NS.VOID_URI + "Linkset"));
			r.addProperty(
					ResourceFactory.createProperty(NS.VOID_URI
							+ "objectsTarget"),
					ResourceFactory.createResource(datasetSource.getUri()));
			r.addProperty(
					ResourceFactory.createProperty(NS.VOID_URI
							+ "subjectsTarget"),
					ResourceFactory.createResource(datasetTarget.getUri()));
			r.addProperty(RDFProperties.wasDerivedFrom, wasDerivedFrom);
			r.addProperty(
					ResourceFactory.createProperty(NS.VOID_URI + "triples"),
					ResourceFactory.createPlainLiteral(String.valueOf(links)));

			// describe dadaset with this uri as subset
			addDatasetToModel(datasetSource, linksetURI);
			addDatasetToModel(datasetTarget, linksetURI);
			
			return true;
		}
		else return false;

	}

	public void getDatasetChildren(DatasetDB d)
			throws LODVaderNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		boolean datasetOrDistribuionFound = false;

		for (int child : d.getSubsetsIDs()) {
			DatasetDB datasetChild = new DatasetDB(child);
			getDatasetChildren(datasetChild);
			addDatasetToModel(d, new DatasetDB(child).getUri());
			datasetOrDistribuionFound = true;
		}

		for (int dist : d.getDistributionsIDs()) {
			retrieveByDistribution(new  DistributionDB(dist).getUri());
			datasetOrDistribuionFound = true;
		}

		if (!datasetOrDistribuionFound)
			throw new LODVaderNoDatasetFoundException(
					"Not possible to find datasets, subsets or distributions within "
							+ d.getUri()+". Please enter a valid dataset URI.");

	}

}
