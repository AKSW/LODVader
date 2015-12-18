package lodVader.spring.REST.models.distribution;

import java.net.MalformedURLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import lodVader.LODVaderProperties;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.exceptions.api.DynamicLODAPINoLinksFoundException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.mongodb.queries.LinksetQueries;
import lodVader.ontology.NS;
import lodVader.ontology.RDFProperties;

public class RetrieveRDF {
	public Model outModel = null;

	public String publicURL;

	final static Logger logger = LoggerFactory.getLogger(RetrieveRDF.class);

	public RetrieveRDF(String source, String target, String pulicURL)
			throws LODVaderNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		this.publicURL = pulicURL;
		retrieveRDF(source, target);
	}

	public RetrieveRDF(String URI, String pulicURL)
			throws LODVaderNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		this.publicURL = pulicURL;
		retrieveRDF(URI, (String) null);
	}

	public void retrieveRDF(String source, String target)
			throws LODVaderNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		outModelInit();

		// try to find by distribution
		DistributionDB dist; 
		try {
			boolean found = false;
			dist = new DistributionDB(source);

			if (dist.getDefaultDatasets() != null) {
				if (dist.getDefaultDatasets().size() > 0) {
					retrieveByDistribution(dist.getUri());
					logger.debug("APIRetrieve found a distribution to retrieve RDF: " + dist.getUri());
					found = true;
				}
			} else {
				DatasetDB d = new DatasetDB(source);
				getDatasetChildren(d);
				found = true;
			}
			if (!found)
				throw new LODVaderNoDatasetFoundException("We couldn't find the dataset: " + source);

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// printModel();
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

	public void retrieveByDistribution(String distributionURI)
			throws DynamicLODAPINoLinksFoundException, MalformedURLException {
		// get indegree and outdegree for a distribution
		DistributionDB dis = new DistributionDB(distributionURI);

		ArrayList<LinksetDB> in = new LinksetQueries().getLinksetsInDegreeByDistribution(dis.getLODVaderID(),
				LinksetDB.LINK_NUMBER_LINKS, LODVaderProperties.LINKSET_TRESHOLD, -1);
		ArrayList<LinksetDB> out = new LinksetQueries().getLinksetsOutDegreeByDistribution(dis.getLODVaderID(),
				LinksetDB.LINK_NUMBER_LINKS, LODVaderProperties.LINKSET_TRESHOLD, -1);

		// add choosen distribution to jena
		// addDistributionToModel(new
		// DistributionMongoDBObject(distributionURI));

		boolean linksetsFound = false;

		// add linksets to jena model
		for (LinksetDB linkset : in) {
			DistributionDB distributionSubject = new DistributionDB(linkset.getDistributionTarget());

			DistributionDB distributionObject = new DistributionDB(linkset.getDistributionSource());

			for (int d1 : distributionSubject.getDefaultDatasets()) {
				for (int d2 : distributionObject.getDefaultDatasets()) {
					if (addLinksetToModel(d2, d1, linkset.getLinks()))
						linksetsFound = true;
				}
			}
		}
		// add linksets to jena model
		for (LinksetDB linkset : out) {
			DistributionDB distributionSubject = new DistributionDB(linkset.getDistributionTarget());

			DistributionDB distributionObject = new DistributionDB(linkset.getDistributionSource());

			for (int d1 : distributionSubject.getDefaultDatasets()) {
				for (int d2 : distributionObject.getDefaultDatasets()) {
					if (addLinksetToModel(d2, d1, linkset.getLinks()))
						linksetsFound = true;
				}
			}

		}
	}

	private void addDistributionToModel(DistributionDB distribution) {
		// add distribution to jena model
		Resource r = outModel.createResource(distribution.getDownloadUrl());
		r.addProperty(RDFProperties.type, ResourceFactory.createResource(NS.DCAT_URI + "distribution"));

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
		r.addProperty(RDFProperties.type, ResourceFactory.createResource(NS.VOID_URI + "Dataset"));

		String name;

		if (dataset.getTitle() == null)
			name = dataset.getUri();
		else
			name = dataset.getTitle();

		r.addProperty(RDFProperties.title, name);
		r.addProperty(RDFProperties.triples, String.valueOf(new DatasetQueries().getNumberOfTriples(dataset)));
		r.addProperty(RDFProperties.subset, outModel.createResource(subset));
	}

	private boolean addLinksetToModel(int source, int target, int links) {
		DatasetDB datasetSource = new DatasetDB(source);
		DatasetDB datasetTarget = new DatasetDB(target);

		if (!datasetSource.getIsVocabulary() && !datasetTarget.getIsVocabulary()) {
			// add linksets
			// String linksetURI = target + "_" + source;
			String linksetURI = publicURL + "?retrieveDataset&source=" + datasetSource.getUri() + "&target="
					+ datasetTarget.getUri();
			Resource r = outModel.createResource(linksetURI);
			Resource wasDerivedFrom = outModel.createResource(publicURL);

			r.addProperty(RDFProperties.type, ResourceFactory.createResource(NS.VOID_URI + "Linkset"));
			r.addProperty(ResourceFactory.createProperty(NS.VOID_URI + "objectsTarget"),
					ResourceFactory.createResource(datasetSource.getUri()));
			r.addProperty(ResourceFactory.createProperty(NS.VOID_URI + "subjectsTarget"),
					ResourceFactory.createResource(datasetTarget.getUri()));
			r.addProperty(RDFProperties.wasDerivedFrom, wasDerivedFrom);
			r.addProperty(ResourceFactory.createProperty(NS.VOID_URI + "triples"),
					ResourceFactory.createPlainLiteral(String.valueOf(links)));

			// describe dadaset with this uri as subset
			addDatasetToModel(datasetSource, linksetURI);
			addDatasetToModel(datasetTarget, linksetURI);

			return true;
		} else
			return false;

	}

	public void getDatasetChildren(DatasetDB d)
			throws LODVaderNoDatasetFoundException, DynamicLODAPINoLinksFoundException, MalformedURLException {
		boolean datasetOrDistribuionFound = false;

		for (int child : d.getSubsetsIDs()) {
			DatasetDB datasetChild = new DatasetDB(child);
			getDatasetChildren(datasetChild);
			addDatasetToModel(d, new DatasetDB(child).getUri());
			datasetOrDistribuionFound = true;
		}

		for (int dist : d.getDistributionsIDs()) {
			retrieveByDistribution(new DistributionDB(dist).getUri());
			datasetOrDistribuionFound = true;
		}

		if (!datasetOrDistribuionFound)
			throw new LODVaderNoDatasetFoundException("Not possible to find datasets, subsets or distributions within "
					+ d.getUri() + ". Please enter a valid dataset URI.");

	}

}
