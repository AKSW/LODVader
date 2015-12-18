package lodVader.spring.REST.models.distribution;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.spring.REST.ServiceAPIOptions;

public class DetailModel {

	private DistributionDB distribution;

	private String type;

	private String rdfURL;

	private ArrayList<TopN> topNList = new ArrayList<TopN>();

	private class TopN {

		public TopN(String resource, String amount) {
			this.resource = resource;
			this.amount = amount;
		}

		public TopN() {
		}

		String resource;

		String amount;

		public String getResource() {
			return resource;
		}

		public void setResource(String resource) {
			this.resource = resource;
		}

		public String getAmount() {
			return amount;
		}

		public void setAmount(String amount) {
			this.amount = amount;
		}

	}

	public void details(int distributionID, int topN, String type) {

		this.type = type;
		NumberFormat formatterLinks = new DecimalFormat("###,###,###,###");
		distribution = new DistributionDB(distributionID);

		String collectionName = "";

		if (type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)) {
			collectionName = AllPredicatesRelationDB.COLLECTION_NAME;
		} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)) {
			collectionName = OwlClassRelationDB.COLLECTION_NAME;
		} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)) {
			collectionName = RDFSubClassOfRelationDB.COLLECTION_NAME;
		} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_RDF_TYPE)) {
			collectionName = RDFTypeObjectRelationDB.COLLECTION_NAME;
		}

		List<GeneralRDFResourceRelationDB> list = new GeneralRDFResourceRelationDB(collectionName)
				.getTopNPredicates(distributionID, topN);

		for (GeneralRDFResourceRelationDB d : list) {
			TopN topNresource = new TopN();

			if (type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES))
				topNresource.setResource(new OwlClassDB(d.getPredicateID()).getUri());
			else if (type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES))
				topNresource.setResource(new RDFSubClassOfDB(d.getPredicateID()).getUri());
			else if (type.equals(ServiceAPIOptions.DATASET_TYPE_RDF_TYPE))
				topNresource.setResource(new RDFTypeObjectDB(d.getPredicateID()).getUri());
			else
				topNresource.setResource(new AllPredicatesDB(d.getPredicateID()).getUri());

			topNresource.setAmount(formatterLinks.format(d.getAmount()));
			topNList.add(topNresource);
		}
	
	}

	public DistributionDB getDistribution() {
		return distribution;
	}

	public void setDistribution(DistributionDB distribution) {
		this.distribution = distribution;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getRdfURL() {
		return rdfURL;
	}

	public void setRdfURL(String rdfURL) {
		this.rdfURL = rdfURL;
	}

	public ArrayList<TopN> getTopNList() {
		return topNList;
	}

	public void setTopNList(ArrayList<TopN> topNList) {
		this.topNList = topNList;
	}

}
