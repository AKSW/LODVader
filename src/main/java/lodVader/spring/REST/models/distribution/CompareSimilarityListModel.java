package lodVader.spring.REST.models.distribution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

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

public class CompareSimilarityListModel {

	ArrayList<SimilarityTableData> similarityTableData = new ArrayList<SimilarityTableData>();

	public DistributionDB distribution1;

	public DistributionDB distribution2;

	public ArrayList<SimilarityTableData> getSimilarityTableData() {
		return similarityTableData;
	}

	public void setSimilarityTableData(ArrayList<SimilarityTableData> similarityTableData) {
		this.similarityTableData = similarityTableData;
	}

	private class SimilarityTableData {

		public SimilarityTableData(String url, int link1, int link2) {
			this.url = url;
			this.link1 = link1;
			this.link2 = link2;
		}

		String url;

		Integer link1;

		Integer link2;

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public Integer getLink1() {
			return link1;
		}

		public void setLink1(Integer link1) {
			this.link1 = link1;
		}

		public Integer getLink2() {
			return link2;
		}

		public void setLink2(Integer link2) {
			this.link2 = link2;
		}

	}

	public void compareDatasets(int distribution1ID, int distribution2ID, String type) {

		distribution1 = new DistributionDB(distribution1ID);
		distribution2 = new DistributionDB(distribution2ID);

		HashSet<String> values1 = new HashSet<String>();
		HashSet<String> values2 = new HashSet<String>();
		HashSet<Integer> intersection = new HashSet<Integer>();

		String collectionName = "";

		if (type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)) {
			values1 = new AllPredicatesRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new AllPredicatesRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = AllPredicatesRelationDB.COLLECTION_NAME;
		} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)) {
			values1 = new OwlClassRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new OwlClassRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = OwlClassRelationDB.COLLECTION_NAME;
		} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)) {
			values1 = new RDFSubClassOfRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new RDFSubClassOfRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = RDFSubClassOfRelationDB.COLLECTION_NAME;
		} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_RDF_TYPE)) {
			values1 = new RDFTypeObjectRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new RDFTypeObjectRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = RDFTypeObjectRelationDB.COLLECTION_NAME;
		}

		intersection = makeIntersection(values1, values2);

		Set<GeneralRDFResourceRelationDB> relations = new GeneralRDFResourceRelationDB(collectionName)
				.getPredicatesIn(intersection, distribution1.getLODVaderID(), distribution2.getLODVaderID());

		// group by predicate value
		for (GeneralRDFResourceRelationDB relation : relations) {
			if (relation.getDistributionID() != distribution1ID) {

				int v1 = 0;
				int v2 = 0;

				for (GeneralRDFResourceRelationDB value2 : relations) {
					if (value2.getDistributionID() == distribution1.getLODVaderID()
							&& relation.getPredicateID() == value2.getPredicateID())
						v1 = value2.getAmount();
				}
				for (GeneralRDFResourceRelationDB value2 : relations) {
					if (value2.getDistributionID() == distribution2.getLODVaderID()
							&& relation.getPredicateID() == value2.getPredicateID())
						v2 = value2.getAmount();
				}

				if (type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)) {
					AllPredicatesDB a = new AllPredicatesDB(relation.getPredicateID());
					addSimilarityValue(new SimilarityTableData(a.getUri(), v1, v2));
				} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)) {
					OwlClassDB a = new OwlClassDB(relation.getPredicateID());
					addSimilarityValue(new SimilarityTableData(a.getUri(), v1, v2));
				} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)) {
					RDFSubClassOfDB a = new RDFSubClassOfDB(relation.getPredicateID());
					addSimilarityValue(new SimilarityTableData(a.getUri(), v1, v2));
				} else if (type.equals(ServiceAPIOptions.DATASET_TYPE_RDF_TYPE)) {
					RDFTypeObjectDB a = new RDFTypeObjectDB(relation.getPredicateID());
					addSimilarityValue(new SimilarityTableData(a.getUri(), v1, v2));
				}
			}
		}
	}

	private void addSimilarityValue(SimilarityTableData data) {
		boolean found = false;
		for (SimilarityTableData s : similarityTableData) {
			if (s.getUrl().equals(data.getUrl()))
				found = true;
		}
		if (!found)
			similarityTableData.add(data);
	}

	private HashSet<Integer> makeIntersection(HashSet<String> values1, HashSet<String> values2) {
		HashSet<Integer> intersection = new HashSet<Integer>();
		for (String i : values1) {
			if (values2.contains(i)) {
				intersection.add(Integer.valueOf(i));
			}
		}

		return intersection;
	}
}
