package lodVader.spring.REST.models.degree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DatasetLinksetDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.ResourceDB;

public class IndegreeDatasetModel {

	public StringBuilder result = new StringBuilder();

	public boolean isVocabulary = true;
	public boolean isDeadLinks = false;

	/**
	 * MapReduce functions for indegree linksets
	 */

	public String mapindegreeWithVocabs;

	public String mapindegreeNoVocabs;

	public String reduceinDegree;

	class Result implements Comparator<Result>, Comparable<Result> {
		int targetDataset;
		int links = 0;
		HashSet<Integer> sourceDatasetList = new HashSet<>();

		@Override
		public int compare(Result o1, Result o2) {
			return o1.links - o2.links;
		}

		@Override
		public int compareTo(Result o) {
			return this.sourceDatasetList.size() - o.sourceDatasetList.size();
		}
	}

	HashMap<Integer, Result> tmpResults = new HashMap<Integer, Result>();

	ArrayList<Result> finalList = new ArrayList<Result>();

	@Test
	public void calc() {

		/**
		 * MapReduce to find indegree with vocabularies
		 */

		result.append("===============================================================\n");
		result.append("Comparing with vocabularies\n");
		result.append("===============================================================\n\n");

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DatasetLinksetDB.COLLECTION_NAME);

		DBCursor instances;


		if(!isDeadLinks){
			BasicDBList and = new BasicDBList();
//			and.add(new BasicDBObject(DatasetLinksetDB.LINKS, new BasicDBObject("$gt", 0)));
//			and.add(new BasicDBObject(DatasetLinksetDB.DATASET_SOURCE, new BasicDBObject("$ne", DatasetLinksetDB.DATASET_TARGET)));
			
			instances = collection.find( new BasicDBObject(DatasetLinksetDB.LINKS, new BasicDBObject("$gt", 0)));
		}
		else{
			BasicDBList and = new BasicDBList();
//			and.add(new BasicDBObject(DatasetLinksetDB.DEAD_LINKS, new BasicDBObject("$gt", 0)));
//			and.add(new BasicDBObject(DatasetLinksetDB.DATASET_SOURCE, new BasicDBObject("$ne", DatasetLinksetDB.DATASET_TARGET)));
//			instances = collection.find( new BasicDBObject("$and", and));
			instances = collection.find(new BasicDBObject(DatasetLinksetDB.DEAD_LINKS, new BasicDBObject("$gt", 0)));
		}

		for (DBObject object : instances) {

			DatasetLinksetDB linkset = new DatasetLinksetDB(object);

			if (linkset.getDistributionTargetIsVocabulary() == isVocabulary) {

				Result result = tmpResults.get(linkset.getDatasetTarget());

				if (result == null) {
					result = new Result();
				}

				if (isDeadLinks)
					result.links = result.links + linkset.getDeadLinks();
				else
					result.links = result.links + linkset.getLinks();

				result.sourceDatasetList.add(linkset.getDatasetSource());
				result.targetDataset = linkset.getDatasetTarget();

				tmpResults.put(linkset.getDatasetTarget(), result);
			}
		}

		for (Integer r : tmpResults.keySet()) {
			finalList.add(tmpResults.get(r));
		}
		result.append("\n===== Sorted by links=======");
		Collections.sort(finalList, new Result());
		printTableindegree();

		result.append("\n===== Sorted by number of datasets=======");
		Collections.sort(finalList);
		printTableindegree();

		result.append("\n\n\n\n===============================================================\n");
		result.append("Comparing without vocabularies\n");
		result.append("===============================================================\n\n");

		tmpResults = new HashMap<Integer, Result>();

		finalList = new ArrayList<Result>();

		isVocabulary = false;

		for (DBObject object : instances) {

			DatasetLinksetDB linkset = new DatasetLinksetDB(object);

			if (linkset.getDistributionTargetIsVocabulary() == isVocabulary) {

				Result result = tmpResults.get(linkset.getDatasetTarget());

				if (result == null) {
					result = new Result();
				}

				if (isDeadLinks)
					result.links = result.links + linkset.getDeadLinks();
				else
					result.links = result.links + linkset.getLinks();

				result.sourceDatasetList.add(linkset.getDatasetSource());
				result.targetDataset = linkset.getDatasetTarget();

				tmpResults.put(linkset.getDatasetTarget(), result);
			}
		}

		for (Integer r : tmpResults.keySet()) {
			finalList.add(tmpResults.get(r));
		}
		result.append("\n===== Sorted by links=======");
		Collections.sort(finalList, new Result());
		printTableindegree();

		result.append("\n===== Sorted by number of datasets=======");
		Collections.sort(finalList);
		printTableindegree();

	}

	private void printTableindegree() {

		result.append("\n\nName\t indegree \t Links \n");

		DatasetDB tmpDataset;

		for (Result r : finalList) {
			tmpDataset = new DatasetDB(r.targetDataset);
			result.append(tmpDataset.getTitle());
			result.append("\t" + r.sourceDatasetList.size());
			result.append("\t" + r.links);
			result.append("\n");
		}

		result.append("\n\n\n");
	}

}
