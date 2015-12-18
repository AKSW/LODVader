package lodVader.mongodb.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.LODVaderProperties;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;

public class DatasetQueries {

	static long triples = 0;

	public long getNumberOfTripless(String dataset) {
		triples = 0;
		getNumberOfTriples(new DatasetDB(dataset));
		return triples;
	}

	public long getNumberOfTriples(DatasetDB dataset) {
		triples = 0;
		getTriples(dataset);
		return triples;
	}

	private void getTriples(DatasetDB dataset) {

		for (int subset : dataset.getSubsetsIDs()) {
			getTriples(new DatasetDB(subset));
		}

		for (int dist : dataset.getDistributionsIDs()) {
			DistributionDB d = new DistributionDB(dist);
			triples = triples + d.getTriples();
		}
	}

	// return number of datasets (vocabularies and not)
	public int getNumberOfDatasets() {
		int numberOfDatasets = 0;
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			numberOfDatasets = (int) collection.count();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfDatasets;
	}

	/**
	 * Get all datasets
	 * @return list of datasets
	 */
	public ArrayList<DatasetDB> getDatasets(Boolean vocabularies) {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			
			DBCursor instances ;
			
			if(vocabularies==null)
				instances = collection.find();
			else if(vocabularies)
				instances = collection.find(new BasicDBObject(DatasetDB.IS_VOCABULARY, true));
			else
				instances = collection.find(new BasicDBObject(DatasetDB.IS_VOCABULARY, false));
			
			instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// get an array of datasets
	public ArrayList<DatasetDB> getDatasets(ArrayList<Integer> datasetsIDs) {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject();
			query.put(DatasetDB.LOD_VADER_ID, new BasicDBObject("$in", datasetsIDs));
			
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all datasets
	public ArrayList<DatasetDB> getDatasetsNotVocab() {
//		public ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetDB.IS_VOCABULARY, false);
			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetDB.TITLE, 1));

			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// return all datasets
	public ArrayList<DatasetDB> getTopDatasetsNotVocab() {
//		public ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetDB.IS_VOCABULARY, false);
			ArrayList<Integer> topDatasetID = new ArrayList<Integer>();
			topDatasetID.add(0);
			query.append(DatasetDB.PARENT_DATASETS, new BasicDBObject("$in", topDatasetID));
			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetDB.TITLE, 1));

			for (DBObject instance : instances) {
				if(((ArrayList<String>) instance.get(DatasetDB.PARENT_DATASETS)).size()==1){
					list.add(new DatasetDB(instance));
					
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	
	// return all datasets
	public ArrayList<DatasetDB> getDatasetsByID(ArrayList<Integer> ids) {
//		public ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetDB.IS_VOCABULARY, false);
			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetDB.TITLE, 1));

			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all datasets not vocabularies with links

	// public void getDatasetsNotVocabWithLinks() {
	public ArrayList<DatasetDB> getDatasetsNotVocabWithLinks() {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(LinksetDB.LINK_NUMBER_LINKS,
					new BasicDBObject("$gt", LODVaderProperties.LINKSET_TRESHOLD));
			List<Integer> out = collection.distinct(
					LinksetDB.DATASET_TARGET, query);

			List<Integer> in = collection.distinct(
					LinksetDB.DATASET_SOURCE, query);

			TreeSet<Integer> t = new TreeSet<Integer>();
			for (Integer s : out) {
				t.add(s);
			}
			for (Integer s : in) {
				t.add(s);
			}

			collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			query = new BasicDBObject(DatasetDB.LOD_VADER_ID,
					new BasicDBObject("$in", t));
			query.append(DatasetDB.IS_VOCABULARY, false);

			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetDB.TITLE, 1));

			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all datasets
	public ArrayList<DatasetDB> getDatasetsVocab() {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetDB.IS_VOCABULARY, true);
			// query.append("$where", "this.distributions_uris.length > 0");
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public ArrayList<DatasetDB> getSubsetsAsMongoDBObject(DatasetDB dataset) {

		ArrayList<DatasetDB> list = new ArrayList<DatasetDB>();
		if(dataset.getSubsetsIDs().size()==0)
			return list;
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetDB.LOD_VADER_ID, new BasicDBObject("$in", dataset.getSubsetsIDs()));

			// query.append("$where", "this.distributions_uris.length > 0");
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));  
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public ArrayList<DistributionDB> getDistributionsAsMongoDBObject(DatasetDB dataset) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		try {
			DBCollection collection = DBSuperClass2.getCollection(
					DistributionDB.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DistributionDB.LOD_VADER_ID, new BasicDBObject("$in", dataset.getDistributionsIDs()));
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

}
