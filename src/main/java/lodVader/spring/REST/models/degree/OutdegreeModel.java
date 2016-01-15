package lodVader.spring.REST.models.degree;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.ResourceDB;
import lodVader.mongodb.collections.TmpMapReduce;

public class OutdegreeModel {

	public StringBuilder result = new StringBuilder();
	
	public boolean isDataset = false;
	
	/**
	 * MapReduce functions for outdegree linksets
	 */

	public  String mapOutdegreeWithVocabs = "function() { " + "if ( this.links > 0 && this.distributionTargetIsVocabulary == true )"
			+ "emit(this.distributionSource, {'distribution': this.distributionSource, "
			+ "'totalDistributionsOutdegree': 1," + "'links': this.links});" + "};";
	
	public String mapOutdegreeNoVocabs = "function() { " + "if ( this.links > 0 && this.distributionTargetIsVocabulary == false )"
			+ "emit(this.distributionSource, {'distribution': this.distributionSource, "
			+ "'totalDistributionsOutdegree': 1," + "'links': this.links});" + "};";

	public String reduceOutDegree = "function(key, values) {" + "var linksSum = 0;" + "var distributionSum = 0;"
			+ "values.forEach(function(linkset) {" + "linksSum += linkset.links;" + "distributionSum += 1;" + "});"
			+ "return {'distribution': key, 'totalDistributionsOutdegree':distributionSum,'links':linksSum};" + "};";
	
	
	

	public void mapReduceOutDegree(int n) {

		
		/**
		 * MapReduce to find outdegree with vocabularies
		 */
		
		MapReduceCommand cmd = new MapReduceCommand(DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME),
				mapOutdegreeWithVocabs, reduceOutDegree,	TmpMapReduce.COLLECTION_NAME, MapReduceCommand.OutputType.REPLACE, null);

		DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME).mapReduce(cmd);

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(TmpMapReduce.COLLECTION_NAME);

		DBCursor instances;

		instances = collection.find().sort(new BasicDBObject("value.links", -1)).limit(n);
		
		result.append("\n\n\n\n===============================================================\n");
		result.append("Comparing with vocabularies\n");
		result.append("===============================================================\n\n");

		result.append("=====================================================\n");
		result.append("Top " + n + " by number of outdegree links \n");
		result.append("=====================================================\n\n");

		printTableOutdegree(instances);

		instances = collection.find().sort(new BasicDBObject("value.totalOutdegree", -1)).limit(n);

		result.append("=====================================================\n");
		result.append("Top " + n + " by number of outdegree distributions\n");
		result.append("=====================================================\n\n");

		printTableOutdegree(instances);
		
		
		/**
		 * MapReduce to find outdegree with distributions (not vocabularies)
		 */
		
		cmd = new MapReduceCommand(DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME),
				mapOutdegreeNoVocabs, reduceOutDegree,	TmpMapReduce.COLLECTION_NAME, MapReduceCommand.OutputType.REPLACE, null);

		DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME).mapReduce(cmd);

		collection = DBSuperClass2.getDBInstance().getCollection(TmpMapReduce.COLLECTION_NAME);

		instances = collection.find().sort(new BasicDBObject("value.links", -1)).limit(n);

		result.append("\n\n\n\n===============================================================\n");
		result.append("Comparing with distributions/datasets\n");
		result.append("===============================================================\n\n");
		
		result.append("=====================================================\n");
		result.append("Top " + n + " by number of outdegree links  \n");
		result.append("=====================================================\n\n");

		printTableOutdegree(instances);

		instances = collection.find().sort(new BasicDBObject("value.totalOutdegree", -1)).limit(n);

		result.append("=====================================================\n");
		result.append("Top " + n + " by number of outdegree distributions\n");
		result.append("=====================================================\n\n");

		printTableOutdegree(instances);
		
		


		
//		System.out.println(result.toString());
	}

	private void printTableOutdegree(DBCursor instances) {
		result.append("Name\t Outdegree \t Links \n");

		BasicDBObject values = null;
		ResourceDB tmpDistribution;

		for (DBObject instance : instances) {

			if (!isDataset)
				tmpDistribution = new DistributionDB(((Number) (instance.get("_id"))).intValue());
			else
				tmpDistribution = new DatasetDB(((Number) (instance.get("_id"))).intValue());

			values = new BasicDBObject((BasicDBObject) instance.get("value"));

			result.append(tmpDistribution.getTitle());
			result.append("\t" + values.get("totalOutdegree"));
			result.append("\t" + values.getString("links"));

			result.append("\n");
		}

		result.append("\n\n\n");
	}
	
}
