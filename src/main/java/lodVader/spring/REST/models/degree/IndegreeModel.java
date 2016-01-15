package lodVader.spring.REST.models.degree;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.TmpMapReduce;

public class IndegreeModel {

	public StringBuilder result = new StringBuilder();

	/**
	 * MapReduce functions for indegree linksets
	 */

	public String mapIndegreeWithVocabs = "function() { "
			+ "if ( this.links > 0 && this.distributionSourceIsVocabulary == true )"
			+ "emit(this.distributionTarget, {'distribution': this.distributionTarget, "
			+ "'totalDistributionsIndegree': 1," + "'links': this.links});" + "};";

	public String mapIndegreeNoVocabs = "function() { "
			+ "if ( this.links > 0 && this.distributionSourceIsVocabulary == false )"
			+ "emit(this.distributionTarget, {'distribution': this.distributionTarget, "
			+ "'totalDistributionsIndegree': 1," + "'links': this.links});" + "};";

	public String reduceInDegree = "function(key, values) {" + "var linksSum = 0;" + "var distributionSum = 0;"
			+ "values.forEach(function(linkset) {" + "linksSum += linkset.links;" + "distributionSum += 1;" + "});"
			+ "return {'distribution': key, 'totalDistributionsIndegree':distributionSum,'links':linksSum};" + "};";

	public void mapReduceInDegree(int n) {
		/**
		 * MapReduce to find indegree with vocabularies
		 */

		MapReduceCommand cmd = new MapReduceCommand(DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME),
				mapIndegreeWithVocabs, reduceInDegree, TmpMapReduce.COLLECTION_NAME,
				MapReduceCommand.OutputType.REPLACE, null);

		DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME).mapReduce(cmd);

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(TmpMapReduce.COLLECTION_NAME);
		DBCursor instances = collection.find().sort(new BasicDBObject("value.links", -1)).limit(n);
		result.append("\n\n\n\n===============================================================\n");
		result.append("Comparing with vocabularies\n");
		result.append("===============================================================\n\n");
		
		
		result.append("=====================================================\n");
		result.append("Top " + n + " by number of indegree links\n");
		result.append("=====================================================\n\n");

		printTableIndegree(instances);

		instances = collection.find().sort(new BasicDBObject("value.totalDistributionsOutdegree", -1)).limit(n);

		result.append("=====================================================\n");
		result.append("Top " + n + " by number of indegree distributions/datasets\n");
		result.append("=====================================================\n\n");

		printTableIndegree(instances);

		/**
		 * MapReduce to find indegree with distributions (not vocabularies)
		 */

		cmd = new MapReduceCommand(DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME), mapIndegreeNoVocabs,
				reduceInDegree, TmpMapReduce.COLLECTION_NAME, MapReduceCommand.OutputType.REPLACE, null);

		DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME).mapReduce(cmd);

		collection = DBSuperClass2.getDBInstance().getCollection(TmpMapReduce.COLLECTION_NAME);
		instances = collection.find().sort(new BasicDBObject("value.links", -1)).limit(n);
		
		result.append("\n\n\n\n===============================================================\n");
		result.append("Comparing with datasets/distributions\n");
		result.append("===============================================================\n\n");

		result.append("=====================================================\n");
		result.append("Top " + n + " by number of indegree links \n");
		result.append("=====================================================\n\n");

		printTableIndegree(instances);

		instances = collection.find().sort(new BasicDBObject("value.totalDistributionsOutdegree", -1)).limit(n);

		result.append("=====================================================\n");
		result.append("Top " + n + " by number of indegree distributions\n");
		result.append("=====================================================\n\n");

		printTableIndegree(instances);

	}

	private void printTableIndegree(DBCursor instances) {
		result.append("Name\t Indegree \t Links\n");

		BasicDBObject values = null;
		DistributionDB tmpDistribution;

		for (DBObject instance : instances) {

			tmpDistribution = new DistributionDB(((Number) (instance.get("_id"))).intValue());
			values = new BasicDBObject((BasicDBObject) instance.get("value"));

			result.append(tmpDistribution.getTitle());
			result.append("\t" + values.get("totalDistributionsIndegree"));
			result.append("\t" + values.getString("links"));

			result.append("\n");
		}

		result.append("\n\n\n");
	}

}
