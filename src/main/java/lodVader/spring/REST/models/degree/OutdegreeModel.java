package lodVader.spring.REST.models.degree;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.TmpMapReduce;

public class OutdegreeModel {

	StringBuilder result = new StringBuilder();

	int n = 10;

	@Test
	public void mapReduceOutdegree() {
		String map = "function() { " + "if ( this.links > 0 )"
				+ "emit(this.distributionSource, {'distribution': this.distributionSource, "
				+ "'totalDistributionsOutdegree': 1," + "'links': this.links});" + "};";

		String reduce = "function(key, values) {" + "var linksSum = 0;" + "var distributionSum = 0;"
				+ "values.forEach(function(linkset) {" + "linksSum += linkset.links;" + "distributionSum += 1;" + "});"
				+ "return {'distribution': key, 'totalDistributionsOutdegree':distributionSum,'links':linksSum};"
				+ "};";

		MapReduceCommand cmd = new MapReduceCommand(DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME), map, reduce,

		TmpMapReduce.COLLECTION_NAME, MapReduceCommand.OutputType.REPLACE, null);

		DBSuperClass2.getCollection(LinksetDB.COLLECTION_NAME).mapReduce(cmd);

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(TmpMapReduce.COLLECTION_NAME);

		DBCursor instances;

		instances = collection.find().sort(new BasicDBObject("value.links", -1));

		result.append("=====================================================\n");
		result.append("Top " + n + " distributions by number of outdegree links (vocabularies) \n");
		result.append("=====================================================\n\n");

		printTable(instances, true);
		printTable(instances, false);

		instances = collection.find().sort(new BasicDBObject("value.totalDistributionsOutdegree", -1));

		result.append("=====================================================\n");
		result.append("Top " + n + " distributions by number of outdegree distributions\n");
		result.append("=====================================================\n\n");

		printTable(instances,true);

		System.out.println(result.toString());
	}

	private void printTable(DBCursor instances, boolean printWithVocabulary) {
		result.append("Distribution Name\t Distributions Outdegree \t Links Outdegree\n");

		BasicDBObject values = null;
		DistributionDB tmpDistribution;

		for (DBObject instance : instances) {

			tmpDistribution = new DistributionDB(((Number) (instance.get("_id"))).intValue());
			if (tmpDistribution.getIsVocabulary() == printWithVocabulary) {
				values = new BasicDBObject((BasicDBObject) instance.get("value"));

				result.append(tmpDistribution.getTitle());
				result.append("\t" + values.get("totalDistributionsOutdegree"));
				result.append("\t" + values.getString("links"));

				result.append("\n");
			}
		}

		result.append("\n\n\n");
	}

}
