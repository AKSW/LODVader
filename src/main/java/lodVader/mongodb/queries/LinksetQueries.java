package lodVader.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.API.diagram.DiagramData;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.spring.REST.ServiceAPIOptions;

public class LinksetQueries {

	/**
	 * 
	 * @return number of total discovered links
	 */
	public Double getNumberOfDiscoveredLinks() {
		Double numberOfTriples = 0.0;
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(LinksetDB.COLLECTION_NAME);

			BasicDBObject groupFields = new BasicDBObject("_id", null);

			groupFields.append("sum", new BasicDBObject("$sum", "$links"));

			DBObject group = new BasicDBObject("$group", groupFields);

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(group);
			AggregationOutput output = collection.aggregate(pipeline);

			for (DBObject result : output.results()) {
				numberOfTriples = Double.valueOf(result.get("sum").toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfTriples;
	}

	public ArrayList<LinksetDB> getLinksets(BasicDBObject query) {

		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(LinksetDB.COLLECTION_NAME);

		DBCursor d = collection.find(query);

		while (d.hasNext()) {
			list.add(new LinksetDB(d.next()));
		}

		return list;

	}

	public ArrayList<LinksetDB> getLinksets(int distribution, int topValue, String type) {

		DistributionDB distributionDB = new DistributionDB(distribution);

		try {
			ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();

			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(LinksetDB.COLLECTION_NAME);

			BasicDBObject query = new BasicDBObject(LinksetDB.DISTRIBUTION_SOURCE, distributionDB.getLODVaderID());

			BasicDBObject sort;

			if (type.equals(ServiceAPIOptions.DATASET_TYPE_LINKS))
				sort = new BasicDBObject(LinksetDB.LINK_NUMBER_LINKS, -1);
			else if (type.equals(ServiceAPIOptions.DATASET_TYPE_TOP_BAD_LINKS))
				sort = new BasicDBObject(LinksetDB.INVALID_LINKS, -1);
			else if (type.equals(ServiceAPIOptions.DATASET_TYPE_STRENGTH))
				sort = new BasicDBObject(LinksetDB.LINK_STRENGHT, -1);
			else if (type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES))
				sort = new BasicDBObject(LinksetDB.OWL_CLASS_SIMILARITY, -1);
			else if (type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES))
				sort = new BasicDBObject(LinksetDB.RDF_SUBCLASS_SIMILARITY, -1);
			else if (type.equals(ServiceAPIOptions.DATASET_TYPE_RDF_TYPE))
				sort = new BasicDBObject(LinksetDB.RDF_TYPE_SIMILARITY, -1);
			else
				sort = new BasicDBObject(LinksetDB.PREDICATE_SIMILARITY, -1);

			DBCursor instances = collection.find(query).sort(sort).limit(topValue);

			if (type.equals(ServiceAPIOptions.DATASET_TYPE_TOP_BAD_LINKS)) {
				int lastDataset = -1;
				for (DBObject instance : instances) {
					if (lastDataset != Integer.parseInt(instance.get(LinksetDB.DATASET_TARGET).toString()))
						list.add(new LinksetDB(instance.get(LinksetDB.LINKSET_ID).toString()));
					lastDataset = Integer.parseInt(instance.get(LinksetDB.DATASET_TARGET).toString());
				}
			} else
				for (DBObject instance : instances) {
					list.add(new LinksetDB(instance.get(LinksetDB.LINKSET_ID).toString()));
				}

			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public ArrayList<LinksetDB> getLinksetsInDegreeByDistribution(int distributionID, String linkType, double min,
			double max) {

		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(LinksetDB.COLLECTION_NAME);

			DBObject clause1 = new BasicDBObject(LinksetDB.DISTRIBUTION_TARGET, distributionID);
			BasicDBList and = new BasicDBList();
			and.add(clause1);
			DBObject clause2;
			DBObject clause3;

			if (min > 0) {
				clause2 = new BasicDBObject(linkType, new BasicDBObject("$gt", min));
				and.add(clause2);
			}
			if (max > 0) {
				clause3 = new BasicDBObject(linkType, new BasicDBObject("$lt", max));
				and.add(clause3);
			}

			DBObject query = new BasicDBObject("$and", and);
			DBCursor d = collection.find(query);

			while (d.hasNext()) {
				list.add(new LinksetDB(d.next()));
			}

			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void getLinksetsDegrees(DiagramData diagramData, String linkType, double min, double max) {

		HashMap<Integer, ArrayList<LinksetDB>> list = new HashMap<Integer, ArrayList<LinksetDB>>();
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(LinksetDB.COLLECTION_NAME);

			BasicDBList and = new BasicDBList();
			DBObject clause2;
			DBObject clause3;

			if (min > 0) {
				clause2 = new BasicDBObject(linkType, new BasicDBObject("$gt", min));
				and.add(clause2);
			}
			if (max > 0) {
				clause3 = new BasicDBObject(linkType, new BasicDBObject("$lt", max));
				and.add(clause3);
			}
			DBObject orderBy = new BasicDBObject(LinksetDB.DISTRIBUTION_TARGET, 1);
			DBObject query = new BasicDBObject("$and", and);

			DBCursor d = collection.find(query).sort(orderBy);

			int lastDistributionId = 0;
			ArrayList<LinksetDB> listOfLinksets = null;

			while (d.hasNext()) {
				LinksetDB linkset = new LinksetDB(d.next());

				if (diagramData.indegreeLinks.get(linkset.getDistributionTarget()) == null) {
					diagramData.indegreeLinks.put(linkset.getDistributionTarget(), new ArrayList<LinksetDB>());
					diagramData.indegreeLinks.get(linkset.getDistributionTarget()).add(linkset);
				} else {
					diagramData.indegreeLinks.get(linkset.getDistributionTarget()).add(linkset);
				}

				if (diagramData.outdegreeLinks.get(linkset.getDistributionSource()) == null) {
					diagramData.outdegreeLinks.put(linkset.getDistributionSource(), new ArrayList<LinksetDB>());
					diagramData.outdegreeLinks.get(linkset.getDistributionSource()).add(linkset);
				} else {
					diagramData.outdegreeLinks.get(linkset.getDistributionSource()).add(linkset);
				}

				diagramData.distributionsID.add(linkset.getDistributionSource());
				diagramData.distributionsID.add(linkset.getDistributionTarget());

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ArrayList<LinksetDB> getLinksetsOutDegreeByDistribution(int distributionID, String linkType, double min,
			double max) {
		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();
		try {

			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(LinksetDB.COLLECTION_NAME);

			DBObject clause1 = new BasicDBObject(LinksetDB.DISTRIBUTION_SOURCE, distributionID);

			BasicDBList and = new BasicDBList();
			and.add(clause1);
			DBObject clause2;
			DBObject clause3;

			if (min > 0) {
				clause2 = new BasicDBObject(linkType, new BasicDBObject("$gt", min));
				and.add(clause2);
			}
			if (max > 0) {
				clause3 = new BasicDBObject(linkType, new BasicDBObject("$lt", max));
				and.add(clause3);
			}

			DBObject query = new BasicDBObject("$and", and);
			DBCursor d = collection.find(query);
			while (d.hasNext()) {
				list.add(new LinksetDB(d.next()));
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
