package lodVader.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.LODVaderProperties;
import lodVader.API.core.ServiceAPIOptions;
import lodVader.API.diagram.DiagramData;
import lodVader.API.server.CreateD3JSONFormat;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;

public class LinksetQueries {

	public ArrayList<LinksetDB> getLinksets() {

		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new LinksetDB(instance.get(DBSuperClass.URI)
						.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public ArrayList<LinksetDB> getLinksetsWithLinks() {

		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			DBObject query = new BasicDBObject(LinksetDB.LINK_NUMBER_LINKS,
					new BasicDBObject("$gt", 50));
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new LinksetDB(instance.get(DBSuperClass.URI)
						.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// @Test
	// public void getLinksetsGroupByDatasets() {
	public ArrayList<LinksetDB> getLinksetsGroupByDatasets() {
		AggregationOutput output;
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);

			// Now the $group operation
			DBObject groupFields = new BasicDBObject("_id", new BasicDBObject(
					"objectsDatasetTarget", "$objectsDatasetTarget").append(
					"subjectsDatasetTarget", "$subjectsDatasetTarget"));
			groupFields.put("id", new BasicDBObject("$first", "$_id"));

			DBObject group = new BasicDBObject("$group", groupFields);

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(group);
			output = collection.aggregate(pipeline);

			// for (DBObject result : output.results()) {
			// System.out.println(result);
			// }
			ArrayList<LinksetDB> linksetList = new ArrayList<LinksetDB>();

			for (DBObject result : output.results()) {
				linksetList.add(new LinksetDB(result.get("id")
						.toString()));
			}

			return linksetList;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public ArrayList<LinksetDB> getLinksetsGroupByDistributions() {
		try {
			ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new LinksetDB(instance.get(DBSuperClass.URI)
						.toString()));
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public ArrayList<LinksetDB> getLinksetsByDistribution(
			String distribution, int topValue, String type) {
		
		DistributionDB distributionDB = new DistributionDB(distribution);
//		distribution = distributionDB.getDynLodID();

		try {
			ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);

//			ArrayList<BasicDBObject> a = new ArrayList<BasicDBObject>();
			BasicDBObject a = new BasicDBObject(
					LinksetDB.DISTRIBUTION_SOURCE, distributionDB.getLODVaderID());
//			BasicDBObject b = new BasicDBObject(
//					LinksetDB.DISTRIBUTION_TARGET, distributionDB.getDynLodID());

//			BasicDBList params = new BasicDBList();
//			params.add(a);
//			params.add(b);
			
//			System.out.println(distributionDB.getDynLodID());

//			BasicDBObject or = new BasicDBObject(new BasicDBObject("$or", params));
			
			BasicDBObject sort;
			if(type.equals(ServiceAPIOptions.DATASET_TYPE_LINKS))
				sort = new BasicDBObject(LinksetDB.LINK_NUMBER_LINKS,-1);
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_STRENGTH))
				sort = new BasicDBObject(LinksetDB.LINK_STRENGHT,-1);
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES))
				sort = new BasicDBObject(LinksetDB.OWL_CLASS_SIMILARITY,-1);
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES))
				sort = new BasicDBObject(LinksetDB.RDF_SUBCLASS_SIMILARITY,-1);
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE))
				sort = new BasicDBObject(LinksetDB.RDF_TYPE_SIMILARITY,-1);
			else
				sort = new BasicDBObject(LinksetDB.PREDICATE_SIMILARITY,-1);				
			
			DBCursor instances = collection.find(a)
			.sort(sort).limit(topValue);
			
//			System.out.println(instances.count());

			for (DBObject instance : instances) {
				list.add(new LinksetDB(instance.get(DBSuperClass.URI)
						.toString()));
			}

			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public ArrayList<LinksetDB> getLinksetsInDegreeByDistribution(
			int id, String linkType, double min, double max) {
		
		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetDB.DISTRIBUTION_TARGET, id);  
			BasicDBList and = new BasicDBList();
			and.add(clause1);
			DBObject clause2;
			DBObject clause3;
			
			if(min>0){
			clause2 = new BasicDBObject(linkType,
					new BasicDBObject("$gt", min)); 
			and.add(clause2);
			}
			if(max>0){
			clause3 = new BasicDBObject(linkType,
					new BasicDBObject("$lt", max)); 
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
	
	public void getLinksetsDegrees(
			DiagramData diagramData, String linkType, double min, double max) {
		
		HashMap<Integer, ArrayList<LinksetDB>> list = new HashMap<Integer, ArrayList<LinksetDB>>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			
			BasicDBList and = new BasicDBList();
			DBObject clause2;
			DBObject clause3;
			
			if(min>0){
			clause2 = new BasicDBObject(linkType,
					new BasicDBObject("$gt", min)); 
			and.add(clause2);
			}
			if(max>0){
			clause3 = new BasicDBObject(linkType,
					new BasicDBObject("$lt", max)); 
			and.add(clause3);
			}
			DBObject orderBy = new BasicDBObject(LinksetDB.DISTRIBUTION_TARGET, 1);
			DBObject query = new BasicDBObject("$and", and);
			
			System.out.println(query);
			
			DBCursor d = collection.find(query).sort(orderBy);

			int lastDistributionId = 0;
			ArrayList<LinksetDB> listOfLinksets = null;
			
			while (d.hasNext()) {
				LinksetDB linkset = new LinksetDB(d.next());
				
				if(diagramData.indegreeLinks.get(linkset.getDistributionTarget()) == null){
					diagramData.indegreeLinks.put(linkset.getDistributionTarget(), new ArrayList<LinksetDB>());
					diagramData.indegreeLinks.get(linkset.getDistributionTarget()).add(linkset);
				}
				else{
					diagramData.indegreeLinks.get(linkset.getDistributionTarget()).add(linkset);					
				}
				
				if(diagramData.outdegreeLinks.get(linkset.getDistributionSource()) == null){
					diagramData.outdegreeLinks.put(linkset.getDistributionSource(), new ArrayList<LinksetDB>());
					diagramData.outdegreeLinks.get(linkset.getDistributionSource()).add(linkset);
				}
				else{
					diagramData.outdegreeLinks.get(linkset.getDistributionSource()).add(linkset);					
				}
			

				diagramData.distributionsID.add(linkset.getDistributionSource());
				diagramData.distributionsID.add(linkset.getDistributionTarget());
					
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	public ArrayList<LinksetDB> getLinksetsOutDegreeByDistribution(
			int id, String linkType, double min, double max) {
		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetDB.DISTRIBUTION_SOURCE, id);  
			
			BasicDBList and = new BasicDBList();
			and.add(clause1);
			DBObject clause2;
			DBObject clause3;
			
			if(min>0){
			clause2 = new BasicDBObject(linkType,
					new BasicDBObject("$gt", min)); 
			and.add(clause2);			
			}
			if(max>0){
			clause3 = new BasicDBObject(linkType,
					new BasicDBObject("$lt", max)); 
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
	

	public ArrayList<LinksetDB> getLinksetsInDegreeByDataset(
			String url) {
		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetDB.DATASET_TARGET,  new BasicDBObject("$regex", url+".*"));  
			DBObject clause2 = new BasicDBObject(LinksetDB.LINK_NUMBER_LINKS,
					new BasicDBObject("$gt", 50));   

			BasicDBList or = new BasicDBList();
			or.add(clause1);
			or.add(clause2);
			DBObject query = new BasicDBObject("$and", or);
			DBCursor d = collection.find(query);

			while (d.hasNext()) {
				list.add(new LinksetDB(d.next().get(DBSuperClass.URI)
						.toString()));
			}

			return list;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public ArrayList<LinksetDB> getLinksetsOutDegreeByDataset(
			String url) {
		ArrayList<LinksetDB> list = new ArrayList<LinksetDB>();
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetDB.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetDB.DATASET_SOURCE, new BasicDBObject("$regex", url+".*"));  
			DBObject clause2 = new BasicDBObject(LinksetDB.LINK_NUMBER_LINKS,
					new BasicDBObject("$gt", 50));   

			BasicDBList and = new BasicDBList();
			and.add(clause1);
			and.add(clause2);
			DBObject query = new BasicDBObject("$and", and);
			
			DBCursor d = collection.find(query);

			while (d.hasNext()) {
				list.add(new LinksetDB(d.next().get(DBSuperClass.URI)
						.toString()));
			}

			return list;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean isOnLinksetList(String downloadURLObject,
			String downloladURLSubject) {

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				LinksetDB.COLLECTION_NAME);
		BasicDBObject query = new BasicDBObject(
				LinksetDB.DISTRIBUTION_TARGET,
				downloladURLSubject);
		query.append(LinksetDB.DISTRIBUTION_SOURCE,
				downloadURLObject);

		DBCursor d = collection.find(query);

		if (d.hasNext()) {
			return true;
		}

		return false;
	}
	
	public boolean checkIfDatasetExists(String datasetURL) {

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				LinksetDB.COLLECTION_NAME);
		BasicDBObject clause1 = new BasicDBObject(
				LinksetDB.DATASET_TARGET,
				new BasicDBObject("$regex", datasetURL+".*"));
		BasicDBObject clause2 = new BasicDBObject(LinksetDB.DATASET_SOURCE,
				new BasicDBObject("$regex", datasetURL+".*"));
		
		BasicDBList or = new BasicDBList();
		or.add(clause1);
		or.add(clause2);
		DBObject query = new BasicDBObject("$or", or);
		
		DBCursor d = collection.find(query).limit(1);
		

		if (d.hasNext()) {
			return true;
		}

		return false;
	}
	
	public boolean checkIfDistributionExists(String distributionURL) {

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				LinksetDB.COLLECTION_NAME);
		BasicDBObject clause1 = new BasicDBObject(
				LinksetDB.DISTRIBUTION_TARGET,
				new BasicDBObject("$regex", distributionURL+".*"));
		BasicDBObject clause2 = new BasicDBObject(LinksetDB.DISTRIBUTION_SOURCE,
				 new BasicDBObject("$regex", distributionURL+".*"));
		
		BasicDBList and = new BasicDBList();
		and.add(clause1);
		and.add(clause2);
		DBObject query = new BasicDBObject("$or", and);
		
		DBCursor d = collection.find(query).limit(1);

		if (d.hasNext()) {
			return true;
		}

		return false;
	}
	
	

}
