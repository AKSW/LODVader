package lodVader.API.services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.LODVaderProperties;
import lodVader.API.core.APIMessage;
import lodVader.API.core.ServiceAPIOptions;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.LinksetQueries;
import lodVader.utils.Timer;

public class APIStatistics{
	
	public APIMessage getStatistics(){
		APIMessage apimessage = new APIMessage(); 
		
		JSONObject jsonMsg = new JSONObject();

		// get how many vocabs and datasets are in the database
		int datasets = new DatasetQueries().getDatasetsNotVocab().size();
		
		int vocabularies = new DatasetQueries().getDatasetsVocab().size(); 
		
		int triples = new DistributionQueries().getNumberOfTriples();
		
		NumberFormat formatter = new DecimalFormat("###,###,###,###");

		jsonMsg.put("numberOfDatasets", datasets);
		
		jsonMsg.put("numberOfVocabularies", formatter.format(vocabularies));		
		
		jsonMsg.put("numberOfTriples", formatter.format(triples));

		apimessage.addStatisticsMsg(jsonMsg);
		
		return apimessage;
	}
	
	
	public APIMessage listDistributions(int skip, int limit, int searchVocabularies, String search, 
			String searchSubject, String searchProperty, String searchObject){
		APIMessage apimessage = new APIMessage(); 	
		
		boolean hasResource = false;
		boolean resourceFound = false;
		
		if(((searchSubject != null) || 
			     (searchProperty != null) || 
			     (searchObject != null)))
		hasResource = true;
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();
		
		List<Set<Integer>> setOfSetsOfDistributions = new ArrayList<Set<Integer>> ();
		
		if(searchSubject!=null){
			HashSet<Integer> i = new HashSet<Integer>();
			for(DistributionDB n: new DistributionQueries().
					getDistributionsByResource(searchSubject, LODVaderProperties.TYPE_SUBJECT)){
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if(i.size()>0)
				resourceFound=true;
		}
		if(searchObject!=null){
			HashSet<Integer> i = new HashSet<Integer>();
			for(DistributionDB n: new DistributionQueries().
					getDistributionsByResource(searchObject, LODVaderProperties.TYPE_OBJECT)){
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if(i.size()>0)
				resourceFound=true;
		}
		if(searchProperty!=null){
			HashSet<Integer> i = new HashSet<Integer>();
			for(DistributionDB n: new DistributionQueries().
					getDistributionsByResource(searchProperty, LODVaderProperties.TYPE_PROPERTY)){
				i.add(n.getLODVaderID());
			}
			setOfSetsOfDistributions.add(i);
			if(i.size()>0)
				resourceFound=true;
		}
		
		
		List<Integer> in = new ArrayList<Integer>();
		
		
		if(setOfSetsOfDistributions.size()>0){
			Set<Integer> setCross = setOfSetsOfDistributions.get(0);
			for (int i = 1; i < setOfSetsOfDistributions.size(); i++) {
//				if(setOfSetsOfDistributions.get(i).size()>0)
					setCross.retainAll(setOfSetsOfDistributions.get(i));
			}   
			for (Integer v : setCross) {
				in.add(v);
			}
		}
		
		if(( !resourceFound &&
				hasResource ) || (hasResource && in.size() ==0)){
			JSONArray jsonObj = new JSONArray();
			msg.put("distributions", jsonArr);
			msg.put("totalDistributions",0);

			apimessage.addListMsg(msg); 
			return apimessage;
			
		}
		
		// search by name
		DistributionQueries dq = new DistributionQueries();
		ArrayList<DistributionDB> distributions = dq
		.getDistributions(skip, limit, searchVocabularies, search, in);
		
		
		for (DistributionDB d : distributions){
			JSONArray jsonObj = new JSONArray();
			jsonObj.put(d.getTopDatasetTitle());
//			jsonObj.put(new DatasetMongoDBObject(d.getDefaultDatasets().get(0)).getUri());
			jsonObj.put(d.getDownloadUrl());
			jsonObj.put(d.getStatus());
			jsonObj.put(d.getLastTimeStreamed());
			jsonArr.put(jsonObj);
		}
		
		msg.put("distributions", jsonArr);
		msg.put("totalDistributions",dq.getDistributionQuerySize);

		apimessage.addListMsg(msg); 
		
		return apimessage;
	}

	public APIMessage getTop(String distribution, int topN, String type){
		APIMessage apimessage = new APIMessage(); 
		NumberFormat formatterLinks = new DecimalFormat("###,###,###,###");
		DistributionDB dist= new DistributionDB(distribution);
		int distributionID = dist.getLODVaderID();
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();
		
		String collectionName = "" ;

		if(type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)){
			collectionName = AllPredicatesRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)){
			collectionName = OwlClassRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)){
			collectionName = RDFSubClassOfRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE)){
			collectionName = RDFTypeObjectRelationDB.COLLECTION_NAME;
		}
		
		List<GeneralRDFResourceRelationDB> list = new GeneralRDFResourceRelationDB().getTopNPredicates(collectionName, distributionID, topN);
		
		
		for (GeneralRDFResourceRelationDB d : list){
			JSONArray jsonObj = new JSONArray();
			
			if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES))
				jsonObj.put(new OwlClassDB(d.getPredicateID()).getUri());
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES))
				jsonObj.put(new RDFSubClassOfDB(d.getPredicateID()).getUri());
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE))
				jsonObj.put(new RDFTypeObjectDB(d.getPredicateID()).getUri());
			else
				jsonObj.put(new AllPredicatesDB(d.getPredicateID()).getUri());
			
			jsonObj.put(formatterLinks.format(d.getAmount()));
			
			jsonArr.put(jsonObj);
		}

		
		
		msg.put("distributions", jsonArr);
		msg.put("distributionTitle", dist.getTitle());
		msg.put("datasetTitle", dist.getTopDatasetTitle());
		msg.put("distributionTriples", dist.getTriples());
		msg.put("isVocabulary", dist.getIsVocabulary());
		msg.put("distributionDownloadURL", dist.getDownloadUrl());
		apimessage.addListMsg(msg); 
		
		return apimessage;
	}
	
	public APIMessage datasetDetails(String distribution, int topN, String type){
		APIMessage apimessage = new APIMessage(); 
		NumberFormat formatterLinks = new DecimalFormat("###,###,###,###");
		NumberFormat formatterDecimal = new DecimalFormat("#.####");
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();

		DistributionDB dist= new DistributionDB(distribution);
		
		// get how many vocabs and datasets are in the database
		ArrayList<LinksetDB> links = new LinksetQueries().getLinksetsByDistribution(distribution, topN, type);
		
		
		for (LinksetDB d : links){
			DistributionDB datasetDB = new DistributionDB(d.getDistributionTarget());
			JSONArray jsonObj = new JSONArray();
//			jsonObj.put(d.getDistributionTarget());
			jsonObj.put(datasetDB.getTitle());
			
			if(type.equals(ServiceAPIOptions.DATASET_TYPE_LINKS))
				jsonObj.put(formatterLinks.format(d.getLinks()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_STRENGTH))
				jsonObj.put(formatterDecimal.format(d.getStrength()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES))
				jsonObj.put(formatterDecimal.format(d.getOwlClassSimilarity()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES))
				jsonObj.put(formatterDecimal.format(d.getRdfSubClassSimilarity()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE))
				jsonObj.put(formatterDecimal.format(d.getRdfTypeSimilarity()));
			else
				jsonObj.put(formatterDecimal.format(d.getPredicateSimilarity()));
			
			jsonObj.put(datasetDB.getDownloadUrl());
			jsonObj.put(datasetDB.getIsVocabulary());
			jsonObj.put(datasetDB.getLODVaderID());
			
			jsonArr.put(jsonObj);
		}

		msg.put("distributions", jsonArr);
		msg.put("distributionTitle", dist.getTitle());
		msg.put("distributionID", dist.getLODVaderID());
		msg.put("datasetTitle", dist.getTopDatasetTitle());
		apimessage.addListMsg(msg); 
		
		return apimessage;
	}	
	
//	@Test
//	public void compareDatasets(){
		public APIMessage compareDatasets(int dataset1URL, int dataset2URL, String type){
		
//		String dataset1URL = "https://raw.githubusercontent.com/AKSW/n3-collection/master/Reuters-128.nt";
//		String dataset2URL = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#";
//		String type = ServiceAPIOptions.DATASET_TYPE_PREDICATES;
		
		
		APIMessage apimessage = new APIMessage(); 
		
		DistributionDB distribution1 = new DistributionDB(dataset1URL);
		DistributionDB distribution2 = new DistributionDB(dataset2URL);
		
		
		HashSet<String> values1 = new HashSet<String>();
		HashSet<String> values2 = new HashSet<String>();
		HashSet<Integer> intersection = new HashSet<Integer>();
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();
		
		String collectionName = "";
		
		if(type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)){
			values1 = new AllPredicatesRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new AllPredicatesRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = AllPredicatesRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)){
			values1 = new OwlClassRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new OwlClassRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = OwlClassRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)){
			values1 = new RDFSubClassOfRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new RDFSubClassOfRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = RDFSubClassOfRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE)){
			values1 = new RDFTypeObjectRelationDB().getSetOfPredicates(distribution1.getLODVaderID());
			values2 = new RDFTypeObjectRelationDB().getSetOfPredicates(distribution2.getLODVaderID());
			collectionName = RDFTypeObjectRelationDB.COLLECTION_NAME;
		}	
	
		intersection = makeIntersecion(values1, values2);

		Set<GeneralRDFResourceRelationDB> h = new GeneralRDFResourceRelationDB().getPredicatesIn
				(collectionName, intersection, distribution1.getLODVaderID(), distribution2.getLODVaderID());
		
	
		// group by predicate value
		HashMap<String, HashMap<Integer, Integer>> m = new HashMap<String, HashMap<Integer, Integer>>();
		for(GeneralRDFResourceRelationDB value: h){
//			m.put(value.getPredicateID(), value)
		
			int v1 = 0;
			int v2 = 0;
			
			for(GeneralRDFResourceRelationDB value2: h){
				if(value2.getDistributionID() == distribution1.getLODVaderID() && value.getPredicateID()  == value2.getPredicateID())
					v1=value2.getAmount();	
			}
			for(GeneralRDFResourceRelationDB value2: h){
				if(value2.getDistributionID() == distribution2.getLODVaderID() &&  value.getPredicateID()  == value2.getPredicateID())
					v2=value2.getAmount();	
			}	
			
			if(type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				AllPredicatesDB a = new AllPredicatesDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				OwlClassDB a = new OwlClassDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				RDFSubClassOfDB a = new RDFSubClassOfDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}			
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				RDFTypeObjectDB a = new RDFTypeObjectDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}		
			
		}
		
		
		for (String d : m.keySet()){
//			DistributionDB datasetDB = new DistributionDB(d.getDistributionTarget());
			JSONArray jsonObj = new JSONArray();
//			jsonObj.put(d.getDistributionTarget());
			jsonObj.put(d);
			jsonObj.put(m.get(d));
			
			
			
			jsonArr.put(jsonObj);
		}

		msg.put("similarityTableData", jsonArr);
		apimessage.addListMsg(msg); 
//		System.out.println(apimessage.toJSONString());
		
		return apimessage;
	}

	private HashSet<Integer> makeIntersecion(HashSet<String> values1, HashSet<String> values2){
		HashSet<Integer> intersection = new HashSet<Integer>();
	      for (String i : values1) {
	            if(values2.contains(i)) {
	                intersection.add(Integer.valueOf(i));
	            }
	        }
	      
	      return intersection;
	}
	
	
}
