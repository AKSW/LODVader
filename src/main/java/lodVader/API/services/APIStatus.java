package lodVader.API.services;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import lodVader.LODVaderProperties;
import lodVader.API.core.API;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.LinksetQueries;

public class APIStatus extends API {

//	APIStatusMongoDBObject apiStatus = null;
	
	ArrayList<DistributionDB> distributions;
	
	final static Logger logger = Logger.getLogger(APIStatus.class);

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public APIStatus(String url) {
		
//		logger.debug("APIStatus initialized. ");
		
		
//		apiStatus = new APIStatusMongoDBObject(url);
		distributions=new DistributionQueries().getDistributionsByTopDatasetID(url);
		
		apiMessage.setCoreMsgSuccess();
//		apiMessage.setParserMsg("Dataset status:  " + apiStatus.getMessage());
		
//		logger.debug("APIStatus number of distributions found: "+distributions.size());
	
		for (DistributionDB distribution : distributions) {
			
			JSONObject datasetMessage = new JSONObject();
			
			ArrayList<DatasetDB> d = distribution.getDefaultDatasetsAsResources();
			
			Iterator<DatasetDB> i = d.iterator();
			
			ArrayList<String> parentNames = new ArrayList<String>();
			while(i.hasNext()){
				parentNames.add(i.next().getUri());
			}
			
			if(distribution.getStatus().equals(DistributionDB.STATUS_ERROR)){
				datasetMessage.put(DistributionDB.LAST_MSG, distribution.getLastMsg());
			}
			datasetMessage.put(DistributionDB.DOWNLOAD_URL, distribution.getDownloadUrl());
			datasetMessage.put(DistributionDB.RESOURCE_URI, distribution.getResourceUri()); 
			datasetMessage.put(DistributionDB.DEFAULT_DATASETS, parentNames); 
			datasetMessage.put(DistributionDB.STATUS, distribution.getStatus());
			datasetMessage.put(DistributionDB.TITLE, distribution.getTitle());
			datasetMessage.put(DistributionDB.DOWNLOAD_URL, distribution.getDownloadUrl());
			datasetMessage.put(DistributionDB.LAST_MSG, distribution.getLastMsg());
			datasetMessage.put(DistributionDB.TRIPLES, distribution.getTriples());
			datasetMessage.put(DistributionDB.LAST_TIME_STREAMED, distribution.getLastTimeStreamed());
			
			
			// indegrees
			ArrayList<LinksetDB> indegrees = new  LinksetQueries().getLinksetsInDegreeByDistribution(distribution.getLODVaderID(), LinksetDB.LINK_NUMBER_LINKS,50,-1);
			int indegreeCount = 0;
			JSONArray inegreeArray = new JSONArray();
			
			for(LinksetDB linkset : indegrees){
				JSONObject indegreeTmpObj = new JSONObject();
				
				// check whether is vocabulary
				DatasetDB dataset = new DatasetDB(linkset.getDatasetSource()); 
				if(dataset.getIsVocabulary()){
					indegreeTmpObj.put("isVocabulary", true);
				}
				else {
					dataset = new DatasetDB(linkset.getDatasetTarget()); 
					if(dataset.getIsVocabulary()){
						indegreeTmpObj.put("isVocabulary", true);
					}
				}
				
				indegreeTmpObj.put("links",linkset.getLinks());
				indegreeTmpObj.put("sourceDataset", new DatasetDB(linkset.getDatasetSource()).getUri());
				indegreeTmpObj.put("targetDataset",  new DatasetDB(linkset.getDatasetTarget()).getUri());
				indegreeTmpObj.put("sourceDistribution", new DistributionDB(linkset.getDistributionSource()).getUri());
				indegreeTmpObj.put("targetDistribution", new DistributionDB(linkset.getDistributionTarget()).getUri());
				
				inegreeArray.put(indegreeTmpObj); 
			}
			
			datasetMessage.put("indegree", inegreeArray);
			
//			datasetMessage.put("indegreeDatasetCount", indegrees.size());
//			datasetMessage.put("indegreeLinksCount", indegreeCount);
			
			// outdegrees
			ArrayList<LinksetDB> outdegrees = new LinksetQueries().getLinksetsOutDegreeByDistribution(distribution.getLODVaderID(), LinksetDB.LINK_NUMBER_LINKS,50,-1);
			int outdegreeCount = 0;
			JSONArray outdegreeArray = new JSONArray();
			for(LinksetDB linkset : outdegrees){
				JSONObject outdegreeTmpObj = new JSONObject();
				
				// check whether is vocabulary
				DatasetDB dataset = new DatasetDB(linkset.getDatasetSource()); 
				if(dataset.getIsVocabulary()){
					outdegreeTmpObj.put("isVocabulary", true);
				}
				else {
					dataset = new DatasetDB(linkset.getDatasetTarget()); 
					if(dataset.getIsVocabulary()){
						outdegreeTmpObj.put("isVocabulary", true);
					}
				}
				
				outdegreeTmpObj.put("links",linkset.getLinks());
				outdegreeTmpObj.put("sourceDataset", new DatasetDB(linkset.getDatasetSource()).getUri());
				outdegreeTmpObj.put("targetDataset",new DatasetDB(linkset.getDatasetTarget()).getUri());
				outdegreeTmpObj.put("sourceDistribution",new DistributionDB(linkset.getDistributionSource()).getUri());
				outdegreeTmpObj.put("targetDistribution",new DistributionDB(linkset.getDistributionTarget()).getUri());
				
				outdegreeArray.put(outdegreeTmpObj); 
			}
			
			datasetMessage.put("outdegree", outdegreeArray);
			
//			datasetMessage.put("outdegreeDatasetCount", outdegrees.size());
//			datasetMessage.put("outdegreeLinksCount", outdegreeCount);	
			
//			logger.debug("APIStatus message: "+ datasetMessage.toString(4));
			
			apiMessage.addDistributionMsg(datasetMessage);
		}
		
	}

}
