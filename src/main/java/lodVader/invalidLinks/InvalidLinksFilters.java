package lodVader.invalidLinks;

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.gridFS.SuperBucket;
import lodVader.threads.ProcessNSFromTuple;

public class InvalidLinksFilters {

	final static Logger logger = LoggerFactory.getLogger(InvalidLinksFilters.class);

	HashMap<Integer, ArrayList<SuperBucket>> datasetSubjectFilters = new HashMap<Integer, ArrayList<SuperBucket>>();

	HashMap<Integer, ArrayList<SuperBucket>> datasetObjectFilters = new HashMap<Integer, ArrayList<SuperBucket>>();

	public void loadDatasetObjectFilter(int datasetID) {
		if(!datasetObjectFilters.containsKey(datasetID)){
			datasetObjectFilters.put(datasetID, new ObjectsBucket().getFiltersFromDataset(datasetID));
		}
	}
	public void loadDatasetSubjectFilter(int datasetID) {
		if(!datasetSubjectFilters.containsKey(datasetID)){
			logger.info("Loading dataset"+new DatasetDB(datasetID).getTitle()+" to filter...");
			datasetSubjectFilters.put(datasetID, new SubjectsBucket().getFiltersFromDataset(datasetID));
		}
	}

	public boolean queryDatasetSubject(String resource, int datasetID){
		for(SuperBucket buket: datasetSubjectFilters.get(datasetID)){
			if(buket.filter.compare(resource))
				return true;
		}
		return false;
	}
	
	public boolean queryDatasetObject(String resource, int datasetID){
		for(SuperBucket buket: datasetObjectFilters.get(datasetID)){
			if(buket.filter.compare(resource))
				return true;
		}
		return false;
	}
}
