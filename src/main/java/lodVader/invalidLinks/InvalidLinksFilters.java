package lodVader.invalidLinks;

import java.util.ArrayList;
import java.util.HashMap;

import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.gridFS.SuperBucket;

public class InvalidLinksFilters {

	HashMap<Integer, ArrayList<SuperBucket>> datasetSubjectFilters = new HashMap<Integer, ArrayList<SuperBucket>>();

	HashMap<Integer, ArrayList<SuperBucket>> datasetObjectFilters = new HashMap<Integer, ArrayList<SuperBucket>>();

	public void loadDatasetObjectFilter(int datasetID) {
		if(!datasetObjectFilters.containsKey(datasetID)){
			datasetObjectFilters.put(datasetID, new ObjectsBucket().getFiltersFromDataset(datasetID));
		}
	}
	public void loadDatasetSubjectFilter(int datasetID) {
		if(!datasetSubjectFilters.containsKey(datasetID)){
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
