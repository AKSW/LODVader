package lodVader.invalidLinks;

import java.util.ArrayList;

import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.gridFS.SuperBucket;

public class InvalidLinksFilters {

	ArrayList<SuperBucket> datasetSubjectFilters = null;

	ArrayList<SuperBucket> datasetObjectFilters = null;

	public void loadDatasetObjectFilter(int datasetID) {
		datasetObjectFilters = new ObjectsBucket().getFiltersFromDataset(datasetID);
	}
	public void loadDatasetSubjectFilter(int datasetID) {
		datasetSubjectFilters = new SubjectsBucket().getFiltersFromDataset(datasetID);
	}

	public boolean queryDatasetSubject(String resource){
		for(SuperBucket buket: datasetSubjectFilters){
			if(buket.filter.compare(resource))
				return true;
		}
		return false;
	}
	
	public boolean queryDatasetObject(String resource){
		for(SuperBucket buket: datasetObjectFilters){
			if(buket.filter.compare(resource))
				return true;
		}
		return false;
	}
}
