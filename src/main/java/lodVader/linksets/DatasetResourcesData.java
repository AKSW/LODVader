package lodVader.linksets;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.collections.DatasetDB;

public class DatasetResourcesData {
	
	public DatasetResourcesData(int datasetId) {
		this.dataset  = new DatasetDB(datasetId);
		int size = this.dataset.getDatasetSize();
		if(size<100000)
			size = 100000;
		filterObjects = new GoogleBloomFilter(size, 0.0000001);
		filterSubjects = new GoogleBloomFilter(size, 0.0000001);
	}
	
	public int datasetID;
	
	public int datasetSize;

	public DatasetDB dataset;

	private GoogleBloomFilter filterSubjects;
	 
	private GoogleBloomFilter filterObjects;
	
	
	public boolean querySubject(String resource) {
		return filterSubjects.compare(resource);
	}

	public boolean queryObject(String resource) {
		return filterObjects.compare(resource);
	}
	
	public void addSubject(String s){
		filterSubjects.add(s);
	}
	public void addObject(String s){
		filterObjects.add(s);
	}
	
	
}
