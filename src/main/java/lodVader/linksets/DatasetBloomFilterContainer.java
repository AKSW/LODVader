package lodVader.linksets;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.utils.bloomfilter.BloomFilterCache;

public class DatasetBloomFilterContainer {

	int maxBFSize = 10000;

	double fpp = 0.00001;

	public DatasetBloomFilterContainer(int datasetId) {
		this.dataset = new DatasetDB(datasetId);
	}

	public int datasetID;

	public DatasetDB dataset;

	
	private BloomFilterCache filterSubjects = new BloomFilterCache(maxBFSize, fpp);

	private BloomFilterCache filterObjects = new BloomFilterCache(maxBFSize, fpp);

	public boolean querySubject(String resource) {
		return filterSubjects.contain(resource);
	}

	public boolean queryObject(String resource) {
		return filterObjects.contain(resource);
	}

	public void addSubject(String resource) {
		filterSubjects.add(resource);
	}

	public void addObject(String resource) {
		filterObjects.add(resource);
	}

}
