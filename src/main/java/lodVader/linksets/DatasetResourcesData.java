package lodVader.linksets;

import java.util.ArrayList;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.collections.DatasetDB;

public class DatasetResourcesData {

	int subjectsIncluded = 0;

	int objectsIncluded = 0;

	int maxBFSize = 10000;

	double fpp = 0.00001;

	public DatasetResourcesData(int datasetId) {
		// this.dataset = new DatasetDB(datasetId);
		// int size = this.dataset.getDatasetTriples();
		// if(size<10000)
		// size = 10000;
		// filterObjects = new GoogleBloomFilter(size, 0.00001);
		// filterSubjects = new GoogleBloomFilter(size, 0.00001);

		this.dataset = new DatasetDB(datasetId);
//		filterObjects = new GoogleBloomFilter(maxBFSize, 0.00001);
//		filterSubjects = new GoogleBloomFilter(maxBFSize, 0.00001);

	}

	public int datasetID;

	public int datasetSize;

	public DatasetDB dataset;

	private ArrayList<GoogleBloomFilter> filterSubjects = new ArrayList<GoogleBloomFilter>();

	private ArrayList<GoogleBloomFilter> filterObjects= new ArrayList<GoogleBloomFilter>();;

	public boolean querySubject(String resource) {

		for (GoogleBloomFilter filter : filterSubjects)
			if (filter.compare(resource))
				return true;

		return false;
	}

	public boolean queryObject(String resource) {

		for (GoogleBloomFilter filter : filterObjects)
			if (filter.compare(resource))
				return true;

		return false;
	}

	public void addSubject(String s) {
		if (subjectsIncluded % maxBFSize == 0) {
			filterSubjects.add(new GoogleBloomFilter(maxBFSize, fpp));
		}
		subjectsIncluded++;
		filterSubjects.get(filterSubjects.size()-1).add(s);
	}

	public void addObject(String s) {
		if (objectsIncluded % maxBFSize == 0) {
			filterObjects.add(new GoogleBloomFilter(maxBFSize, fpp));
		}
		objectsIncluded++;
		filterObjects.get(filterObjects.size()-1).add(s);
	}

}
