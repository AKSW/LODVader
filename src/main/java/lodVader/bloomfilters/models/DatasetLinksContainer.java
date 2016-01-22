package lodVader.bloomfilters.models;

import java.util.ArrayList;
import java.util.HashMap;

import lodVader.bloomfilters.GoogleBloomFilter;

/**
 * This class holds temporary data of resources (subject or object) described by
 * datasets. Usually it's used to create links between Distributions and
 * Datasets
 * 
 * @author Ciro Baron Neto
 *
 */
public class DatasetLinksContainer {

	// default bloom filter size
	static int bfSize = 100000;

	// default bloom filter fpp
	static double bfFpp = 0.001;

	private ArrayList<DatasetResources> datasetLinks = new ArrayList<DatasetResources>();

	// counters for links between distribution and dataset
	public int datasetLinksCounter = 0;
	
	int distributionID = 0;
	
	int datasetID = 0;
	
	public DatasetLinksContainer(int distributionID, int datasetID) {
		this.datasetID = datasetID;
		this.distributionID = distributionID;
	}

//	/**
//	 * Create a list of bloom filters for a dataset
//	 * @param datasetID
//	 */
//	public void loadDataset() {
//
//		if (datasetLinks == null)
//			datasetLinks = new  ArrayList<DatasetResources>();
//
//		// start bloom filter to count links between distribution and dataset
//		ArrayList<DatasetResources> list = datasetLinks.get(datasetID);
//		if (list == null) {
//			list = new ArrayList<DatasetResources>();
//			list.add(new DatasetResources());
//			datasetLinks.put(datasetID, list);
//		}
//	}

	/**
	 * Query a resource against a dataset
	 * 
	 * @param query
	 * @param datasetID
	 * @return tru case the query element was found
	 */
	public boolean queryDataset(String query) {
		for (DatasetResources datasetResources : datasetLinks) {
			if (datasetResources.bfFilter.compare(query)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Add a new resource to a dataset
	 * 
	 * @param resource
	 * @param datasetID
	 */
	public void addResource(String resource) {
		// do not overload the BFs with a value higher than bfSize
		for (DatasetResources datasetResources : datasetLinks) {
			if (datasetResources.currentFilterSize < bfSize) {
				datasetResources.bfFilter.add(resource);
				datasetResources.currentFilterSize++;
				return;
			}
		}

		// case all datasetResources are full, create a new one and add the
		// resource
		DatasetResources datasetResources = new DatasetResources();
		datasetResources.bfFilter.add(resource);
		datasetResources.currentFilterSize++;
		datasetLinks.add(new DatasetResources());
	}

	/**
	 * Sometimes JAVA GC takes time to empty the list of BF. Since this class is
	 * quite memory consuming, this method will empty all list of BFs
	 */
	public void emptyDatasetResources() {
		datasetLinks = null;
	}

	/**
	 * Increment the number of discovered links of a dataset
	 * @param datasetID
	 */
	public void incrementDatasetCounter() {
		datasetLinksCounter++;
	}
}
