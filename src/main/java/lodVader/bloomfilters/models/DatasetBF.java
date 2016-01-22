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
public class DatasetBF {

	// default bloom filter size
	static int bfSize = 100000;

	// default bloom filter fpp
	static double bfFpp = 0.0001;

	private static HashMap<Integer, ArrayList<DatasetResources>> datasetLinks = new HashMap<Integer, ArrayList<DatasetResources>>();

	// counters for links between distribution and dataset
	public static HashMap<Integer, Integer> datasetLinksCounter = new HashMap<Integer, Integer>();

	/**
	 * create a list of bloom filters for a dataset
	 * 
	 * @param datasetID
	 */
	public static void loadDataset(int datasetID) {

		if (datasetLinks == null)
			datasetLinks = new HashMap<Integer, ArrayList<DatasetResources>>();

		// start bloom filter to count links between distribution and dataset
		ArrayList<DatasetResources> list = datasetLinks.get(datasetID);
		if (list == null) {
			list = new ArrayList<DatasetResources>();
			list.add(new DatasetResources());
			datasetLinks.put(datasetID, list);
		}
	}

	/**
	 * Query a resource against a dataset
	 * 
	 * @param query
	 * @param datasetID
	 * @return tru case the query element was found
	 */
	public static boolean queryDataset(String query, int datasetID) {
		ArrayList<DatasetResources> list = datasetLinks.get(datasetID);
		for (DatasetResources datasetResources : list) {
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
	public static void addResource(String resource, int datasetID) {
		ArrayList<DatasetResources> list = datasetLinks.get(datasetID);

		// do not overload the BFs with a value higher than bfSize
		for (DatasetResources datasetResources : list) {
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
		list.add(new DatasetResources());
	}

	/**
	 * Sometimes JAVA GC takes time to empty the list of BF. Since this class is
	 * quite memory consuming, this method will empty all list of BFs
	 */
	public static void emptyDatasetResources() {
		datasetLinks = null;
	}

	/**
	 * Increment the number of discovered links of a dataset
	 * @param datasetID
	 */
	public static void incrementDatasetCounter(int datasetID) {
		Integer counter = datasetLinksCounter.get(datasetID);
		if(counter == null)
			datasetLinksCounter.put(datasetID,0);
		else
			datasetLinksCounter.put(datasetID, counter++);
	}

}
