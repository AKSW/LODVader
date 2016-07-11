package lodVader.bloomfilters.models;

import lodVader.utils.bloomfilter.BloomFilterCache;

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
	
	public int datasetID;

	public BloomFilterCache datasetLinks = new BloomFilterCache(bfSize, bfFpp);

	// counters for links between distribution and dataset
	public int datasetLinksCounter = 0;

	/**
	 * Query a resource against a dataset
	 * 
	 * @param query
	 * @return tru case the query element was found
	 */
	public boolean queryDataset(String query) {
		return datasetLinks.contain(query);
	}

	/**
	 * Add a new resource to a dataset
	 * 
	 * @param resource
	 */
	public void addResource(String resource) {
		datasetLinks.add(resource);
	}

	/**
	 * Sometimes JAVA GC takes time to empty the list of BF. Since this class is
	 * quite memory consuming, this method will empty all list of BFs
	 */
	public void emptyDatasetResources() {
		datasetLinks.empty();
	}

	/**
	 * 
	 */
	public void incrementDatasetCounter() {
		datasetLinksCounter ++;
	}

}
