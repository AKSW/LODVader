package lodVader.bloomfilters.models;

import lodVader.bloomfilters.GoogleBloomFilter;

/**
 * Class that holds temporary data about datasets resources
 * @author Ciro Baron Neto
 *
 */
public class DatasetResources {
	
	GoogleBloomFilter bfFilter = new GoogleBloomFilter(DatasetLinksContainer.bfSize, DatasetLinksContainer.bfFpp);
	
	int currentFilterSize = 0;

}
