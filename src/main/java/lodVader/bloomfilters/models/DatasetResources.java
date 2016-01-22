package lodVader.bloomfilters.models;

import lodVader.bloomfilters.GoogleBloomFilter;

// Class which holds BFs
public class DatasetResources {
	
	
	GoogleBloomFilter bfFilter = new GoogleBloomFilter(DatasetBF.bfSize, DatasetBF.bfFpp);
	int currentFilterSize = 0;

}
