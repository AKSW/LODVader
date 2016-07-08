/**
 * 
 */
package lodVader.utils.bloomfilter;

import java.util.ArrayList;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;

/**
 * @author Ciro Baron Neto
 * 
 * Jul 7, 2016
 */
public class BloomFilterCache {
	

	// default bloom filter size
	private int initialSize = 100000;

	// default bloom filter fpp
	private double fpp = 0.001;

	private ArrayList<BloomFilterI> caches = new ArrayList<BloomFilterI>();

	
	public BloomFilterCache(int initialSize, double bfFpp) {
		this.initialSize = initialSize;
		this.fpp = bfFpp;
		BloomFilterI bf = BloomFilterFactory.newBloomFilter();
		bf.create(initialSize, bfFpp);
		caches.add(bf);
	}

	/**
	 * Query 
	 * 
	 * @param query
	 * @return true case the query element was found
	 */
	public boolean contain(String query) {
		for (BloomFilterI cache : caches) {
			if (cache.compare(query)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Add a new element
	 * 
	 * @param resource
	 * @param datasetID
	 */
	public void add(String resource) {
		// do not overload the BFs with a value higher than bfSize
		for (BloomFilterI cache : caches) {
			if (cache.getNumberOfElements() < initialSize) {
				cache.add(resource);
				return;
			}
		}

		// case all caches are full, create a new one and add the
		// list
		BloomFilterI cache = BloomFilterFactory.newBloomFilter(); 
		cache.create(initialSize, fpp);
		cache.add(resource);
		caches.add(cache); 
	}

	/**
	 * Sometimes JAVA GC takes time to empty the list of BF. Since this class is
	 * quite memory consuming, this method will empty all list of BFs
	 */
	public void empty() {
		caches = null;
	}
	
}
