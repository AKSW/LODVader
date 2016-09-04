/**
 * 
 */
package lodVader.bloomfilters.impl;

import lodVader.bloomfilters.BloomFilterI;

/**
 * @author Ciro Baron Neto
 * 
 * Jul 7, 2016
 */
public class BloomFilterFactory {
	
	public static BloomFilterI newBloomFilter(){ 
		return new BloomFilterGoogleImpl();
//		return new BloomFilterOrestesImpl();
	}

}
