/**
 * 
 */
package lodVader.utils.bloomfilter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory; 

/**
 * @author Ciro Baron Neto
 * 
 *         Some services to read and write Bloom Filters
 * 
 *         Jul 7, 2016
 */
public class BloomFilterService {

	/**
	 * Write the bloom filter to a file
	 * 
	 * @param filePath
	 * @return
	 */
	public boolean saveFilterToFile(String filePath, BloomFilterI bloomFilter) {

		try {
			bloomFilter.writeTo(new FileOutputStream(new File(filePath))); 

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Load a filter from a file
	 * 
	 * @param path
	 * @param distributionName
	 * @return
	 */
	public BloomFilterI loadFilter(String path) {

		BloomFilterI bloomFilter = BloomFilterFactory.newBloomFilter();

		try {
			bloomFilter.readFrom(new FileInputStream(new File(path)));
			return bloomFilter;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
