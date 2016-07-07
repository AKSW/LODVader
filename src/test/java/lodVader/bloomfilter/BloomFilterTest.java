/**
 * 
 */
package lodVader.bloomfilter;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.utils.bloomfilter.BloomFilterCache;
import lodVader.utils.bloomfilter.BloomFilterService;

/**
 * @author Ciro Baron Neto
 * 
 * Jul 7, 2016
 */
public class BloomFilterTest {
	
	// create a new BF
	@Test
	public void newBloomFilter(){
		Assert.assertNotNull(BloomFilterFactory.newBloomFilter().create(10000, 0.001));
	}
	
	@Test
	public void addAndCompare(){
		BloomFilterI bloomFilter = BloomFilterFactory.newBloomFilter();
		bloomFilter.create(100, 0.001);
		bloomFilter.add("Element1");
		
		Assert.assertTrue(bloomFilter.compare("Element1"));
	}
	
	@Test 
	public void saveAndLoad(){
		BloomFilterI bloomFilter = BloomFilterFactory.newBloomFilter();
		bloomFilter.create(100, 0.001);
		bloomFilter.add("Element1");

		BloomFilterService service = new BloomFilterService();
		service.saveFilterToFile("filter", bloomFilter);
		
		Assert.assertTrue(service.loadFilter("filter").compare("Element1"));
		
		File file = new File("filter");
		file.delete();	
	}
	
	@Test
	public void testCache(){
		BloomFilterCache cache = new BloomFilterCache(1000,0.001);
		
		for(int i=0; i<5000; i++){
			cache.add(String.valueOf(i));
		}
		for(int i=0; i<5000; i++){
			Assert.assertTrue(cache.contain(String.valueOf(i)));
		}	
		cache.empty();
	}

}
