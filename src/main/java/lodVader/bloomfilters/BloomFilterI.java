/**
 * 
 */
package lodVader.bloomfilters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Ciro Baron Neto
 * 
 *         Jul 3, 2014
 */
public interface BloomFilterI {

	/**
	 * Creates a bloom filter based on the number of expected insertions and
	 * false positive probability
	 * 
	 * @param insertions
	 *            - Number of expected insertions
	 * @param fpp
	 *            - false positive probability (between 0 and 1)
	 * @return
	 */
	public boolean create(int initialSize, double fpp);

	/**
	 * Add a new element to the set
	 * 
	 * @param element
	 * @return
	 */
	public boolean add(String element);

	/**
	 * Query an element in the set
	 * 
	 * @param element
	 * @return
	 */
	public boolean compare(String element);

	/**
	 * Get number of elements inserted
	 * 
	 * @return - number of elements
	 */
	public int getNumberOfElements();

	/**
	 * Read from an inputstream and save to filter
	 * @param in
	 * @throws IOException
	 */
	public void readFrom(InputStream in) throws IOException;

	/**
	 * Write to an outputstream
	 * @param out
	 * @throws IOException
	 */
	public void writeTo(OutputStream out) throws IOException;

	/**
	 * Get false positive probability
	 * 
	 * @return - false positive probability
	 */
	public double getFPP();

	/**
	 * Get filter designed size
	 * 
	 * @return - filter size
	 */
	public double getFilterInitialSize();

}
