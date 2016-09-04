/**
 * 
 */
package lodVader.bloomfilters.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import lodVader.bloomfilters.BloomFilterI;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 4, 2016
 */
public class BloomFilterOrestesImpl implements BloomFilterI {

	BloomFilter<String> bf = null;

	int numberOfElements = 0;

	int initialSize = 0;

	Double fpp;

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#create(int, double)
	 */
	@Override
	public boolean create(int initialSize, double fpp) {

		if (fpp > 1)
			fpp = 0.00000001;
		if (initialSize < 200000)
			initialSize = 200000;

		if (bf == null)
			bf = new FilterBuilder(initialSize, fpp).buildBloomFilter();
		this.initialSize = initialSize;
		this.fpp = fpp;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#add(java.lang.String)
	 */
	@Override
	public boolean add(String element) {
		bf.add(element);
		numberOfElements++;

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#compare(java.lang.String)
	 */
	@Override
	public boolean compare(String element) {
		return bf.contains(element);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#getNumberOfElements()
	 */
	@Override
	public int getNumberOfElements() {
		return numberOfElements;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#readFrom(java.io.InputStream)
	 */
	@Override
	public void readFrom(InputStream in) throws IOException {
		ObjectInputStream restore = new ObjectInputStream(in);
		try {
			bf = (BloomFilter<String>) restore.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		restore.close();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#writeTo(java.io.OutputStream)
	 */
	@Override
	public void writeTo(OutputStream out) throws IOException {
		ObjectOutputStream bfOut = new ObjectOutputStream(out);
		bfOut.writeObject(bf);
		bfOut.close();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#getFPP()
	 */
	@Override
	public double getFPP() {
		return this.fpp;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#getFilterInitialSize()
	 */
	@Override
	public double getFilterInitialSize() {
		return initialSize;
	}

}
