package lodVader.integration.mongodb.collections;

import java.net.MalformedURLException;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import lodVader.LODVaderProperties;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LODVaderCounterDB;
import lodVader.utils.FileUtils;

public class DistributionDBTest {

	String downloadURL = "http://lodvader.aksw.org/dump.gz";
	String format = "nt";
	String byteSize = "123";
	String httpFormat = "app";
	String lastModified = "25/12/1990";
	String msg = "No msg!";
	String lastTime = "25/12/1990";
	int objectTriples = 10;
	int subjectTriples = 15;
	String resourceURI = "http://lodvader.aksw.org/";
	int topDataset = 1;
	String topDatasetTitle = "topDataset";
	int triples = 1000;
	int defaultDataset1 = 10001;
	int defaultDataset2 = 10002;
	ArrayList<Integer> defaultDatasets = new ArrayList<Integer>();

	@Test
	public void testDistribution() throws MalformedURLException, LODVaderObjectAlreadyExistsException, LODVaderNoPKFoundException {
		
		new LODVaderProperties().loadProperties();
		FileUtils.checkIfFolderExists();
		LODVaderProperties.MONGODB_DB = LODVaderProperties.MONGODB_DB + "Test";
		DBSuperClass2.getCollection(DistributionDB.COLLECTION_NAME).drop();
		// checking counter
		try {
			new LODVaderCounterDB().incrementAndGetID();
		} catch (Exception e) {
			LODVaderCounterDB c = new LODVaderCounterDB();
			c.setCounterValue(1);
			c.insert(false);
		}



		int id = 0;
		
		defaultDatasets.add(defaultDataset1);
		defaultDatasets.add(defaultDataset2);

		// testing distributions
		DistributionDB dist = new DistributionDB();
		dist.setUri(downloadURL);
		dist.setDownloadUrl(downloadURL);
		dist.setFormat(format);
		dist.setHttpByteSize(byteSize);
		dist.setHttpFormat(httpFormat);
		dist.setHttpLastModified(lastModified);
		dist.setIsVocabulary(false);
		dist.setLastMsg(msg);
		dist.setLastTimeStreamed(lastModified);
		id = new LODVaderCounterDB().incrementAndGetID();
		dist.setLodVaderID(id);
		dist.setNumberOfObjectTriples(objectTriples);
		dist.setNumberOfSubjectTriples(subjectTriples);
		dist.setResourceUri(resourceURI);
		dist.setStatus(DistributionDB.STATUS_DONE);
		dist.setSuccessfullyDownloaded(true);
		dist.setTopDataset(topDataset);
		dist.setTopDatasetTitle(topDatasetTitle);
		dist.setTriples(triples);

		dist.addDefaultDatasets(defaultDataset1);
		dist.addDefaultDatasets(defaultDataset2);
		
		
		try {
			dist.insert(false);
		} catch (LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
			e.printStackTrace();
		}

		dist = new DistributionDB();
		dist.setDownloadUrl(downloadURL);
		dist.setUri(downloadURL);

		
		if(dist.find(true))
		testDist(dist);

		format = "rdf";
		dist.setFormat(format);

		try {
			dist.update(true);
		} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
				| LODVaderNoPKFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		testDist(dist);

		dist = new DistributionDB();
		dist.setDownloadUrl(downloadURL);
		dist.setUri(downloadURL);
		dist.find(true);

		testDist(dist);

		dist = new DistributionDB(id);
		testDist(dist);
		
		dist = new DistributionDB(downloadURL);
		testDist(dist);

		downloadURL = downloadURL+ "/test";
		dist = new DistributionDB(downloadURL);

		Assert.assertTrue(dist.getDownloadUrl().equals(downloadURL));
		
		DBSuperClass2.getCollection(DistributionDB.COLLECTION_NAME).drop();

	
	}
	
	public void testDist(DistributionDB dist){
		Assert.assertTrue(dist.getDownloadUrl().equals(downloadURL));
		Assert.assertTrue(dist.getFormat().equals(format));
		Assert.assertTrue(dist.getHttpByteSize().equals(byteSize));
		Assert.assertTrue(dist.getHttpFormat().equals(httpFormat));
		Assert.assertTrue(dist.getHttpLastModified().equals(lastModified));
		Assert.assertTrue(dist.getLastMsg().equals(msg));
		Assert.assertTrue(dist.getNumberOfObjectTriples() == objectTriples);
		Assert.assertTrue(dist.getNumberOfSubjectTriples() == subjectTriples);
		Assert.assertTrue(dist.getResourceUri().equals(resourceURI));
		
		Assert.assertTrue(dist.getTriples().equals(triples));		
		Assert.assertTrue(dist.getDefaultDatasets().equals(defaultDatasets));		
	}
}
