package lodVader.unitary.mongodb.collections.namespaces;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.BasicDBObject;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;

public class DistributionSubjectNSDBTest {

	final int datasetID = 1;
	final int distributionID = 2;
	final int numberOfResources = 500;
	final String ns = "http://lodvader.aksw.org/ns";

	@Test
	public void makeObjectsTest() throws LODVaderMissingPropertiesException {

		DistributionSubjectNSDB dist = new DistributionSubjectNSDB();
		dist.setDatasetID(datasetID);
		dist.setDistributionID(distributionID);
		dist.setNS(ns);
		dist.setNumberOfResources(numberOfResources);
		dist.updateObject(true);

		dist = new DistributionSubjectNSDB();
		dist.setDatasetID(datasetID);
		dist.setDistributionID(distributionID);
		dist.setNS(ns);
		dist.setNumberOfResources(numberOfResources);

		BasicDBObject query = new BasicDBObject();
		query.put(DistributionSubjectNSDB.DATASET_ID, datasetID);
		query.put(DistributionSubjectNSDB.DISTRIBUTION_ID, distributionID);
		query.put(DistributionSubjectNSDB.NS, ns);

		Assert.assertTrue(dist.getCollection().find(query).size() > 0);
		Assert.assertTrue(dist.getCollection().remove(query).getN() > 0);

	}

}
