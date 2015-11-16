package lodVader.unitary.linkset;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.riot.RiotException;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import lodVader.LODVaderProperties;
import lodVader.Manager;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderNoDatasetFoundException;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.descriptionfileparser.DescriptionFileParser;
import lodVader.utils.FileUtils;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IndexingTest {

	@Test
	public void testACreateDataset() {

		new LODVaderProperties().loadProperties();
		FileUtils.checkIfFolderExists();
		LODVaderProperties.MONGODB_DB = LODVaderProperties.MONGODB_DB + "Test";

		for (int i = 0; i < 10; i++) {
			DatasetDB dataset = new DatasetDB("http://lodvader.aksw.org/" + i);
			dataset.setLabel("label dataset" + i);
			dataset.setTitle("title dataset" + i);
			dataset.setIsVocabulary(false);
			dataset.addParentDatasetID(0);
			Assert.assertTrue(dataset.updateObject(false));
		}
	}

	@Test
	public void testBCreateDistribution() {
		for (int i = 0; i < 10; i++) {
			DistributionDB distribution = new DistributionDB("http://lodvader.aksw.org/dist/" + i);
			distribution.setTitle("title distribution" + i);
			distribution.setIsVocabulary(false);
			distribution.addDefaultDataset(i);
			distribution.setDownloadUrl("http://lodvader.aksw.org/dist/" + i);
			distribution.setFormat("nt");
			distribution.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
			Assert.assertTrue(distribution.updateObject(false));
		}
	}

	@Test
	public void testCRemoveDataset() {
		for (int i = 1; i <= 10; i++) {
			DatasetDB dataset = new DatasetDB(i);
			Assert.assertTrue(dataset.remove());
		}
	}

	@Test
	public void testDRemoveDistributions() {
		for (int i = 11; i <= 20; i++) {
			DistributionDB distribution = new DistributionDB(i);
			Assert.assertTrue(distribution.remove());
		}
	}

	@Test
	public void testEStream() throws RiotException, MalformedURLException, IOException, LODVaderNoDatasetFoundException,
			LODVaderFormatNotAcceptedException, LODVaderLODGeneralException {
		
//		LODVaderProperties.CHECK_LINKS_EACH = 1;
		LODVaderProperties.CHECK_LOV = false;
		LODVaderProperties.ONLY_STREAM_DATASETS_AND_SAVE_NT_FORMAT = true;

		ArrayList<String> listOfURL = new ArrayList<String>();
//		listOfURL.add("http://localhost/dbpedia/dataid.ttl");
		listOfURL.add(
				"https://raw.githubusercontent.com/cirola2000/DynamicLOD/master/src/main/webapp/dataids_example/dataid-datasetTest.ttl");
		listOfURL.add(
				"https://raw.githubusercontent.com/cirola2000/DynamicLOD/master/src/main/webapp/dataids_example/dataid-reuters128.ttl");
		listOfURL.add(
				"https://raw.githubusercontent.com/cirola2000/DynamicLOD/master/src/main/webapp/dataids_example/dataid-news100.ttl");
		listOfURL.add(
				"https://raw.githubusercontent.com/cirola2000/DynamicLOD/master/src/main/webapp/dataids_example/dataid-rss500.ttl");
		

		for (String url : listOfURL) {
			DescriptionFileParser inputRDFParser = new DescriptionFileParser();
			// read and parse description file
			inputRDFParser.readModel(url, "ttl");
			inputRDFParser.parseDistributions();

			new Manager(inputRDFParser.distributionsLinks);
		}

	}

	@Test
	public void testZRemoveDatabase() {
		 DBSuperClass.getInstance().dropDatabase();
	}

}
