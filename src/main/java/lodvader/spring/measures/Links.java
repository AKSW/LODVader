package lodvader.spring.measures;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lodVader.LODVaderProperties;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.enumerators.DatasetStatus;
import lodVader.enumerators.TuplePart;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.GeneralQueries;
import lodVader.mongodb.queries.NSQueries;
import lodVader.utils.NSUtils;

public class Links {

	public void checkLinks() {

		GoogleBloomFilter sFilter = new NSQueries().getNSBloomFilter(TuplePart.SUBJECT);

		AllResources all = new AllResources();
		all.loadNS();

		try {

			FileInputStream fis = new FileInputStream(new File(LODVaderProperties.EVALUATE_LINKS_PATH));
			BufferedWriter fvalid = new BufferedWriter(
					new FileWriter(new File(LODVaderProperties.EVALUATE_LINKS_PATH + "_valid")));
			BufferedWriter funknown = new BufferedWriter(
					new FileWriter(new File(LODVaderProperties.EVALUATE_LINKS_PATH + "_unknown")));

			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			String ns;

			int count = 0;

			while ((line = br.readLine()) != null) {

				if (count % 1000000 == 0)
					System.out.println(count + " links");

				count++;

				ns = new NSUtils().getNSFromString(line);
				if (sFilter.compare(ns)) {
					if (all.query(line))
						fvalid.write(line + "\n");
				} else {
					funknown.write(line + "\n");
				}
			}

			fvalid.close();
			funknown.close();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void checkCohesion() {

		GoogleBloomFilter sFilter = null;

		AllResources all = new AllResources();

		try {

			File folder = new File(LODVaderProperties.EVALUATE_COHESION_PATH);

			File[] listOfFiles = folder.listFiles();

			// iterate folders
			for (int i = 0; i < listOfFiles.length; i++) {
				if (!listOfFiles[i].isFile()) {

					System.out.println(listOfFiles[i].getName().split("_")[0]);

					DatasetDB dataset = new DatasetDB(new GeneralQueries().getMongoDBObject(DatasetDB.COLLECTION_NAME,
							DatasetDB.TITLE, listOfFiles[i].getName().split("_")[0]).get(0));

					// make a NS filter
					ArrayList<DistributionDB> distributions = new DistributionQueries().getDistributions(false,
							DatasetStatus.DONE.toString(), dataset.getLODVaderID());

					// load all distributions BF from an specific dataset
					all.loadNS(dataset.getLODVaderID());

					// get distribution ids
					ArrayList<Integer> distributionIDs = new ArrayList<Integer>();

					for (DistributionDB distribution : distributions) {
						distributionIDs.add(distribution.getLODVaderID());
					}

					// load filter
					sFilter = new NSQueries().getNSBloomFilter(TuplePart.SUBJECT, distributionIDs);

					// iterate files within dataset
					File withinFolder = new File(
							LODVaderProperties.EVALUATE_COHESION_PATH + "/" + listOfFiles[i].getName());
					File[] listOfWithinFiles = withinFolder.listFiles();

					int numberLiterals = 0;
					int numberTotalLinks = 0;
					int numberCohesionLinks = 0;

					for (int j = 0; j < listOfWithinFiles.length; j++) {

						// check whether is a nt file
						if (listOfWithinFiles[j].isFile()) {
							if (listOfWithinFiles[j].getName().contains("nt")) {

								// read file
								FileInputStream fis = new FileInputStream(new File(listOfWithinFiles[j].getPath()));
								BufferedReader br = new BufferedReader(new InputStreamReader(fis));

								String line = null;
								String ns;

								int count = 0;

								while ((line = br.readLine()) != null) {

									if (count % 1000000 == 0)
										System.out.println(count + " links");

									count++;
									
									System.out.println(line);

									// get object
									Pattern pattern = Pattern.compile("^<([^>]+)>\\s+<([^>]+)>\\s(.*)(\\s\\.)");

									Matcher matcher = pattern.matcher(line);

									try {
										System.out.println(matcher.group(1));
										System.out.println(matcher.group(2));
										System.out.println(matcher.group(3));
										String object = matcher.group(3);

										// check wheter is a literal
										if (object.startsWith("\""))
											numberLiterals++;

										// if it's a link
										else {

											numberTotalLinks++;

											// check NS
											ns = new NSUtils().getNSFromString(object);
											if (sFilter.compare(ns)) {

												if (all.query(object)) {
													numberCohesionLinks++;
												}
											}
										}
									} catch (IllegalStateException e) {
										e.printStackTrace();
									}
								}

							}
						}

					}
					System.out.println("Dataset: " + dataset.getTitle());
					System.out.println("Total links: " + numberTotalLinks);
					System.out.println("Total literals: " + numberLiterals);
					System.out.println("Total cohesion: " + numberCohesionLinks);

					System.out.println();

				} else if (listOfFiles[i].isDirectory()) {
					System.out.println("Directory " + listOfFiles[i].getName());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
