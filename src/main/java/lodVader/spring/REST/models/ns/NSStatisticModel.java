package lodVader.spring.REST.models.ns;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.NSQueries;

public class NSStatisticModel {

	public StringBuilder str = new StringBuilder();

	public void getStatistics(Integer datasetID) {

		ArrayList<DistributionDB> distributions;

		if (datasetID != null) {
			distributions = new DistributionQueries().getDistributions(null, null, datasetID);
		} else {
			distributions = new DistributionQueries().getDistributions(null, null, null);
		}

		NumberFormat formatter = new DecimalFormat("###.##");
		NumberFormat formatter2 = new DecimalFormat("#,###,###,###,###");

		for (DistributionDB distribution : distributions) {
			str.append("\n ================================================ ");
			str.append("\nDataset: " + distribution.getTopDatasetTitle());
			str.append("\nDistribution: " + distribution.getDownloadUrl());

			ArrayList<DistributionSubjectNSDB> nsList = new NSQueries()
					.getSubjectNSByDistribution(distribution.getLODVaderID());

			HashMap<String, Integer> tmp = new HashMap<String, Integer>();

			double total = 0.0;

			for (DistributionSubjectNSDB ns : nsList) {
				tmp.put(ns.getNS(), ns.getNumberOfResources());
				total = total + ns.getNumberOfResources();
			}

			str.append("\nTotal number os NS: " + formatter2.format(total));
			str.append("\n ================================================ ");

			for (String s : tmp.keySet()) {
				str.append("\n" + s + "\t Number of NS:" + formatter2.format(tmp.get(s)) + "\t Percentage of NS:"
						+ formatter.format(((tmp.get(s) / total) * 100)) + "%");
			}
			str.append("\n");
			str.append("\n");


		}

		System.out.println(str.toString());

	}

}
