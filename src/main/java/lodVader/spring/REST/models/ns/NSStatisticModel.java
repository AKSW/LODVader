package lodVader.spring.REST.models.ns;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.NSQueries;
import lodVader.utils.NSUtils;

public class NSStatisticModel {

	public StringBuilder str = new StringBuilder();

	public void getStatistics(Integer datasetID, Integer nsLevel) {

		ArrayList<DistributionDB> distributions;

		if (datasetID != 0) {
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


			double total = 0.0;
			NSUtils nsUtils = new NSUtils();

			HashMap<String, Double> tmp = new HashMap<String, Double>();
			// if nsLevel is set
			if (nsLevel != null) {
				for (DistributionSubjectNSDB ns : nsList) {
					Double amount = tmp.get(nsUtils.getNSFromString(ns.getNS(), nsLevel)); 
					if (amount == null) {
						tmp.put(nsUtils.getNSFromString(ns.getNS(), nsLevel), (double) ns.getNumberOfResources());
					} else {
						tmp.put(nsUtils.getNSFromString(ns.getNS(), nsLevel), ns.getNumberOfResources() + amount);
					}
				}

				for (DistributionSubjectNSDB ns : nsList) {
					total = total + (double) ns.getNumberOfResources();
				}

			} else {

				for (DistributionSubjectNSDB ns : nsList) {
					tmp.put(ns.getNS(), (double) ns.getNumberOfResources());
					total = total + (double) ns.getNumberOfResources();
				}
			}

			str.append("\nTotal number of NS: " + formatter2.format(total));
			str.append("\n ================================================ ");

			for (String s : tmp.keySet()) {
				str.append("\n" + s + "\t Number of NS:" + formatter2.format(tmp.get(s)) + "\t Percentage of NS:"
						+ formatter.format(((tmp.get(s) / total) * 100)) + "%");
			}
			str.append("\n");
			str.append("\n");
		}
	}
}
