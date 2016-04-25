package lodVader.spring.REST.controllers;

import lodVader.enumerators.DatasetStatus;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.mongodb.queries.DistributionQueries;

public class CouplingCohesionController {

	// @RequestMapping(value = "/CouplingCohesion/Dataset", method =
	// RequestMethod.GET)
//	public static void main(String[] args) {
//
//		// }
//		// public void load() {
//		for (DatasetDB dataset : new DatasetQueries().getDatasets(false)) {
//
//			int totalTriples = 0;
//			int totalCohesion = 0;
//			int totalObjectResources = 0;
//
//			for (DistributionDB distribution : new DistributionQueries().getDistributions(false,
//					DatasetStatus.DONE.toString(), dataset.getLODVaderID())) {
//				totalTriples = totalTriples + distribution.getTriples();
//				totalCohesion = totalCohesion + distribution.getObjectCohesion();
//				totalObjectResources = totalObjectResources + distribution.getNumberOfObjectTriples();
//			}
//
//			if (totalTriples > 0) {
//				System.out.println("Dataset: " + dataset.getTitle());
//				System.out.println("Total Triples: " + totalTriples);
//				System.out.println("Total Cohesion Links: " + totalCohesion);
//				System.out.println("Cohesion: " + new Double(totalCohesion/totalTriples));
//				System.out.println("Total Object Resources: " + totalObjectResources);
//				System.out.println();
//			}
//		}
//	}
}
