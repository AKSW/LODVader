package lodVader.spring.REST.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DistributionQueries;

@RestController
public class CouplingCohesionController {

	@RequestMapping(value = "/cohesion/distribution", method = RequestMethod.GET, produces="text/plain")
	public String getCohesionList() {

		StringBuilder str = new StringBuilder();

		int totalTriples = 0;
		int totalCohesion = 0;
		int totalObjectResources = 0;

		for (DistributionDB distribution : new DistributionQueries().getDistributionsByCohesion()){
			totalTriples = totalTriples + distribution.getTriples();
			totalCohesion = totalCohesion + distribution.getObjectCohesion();
			totalObjectResources = totalObjectResources + distribution.getNumberOfObjectTriples();
			if (totalTriples > 0) {

			str.append("Dataset: " + distribution.getTopDatasetTitle()+"\n");
			str.append("Total Triples: " + totalTriples+"\n");
			str.append("Total Cohesion Links: " + totalCohesion+"\n");
			str.append("Cohesion: " + new Double(totalCohesion / totalTriples)+"\n");
			str.append("Total Object Resources: " + totalObjectResources+"\n");
			str.append("\n\n\n");
			}
		}
		
		return str.toString();

	}
}
