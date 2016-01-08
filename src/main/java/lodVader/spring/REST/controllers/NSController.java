package lodVader.spring.REST.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lodVader.spring.REST.models.ns.NSStatisticModel;

@RestController
public class NSController {
	
	@RequestMapping(value = "/ns/statistics", method = RequestMethod.GET, produces="text/plain")
	public String statistics(
			@RequestParam(value = "datasetID", required = true) int datasetID,
			@RequestParam(value = "nsLevel", required = false) Integer nsLevel
			) {
		
		NSStatisticModel model = new NSStatisticModel();
		model.getStatistics(datasetID,nsLevel);
		return model.str.toString();
		
	}
}
