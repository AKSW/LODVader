package lodVader.spring.REST.controllers;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import lodVader.spring.REST.models.dataset.StatusModel;

public class DegreeController {

	
	@RequestMapping(value = "/indegree/list", produces=MediaType.APPLICATION_JSON_VALUE)
	public StatusModel status(@RequestParam(value = "dataset", required = true) String datasetAddress) {
		return new StatusModel(datasetAddress);
	}

	
	
}
