package lodVader.spring.REST.controllers;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import lodVader.spring.REST.models.ns.NSStatisticModel;

@RestController
public class NSController {
	
	@RequestMapping(value = "/ns/statistics/{id}", method = RequestMethod.GET, produces="text/plain")
	public String statistics(@PathVariable int id) {
		
		NSStatisticModel model = new NSStatisticModel();
		model.getStatistics(id);
		return model.str.toString();
		
	}
}
