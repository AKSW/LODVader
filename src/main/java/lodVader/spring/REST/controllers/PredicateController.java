 package lodVader.spring.REST.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lodVader.spring.REST.models.predicate.ListModel;

@RestController
public class PredicateController {

	@RequestMapping(value = "/predicate/list", method = RequestMethod.GET, produces="text/csv")
//	@RequestMapping(value = "/predicate/list", method = RequestMethod.GET, produces="text/plain")
	public String getPropertyList(@RequestParam(value = "ns", required = false, defaultValue="") String ns) {
		ListModel model = new ListModel(ns);
		return model.getCsv();
	}

}
