package lodVader.spring.REST.controllers;

import java.util.ArrayList;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;

import com.mongodb.BasicDBObject;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.LinksetQueries;

public class DegreeController {

	
	@RequestMapping(value = "/linkset/update", produces=MediaType.APPLICATION_JSON_VALUE)
	public void list() {
		
		ArrayList<LinksetDB> links = new LinksetQueries().getLinksets(new BasicDBObject());		
		
		for(LinksetDB link : links){
			
			link.setDistributionSourceIsVocabulary(new DistributionDB(link.getDistributionSource()).getIsVocabulary());
			link.setDistributionTargetIsVocabulary(new DistributionDB(link.getDistributionTarget()).getIsVocabulary());
			link.update(false, LinksetDB.LINKSET_ID, link.getLinksetID());
		}
		
		
	}

	
	
}
