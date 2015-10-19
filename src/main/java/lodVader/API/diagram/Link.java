package lodVader.API.diagram;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.json.JSONObject;

public class Link {

	Bubble source;

	Bubble target;

	String links;
	

	public Link(Bubble source, Bubble target, String links) {
		this.source = source;
		this.target = target;
		this.links = links;
	}
	
	public JSONObject getJSON(){
		NumberFormat formatter = new DecimalFormat("#.#######");  
		formatter.setRoundingMode(RoundingMode.CEILING);
	
		JSONObject link = new JSONObject();
		
		link.put("target", target
				.getID());
		link.put("source", source
				.getID());
		if(!links.equals("S") && Double.valueOf(links) <= 1){
			double l = Double.valueOf(links);
			link.put("value", formatter.format(l));
		}
		else
			link.put("value", links);			
		
		return link;
	}

	public Bubble getSource() {
		return source;
	}

	public void setSource(Bubble source) {
		this.source = source;
	}

	public Bubble getTarget() {
		return target;
	}

	public void setTarget(Bubble target) {
		this.target = target;
	}

	public String getLinks() {
		return links;
	}

	public void setLinks(String links) {
		this.links = links;
	}

	

}
