package lodVader.API.diagram;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Link {

	@JsonIgnore
	Node nodeSource;

	@JsonIgnore
	Node nodeTarget;

	@JsonIgnore
	String links;

	public Link(Node source, Node target, String links) {
		this.nodeSource = source;
		this.nodeTarget = target;
		this.links = links;
	}

	public Node getNodeSource() {
		return nodeSource;
	}

	public void setNodeSource(Node source) {
		this.nodeSource = source;
	}

	public Node getNodeTarget() {
		return nodeTarget;
	}

	public void setNodeTarget(Node target) {
		this.nodeTarget = target;
	}
	
	public String getValue(){
		return links;
	}
	
	public int getSource(){
		return nodeSource.getID();
	}
	
	public int getTarget(){
		return nodeTarget.getID();
	}

	public String getLinks() {
		NumberFormat formatter = new DecimalFormat("#.#######");
		formatter.setRoundingMode(RoundingMode.CEILING);

		if (!links.equals("S") && Double.valueOf(links) <= 1) {
			double l = Double.valueOf(links);
			return formatter.format(l);
		} else
			return links;
	}

	public void setLinks(String links) {
		this.links = links;
	}

}
