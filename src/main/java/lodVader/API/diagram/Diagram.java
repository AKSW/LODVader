package lodVader.API.diagram;

import java.util.ArrayList;
import java.util.HashMap;

import org.jgrapht.DirectedGraph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.DirectedWeightedMultigraph;
import org.json.JSONArray;

public class Diagram {
	
	ArrayList<Link> links = new ArrayList<Link>();

	public HashMap<Integer, Bubble> bubbles = new HashMap<Integer, Bubble>();

	public Bubble addBubble(Bubble source) {
		if (!bubbles.containsKey(source.getID())) {
			bubbles.put(source.getID(), source);
			return source; 
//			g.addVertex(String.valueOf(source.getID()));
		} else {
			Bubble b = bubbles.get(source.getID());
			return b;
		}
	}

	public void addLink(Link link) {
		if (link.source.name.equals(link.target.name))
			return;
		for (Link l : links) {
			if (l.source.name.equals(link.source.name)
					&& l.target.name.equals(link.target.name))
				return;
		}
		links.add(link);
	}

	public JSONArray getBubblesJSON() {
		JSONArray nodes = new JSONArray();

		for (Bubble b : bubbles.values()) {
			nodes.put(b.getJSON());

		}

		return nodes;
	}

	public JSONArray getLinksJSON() {
		JSONArray edges = new JSONArray();

		for (Link link : links) {
			edges.put(link.getJSON());
		}

		return edges;
	}

	public void printSelectedBubbles(int[] b1) {
		for (Bubble bubble : bubbles.values()) {
			for (int b : b1) {
				if (bubble.getID() ==  b) {
					bubble.setColor("rgb(189, 189, 189)");
				}
			}
		}
	}

	public boolean checkIfBubbleExists(String bubbleURI) {
		if (bubbles.containsKey(bubbleURI))
			return true;
		else
			return false;
	}

	public ArrayList<Link> getLinks() {
		return links;
	}

	public void setLinks(ArrayList<Link> links) {
		this.links = links;
	}

	public ArrayList<Bubble> getBubbles() {
		return new ArrayList<Bubble>(bubbles.values());
	}

}
