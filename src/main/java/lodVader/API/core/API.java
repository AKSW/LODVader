package lodVader.API.core;

import java.util.ArrayList;

/**
 * Superclass of the web API feature
 * @author ciro
 *
 */
public abstract class API extends Thread{
	
	// API message
	ArrayList<APIMessage> message = new ArrayList<APIMessage>();
	
	public abstract void run();
	
	public APIMessage apiMessage = new APIMessage();
	
	public API() {
		
	}

}
