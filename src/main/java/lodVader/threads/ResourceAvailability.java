package lodVader.threads;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import lodVader.mongodb.collections.LinksetDB;

public class ResourceAvailability extends Thread {

	private URL url;

	private int timeout;

	private int index;

	private int status;

	String mongoObjectURL;

	AtomicInteger concurrentConn;

	private ConcurrentHashMap<Integer, ResourceInstance> r = null;

	public ConcurrentHashMap<String, Integer> c;

	HashMap<Integer, String> h;

	public ResourceAvailability(HashMap<Integer, String> h,
			String mongoObjectURL, ConcurrentHashMap<String, Integer> c,
			AtomicInteger concurrentConn) {
		this.concurrentConn = concurrentConn;
		this.mongoObjectURL = mongoObjectURL;
		this.h = h;
		this.c = c;
		this.start();

	}


	public void run() {

		Iterator it = h.entrySet().iterator();
		int positive = 0;

//		while (it.hasNext()) {
//			try {
//				Map.Entry pair = (Map.Entry) it.next();
//				this.setUrl(new URL(pair.getValue().toString()));
//				if (c.get(this.getUrl().toString()) == -1) {
//
//					while (concurrentConn.get() > 2) {
//						try {
//							Thread.sleep(400);
//						} catch (InterruptedException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//					concurrentConn.addAndGet(1);
//
//					HttpURLConnection connection;
//					// System.out.println("-"+this.getUrl()+"-");
//					HttpURLConnection http = (HttpURLConnection) this.getUrl()
//							.openConnection();
//					http.setConnectTimeout(this.getTimeout());
//
//					// If the web service is available
//					this.setStatus(http.getResponseCode());
//					
//					c.put(this.getUrl().toString(), this.getStatus());
//
////					System.out.println(url + " " + this.getStatus());
//					if (this.getStatus() == 200 || this.getStatus() == 303
//							|| this.getStatus() == 302) {
//						positive++;
//					}
//					concurrentConn.decrementAndGet();
//				} else
//				{
//					if (c.get(this.getUrl().toString()) == 200 || c.get(this.getUrl().toString()) == 303
//							|| c.get(this.getUrl().toString()) == 302) {
//						positive++;
//					}
//		
//				}
//
//			} catch (IOException e) {
//				e.getMessage();
//				c.put(this.getUrl().toString(), this.getStatus());
//				concurrentConn.decrementAndGet();
//
//			}
//		}
//
//		if (positive > 0 && h.size() > 0) {
//
//			LinksetMongoDBObject l = new LinksetMongoDBObject(mongoObjectURL);
//			l.setAvailability((int) ((positive / h.size()) * 100));
//			l.updateObject(true);
//
//		}

	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public URL getUrl() {
		return url;
	}

	public void setUrl(URL url) {
		this.url = url;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

}
