package lodVader.threads;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import javax.activation.DataHandler;

import lodVader.TuplePart;
import lodVader.utils.NSUtils;

public class JobThread implements Runnable {
	ArrayList<String> listOfResources;
	DataModelThread dataThread = null;
	
	NSUtils nsUtils = new NSUtils();
	

	public JobThread(DataModelThread dataThread, ArrayList<String> lines) {
		this.listOfResources = lines;
		this.dataThread = dataThread;

	}

	public void run() {
		boolean isSubject = false;
		if(dataThread.tuplePart.equals(TuplePart.SUBJECT))
			isSubject = true;
		try {
			for (String resource: listOfResources) {
				if (dataThread.filter.compare(resource)) {
					dataThread.links.addAndGet(1);
//					System.out.println(val + dataThread.links.get() + dataThread.sourceColumnIsSubject);
//					dataThread.availabilityCounter++;
//					
//					dataThread.weightCount++;
//					dataThread.count++;
//					
//					if(dataThread.count%dataThread.setSize==0)
//						dataThread.weight++;
//					
//					if(dataThread.weightCount%dataThread.weight==0){
//						String url = lines[i].replace("<", "").replace(">", "");
//						
//						if(c.putIfAbsent(url, -1) == null){
////							new ResourceAvailability((dataThread.count%dataThread.setSize), url, 2000, dataThread.urlStatus, c);
//							dataThread.listURLToTest.put(dataThread.count%dataThread.setSize, url);
//						}
////						else
////							dataThread.urlStatus.put((dataThread.count%dataThread.setSize), 
////									new ResourceInstance(url, c.putIfAbsent(url, 0)));
//						dataThread.weightCount = 0;						
//					}

				}
				else{
					if(isSubject){
					String obj;
					// get FQDN of value to compare
//					String[] ar = resource.split("/");
//					if (ar.length > 5)
//						obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/" + ar[4] + "/"+ ar[5] + "/";
//					if (ar.length > 4)
//						obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/" + ar[4] + "/";
//					if (ar.length > 3)
//						obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/";
//					else if (ar.length > 2)
//						obj = ar[0] + "//" + ar[2] + "/";
//					else {
//						obj = null;
//					}
					
					obj = nsUtils.getNSFromString(resource);
					
					if(obj!=null){
						// compare with tree
						if(dataThread.targetFQDNTree.contains(obj)){
							// case math with tree values, add invalid link by 1
							dataThread.invalidLinks.addAndGet(1);
//							if(!dataThread.isSubject)
						}
							
					}
//					
				}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
