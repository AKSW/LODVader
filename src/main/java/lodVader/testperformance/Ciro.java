package lodVader.testperformance;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import lodVader.utils.NSUtils;
import lodVader.utils.Timer;

public class Ciro {

	public static void main(String[] args) {

		
		ArrayList<String> arr = new ArrayList<String>();
		
		for(int i =0 ; i< 1000000; i++){
			arr.add("http://www.google.com");
		}
			
		NSUtils utils = new NSUtils();
		String ns = "http://www.google.com";
		
		Timer t = new Timer();
		t.startTimer();
		
		for(String s: arr){
			if(s.equals(""))
				System.out.println("");
		}
		
		
		System.out.println(t.stopTimer());
		
	}
}
