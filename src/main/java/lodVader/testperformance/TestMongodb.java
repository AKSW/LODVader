package lodVader.testperformance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.mongodb.DBCollection;

import lodVader.mongodb.DBSuperClass;
import lodVader.utils.Timer;

public class TestMongodb {

	public static void main(String[] args) {

		DBCollection collection = DBSuperClass.getInstance().getCollection("Ciro");
		
		ArrayList<String> list = new ArrayList<>();
		
		for(int i=0; i<1000000; i++){
			list.add("http://www.aijdioasd.com/asjdasd"+i);
		}

		try {
			BufferedWriter bufferedWriter = new BufferedWriter ( new FileWriter ( "/tmp/opa" ) );
			Timer t = new Timer();
			t.startTimer();
		for (String s : list) {
//			BasicDBObject insert = new BasicDBObject("campo",s);
//			collection.insert(insert);
			
			bufferedWriter.write(s);
			
		}
		
		System.out.println(t.stopTimer());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
