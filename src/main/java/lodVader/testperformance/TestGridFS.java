package lodVader.testperformance;

import java.util.TreeSet;

import org.junit.Test;

import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.utils.Timer;

public class TestGridFS  {
	
	
	
	@Test
	public void Go2(){
	
		String str1 = "b";
		String str2 = "b";
		
		System.out.println(str1.compareTo(str2));
		
	}
//		@Test
		public void Go(){
			
		TreeSet<String> resources = new TreeSet<String>();
		
		
//		    String line;
//
//		   	try (BufferedReader br = new BufferedReader(new FileReader("/home/ciro/temp/subjects"))) {
//				while ((line = br.readLine()) != null) {
//				   resources.add(line);
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		
//		   	System.out.println(resources.size());
		
//		ObjectsBucket b = new ObjectsBucket(resources, 2);
//		b.makeBucket();

//		Timer t = new Timer();
//		t.startTimer();
//		System.out.println(new ObjectsBucket().query("http://dbpedia.org/resource/Augustus", 2));
//		System.out.println(t.stopTimer());
//		
//		t = new Timer();
//		t.startTimer();
//		System.out.println(new SubjectsBucket().query("http://dbpedia.org/resource/Augustus", 2));
//		System.out.println(t.stopTimer());
//		
//		t = new Timer();
//		t.startTimer();
//		System.out.println(new ObjectsBucket().query("http://dbpedia.org/resource/Augustus", 2));
//		System.out.println(t.stopTimer());
		
		
		Timer t = new Timer();
		ObjectsBucket o = new ObjectsBucket("http://dbpedia.org/resource/Augustus", 2);
		System.out.println(o.getFilter());
		t.startTimer();
		System.out.println(o.getFilter());
		System.out.println(t.stopTimer());

	}
	
}
