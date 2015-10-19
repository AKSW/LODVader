package lodVader.testperformance;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.LODVaderProperties;
import lodVader.API.services.APIStatistics;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.utils.Timer;

public class TestPerformance {
	
	static APIStatistics api = new APIStatistics();
	
	int subjectsFileSize=0;
	int objectsFileSize=0;
	
	String subjectsPath = null ;
	String objectsPath = null;
	
	String SUBJECTS = "subjects";
	String OBJECTS = "objects";
	String PROPERTIES = "properties";
	
	
	public static void main(String[] args) {
		new TestPerformance().go();
	}
	
	
	public void go(){
		try {
		
		new LODVaderProperties().loadProperties();
		
		subjectsPath = LODVaderProperties.SUBJECT_PATH+SUBJECTS;
		objectsPath = LODVaderProperties.OBJECT_PATH+OBJECTS;
		
		String tmp ;
		BufferedReader b = new BufferedReader(new FileReader(subjectsPath));
		while((tmp = b.readLine())!=null){
			if(tmp.contains("http"))
				subjectsFileSize++;
		}
		
		b = new BufferedReader(new FileReader(objectsPath));
		while((tmp = b.readLine())!=null){
			if(tmp.contains("http"))
				objectsFileSize++;
		}		
		System.out.println(subjectsFileSize);
		System.out.println(objectsFileSize);
		
	
		
		System.out.println("Subject file size: "+subjectsFileSize);
		System.out.println("Object file size: "+objectsFileSize);
			
		test(100, PROPERTIES, null, 0);		
		test(200, PROPERTIES, null, 0);		
		test(300, PROPERTIES, null, 0);		
		test(400, PROPERTIES, null, 0);		
		test(500, PROPERTIES, null, 0);		
		test(600, PROPERTIES, null, 0);		
		test(700, PROPERTIES, null, 0);		
		test(800, PROPERTIES, null, 0);		
	
		test(100, SUBJECTS, subjectsPath, subjectsFileSize);			
		test(200, SUBJECTS, subjectsPath, subjectsFileSize);			
		test(300, SUBJECTS, subjectsPath, subjectsFileSize);			
		test(400, SUBJECTS, subjectsPath, subjectsFileSize);			
		test(500, SUBJECTS, subjectsPath, subjectsFileSize);			
		test(600, SUBJECTS, subjectsPath, subjectsFileSize);			
		test(700, SUBJECTS, subjectsPath, subjectsFileSize);			
		test(800, SUBJECTS, subjectsPath, subjectsFileSize);			

		test(100, OBJECTS, objectsPath, objectsFileSize);			
		test(200, OBJECTS, objectsPath, objectsFileSize);			
		test(300, OBJECTS, objectsPath, objectsFileSize);			
		test(400, OBJECTS, objectsPath, objectsFileSize);			
		test(500, OBJECTS, objectsPath, objectsFileSize);			
		test(600, OBJECTS, objectsPath, objectsFileSize);			
		test(700, OBJECTS, objectsPath, objectsFileSize);			
		test(800, OBJECTS, objectsPath, objectsFileSize);			

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	@Test
	public void oi(){
		Random r = new Random();

		int Low = 0;
		int High = 1000000000;
		
		for (int i = 0; i<1000; i++){
			int R = r.nextInt(High-Low) + Low;
			System.out.println(R);
		}
	}
	
	public void test(int numberOfResources, String type, String path, int size) throws Exception{
		ArrayList<String> list = new ArrayList<String>();
		HashSet<Integer> rand = new HashSet<Integer>();
		int counter = 0;
		String tmp;
		
		Random r = new Random();
		if(type.equals(PROPERTIES))
		size =  DBSuperClass.getInstance()
				.getCollection(AllPredicatesDB.COLLECTION_NAME).find().size();
		
		
		int Low = 0;
		int High = size;

		// take number of resources here
		for (int i = 0; i<numberOfResources; i++){
			int R = r.nextInt(High-Low) + Low;
			rand.add(R);
		}
		
		
		if(!type.equals(PROPERTIES)){
			BufferedReader b = new BufferedReader(new FileReader(path));
			while((tmp = b.readLine())!=null){
				if(tmp.contains("http")){
					counter ++;
					if(rand.contains(counter)){
						if (list.contains(tmp)){
							counter -- ;
						}
						else{
							list.add(tmp);
							System.out.println("Matched "+counter+ " "+ tmp);
						}
					}
				}
			}
		}else 
			if(type.equals(PROPERTIES)){
				DBCollection collection = DBSuperClass.getInstance()
						.getCollection(AllPredicatesDB.COLLECTION_NAME);

				DBCursor instances = collection.find();

				for (DBObject instance : instances) {
					tmp = instance.get(AllPredicatesDB.URI).toString();
					counter ++;
					if(rand.contains(counter)){
					if (list.contains(tmp)){
						counter -- ;
					}
					else{
						list.add(tmp);
						System.out.println("Matched "+counter+ " "+ tmp);
					}
					}
				}
			}
		

		
		for(String s : list){
			if(type.equals(SUBJECTS))
				api.listDistributions( 0, 1, 2, null, s, null, null);
			else if(type.equals(OBJECTS))
				api.listDistributions( 0, 1, 2, null, null, null, s);
			else if(type.equals(PROPERTIES))
				api.listDistributions( 0, 1, 2, null, null, s, null);
		}
		
		Timer t = new Timer();
		Timer t2 = new Timer();
		t2.startTimer();
		for(String s : list){
			if(type.contains(SUBJECTS))
				api.listDistributions( 0, 1, 2, null, s, null, null);
			else if(type.contains(OBJECTS))
				api.listDistributions( 0, 1, 2, null, null, null, s);
			else if(type.contains(PROPERTIES))
				api.listDistributions( 0, 1, 2, null, null, s, null);		
		}
		String timer2 = t2.stopTimer();
		System.out.println(timer2);
		System.out.println(list.size());
		FileWriter writer;
		FileWriter writer2;
		if(type.contains(SUBJECTS)){
			writer = new FileWriter(LODVaderProperties.BASE_PATH+"results/subjects"+(String.valueOf(numberOfResources)));
			writer2 = new FileWriter(LODVaderProperties.BASE_PATH+"results/subjects"+"_result", true);
		}
		else if(type.contains(OBJECTS)){
			writer = new FileWriter(LODVaderProperties.BASE_PATH+"results/objects"+(String.valueOf(numberOfResources)));
			writer2 = new FileWriter(LODVaderProperties.BASE_PATH+"results/objects"+"_result", true);
		}
		else{
			writer = new FileWriter(LODVaderProperties.BASE_PATH+"results/properties"+(String.valueOf(numberOfResources)));
			writer2 = new FileWriter(LODVaderProperties.BASE_PATH+"results/properties"+"_result", true); 	
		}
		for(String s : list){
				writer.write(s+"\n");
		}
		
		writer.close();
		
		writer2.write(timer2+"\n");
		writer2.close();
		
	}
}
