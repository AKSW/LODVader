package lodVader.performance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import org.junit.Test;

public class CollectionPerformance {

//	@Test
	public void testA() {
		try {
			Thread.sleep(8000);

			HashMap<String, Integer> links = new HashMap<String, Integer>();
			int size = 10000000;
			
			for (int i = 0; i < size; i++) {
				String s = "http://"+ i+"yada/yahadad/asdsad";
				links.put(s,1);
			}

			Thread.sleep(5000);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

//	@Test
	public void testB() throws IOException {
        // Create file object
        File file = new File("/tmp/file");
         
        //Delete the file; we will create a new file
        file.delete();
 
        // Get file channel in readonly mode
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
 
        // Get direct byte buffer access using channel.map() operation
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 64);
 
        //Write the content using put methods
        buffer.put("oi ".getBytes());
        
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 1, 64);
        
        buffer.put("dae!".getBytes());
        
	}
	

}
