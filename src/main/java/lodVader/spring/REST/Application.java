package lodVader.spring.REST;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import lodVader.LODVaderProperties;
import lodVader.StartLODVader;
import lodvader.spring.measures.Links;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		if (LODVaderProperties.EVALUATE_LINKS) {
			Links l = new Links();
			l.checkCohesion();
		} else {
			SpringApplication.run(Application.class, args);
			StartLODVader s = new StartLODVader();
		}
	}
}
