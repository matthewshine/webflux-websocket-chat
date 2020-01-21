package sample.webflux.websocket.netty;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
(
	scanBasePackages = 
	{
		"sample.webflux.websocket.netty.configuration",
	}
)
public class Application 
{
	private final static String SPRING_PROFILE = "spring.profiles.active";	
	
	public static void main(String[] args) throws InterruptedException
	{
		// Sets the spring profile to dev if there is no environment variable that sets it
		if (System.getProperty(SPRING_PROFILE) == null) 
		{
			System.setProperty(SPRING_PROFILE, "dev");
		}

		SpringApplication.run(Application.class, args);
	}
}