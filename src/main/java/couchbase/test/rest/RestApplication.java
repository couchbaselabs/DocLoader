package couchbase.test.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;



@SpringBootApplication(exclude = MongoAutoConfiguration.class)
public class RestApplication {

    public static void main(String[] args) {
        SpringApplication.run(RestApplication.class, args);
    }
}

@RestController
class LoaderController {
    public static Logger logger = LogManager.getLogger(LoaderController.class);
    @PostMapping("/load")
    public ResponseEntity<String> load(@RequestBody LoadRequest loadRequest) {
        try {
            return loadRequest.processRequest();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Error decoding JSON: " + e.getMessage());
        }
    }

    @GetMapping("/check-online")
    public ResponseEntity<String>  CheckOnline(){
        return ResponseEntity.status(HttpStatus.OK).body("magma loader online");
    }


    @GetMapping("/shutdown-app")
    public void shutdownApp() {
        System.exit(0);
    }
}
