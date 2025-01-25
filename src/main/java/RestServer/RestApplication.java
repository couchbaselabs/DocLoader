package RestServer;

import java.util.HashMap;
import java.util.Map;

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
class RestHandlers {
    public static Logger logger = LogManager.getLogger(RestHandlers.class);

    @GetMapping(value={"/check-online", "/"})
    public ResponseEntity<Map<String, Object>>  CheckOnline(){
        Map<String, Object> body = new HashMap<>();
        body.put("name", "java_doc_loader");
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    @PostMapping(value="/get_doc_keys")
    public ResponseEntity<Map<String, Object>> get_doc_keys(@RequestBody TaskRequest taskRequest) {
        return taskRequest.get_doc_keys();
    }

    @PostMapping(value="/init_task_manager")
    public ResponseEntity<Map<String, Object>> init_task_manager(@RequestBody TaskRequest taskRequest) {
        return taskRequest.init_task_manager();
    }

    @PostMapping(value="/shutdown_task_manager")
    public ResponseEntity<Map<String, Object>> shutdown_task_manager(@RequestBody TaskRequest taskRequest) {
        return taskRequest.shutdown_task_manager();
    }

    // Blocking call, waits until the task finishes
    @PostMapping(value="/get_task_result")
    public ResponseEntity<Map<String, Object>> get_task_result(@RequestBody TaskRequest taskRequest) {
        return taskRequest.get_task_result();
    }

    // Submit task to TaskManager
    @PostMapping(value="/submit_task")
    public ResponseEntity<Map<String, Object>> submit_task(@RequestBody TaskRequest taskRequest) {
        return taskRequest.submit_task();
    }

    // Graceful way for stopping the load
    @PostMapping(value="/stop_task")
    public ResponseEntity<Map<String, Object>> stop_task(@RequestBody TaskRequest taskRequest) {
        try {
            return taskRequest.stop_task();
        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", e.getMessage());
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
    }

    // Hard way for stopping the task
    @PostMapping(value="/cancel_task")
    public ResponseEntity<Map<String, Object>> cancel_task(@RequestBody TaskRequest taskRequest) {
        try {
            return taskRequest.stop_task();
        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", e.getMessage());
            body.put("status", false);
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
    }

    @PostMapping(value="/create_clients")
    public ResponseEntity<Map<String, Object>> create_clients(@RequestBody TaskRequest taskRequest) {
        try {
            return taskRequest.create_clients();
        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", e.toString());
            body.put("status", false);
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
    }

    @PostMapping(value="/doc_load")
    public ResponseEntity<Map<String, Object>> doc_load(@RequestBody TaskRequest taskRequest) {
        try {
            return taskRequest.doc_load();
        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", e.toString());
            body.put("status", false);
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
    }

    @PostMapping(value="/reset_task_manager")
    public ResponseEntity<Map<String, Object>> reset_task_manager(@RequestBody TaskRequest taskRequest) {
        try {
            return taskRequest.reset_task_manager();
        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", e.toString());
            body.put("status", false);
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
    }

    @PostMapping(value="/shutdown")
    public void shutdownApp() {
        System.exit(0);
    }
}