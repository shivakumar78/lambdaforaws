package com.veeron.lambdaforaws;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class TestController {

	 @Autowired
	    private FunctionCatalog functionCatalog;

	    @PostMapping
	    public ResponseEntity<String> processBinaryData(@RequestBody byte[] input) {
	        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(input))) {

	            // Parse header
	            int dataLength = dis.readInt();
	            int functionNameLength = dis.readInt();

	            // Parse body
	            byte[] functionNameBytes = new byte[functionNameLength];
	            dis.readFully(functionNameBytes);
	            String functionName = new String(functionNameBytes, StandardCharsets.UTF_8);

	            byte[] dataBytes = new byte[dataLength];
	            dis.readFully(dataBytes);
	            String data = new String(dataBytes, StandardCharsets.UTF_8);

	            // Lookup and apply the function
	            Function<String, String> function = functionCatalog.lookup(functionName);
	            if (function == null) {
	                return ResponseEntity.badRequest().body("Function not found: " + functionName);
	            }

	            return ResponseEntity.ok(function.apply(data));

	        } catch (Exception e) {
	            return ResponseEntity.status(500).body("Error processing binary data: " + e.getMessage());
	        }
	    }
}