package com.veeron.lambdaforaws;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.veeron.lambdaforaws.payloadData.PayloadData;
//import com.veeron.lambdaforaws.payloadData.PayloadData.DeviceData;

import jakarta.annotation.PostConstruct;

@SpringBootApplication
public class LambdaforawsApplication {

	private static final Logger logger = LogManager.getLogger(LambdaforawsApplication.class);

	private AmazonDynamoDB amazonDynamoDB;

	private String DYNAMODB_TABLE_NAME = "VehicleDBStream";
	  
	

	private Regions REGION = Regions.US_EAST_1;

	public static void main(String[] args) {
		System.setProperty("aws.java.v1.disableDeprecationAnnouncement", "true");
		SpringApplication.run(LambdaforawsApplication.class, args);
	}

//		 @PostConstruct
		    public static void main() {
		        try {
		PayloadData.GPS gpsData = PayloadData.GPS.newBuilder().setLatitude(37.7749).setLongitude(-122.4194)
				.setAltitude(30.5f).build();

		long currentTimeMillis = System.currentTimeMillis();
		Timestamp timestamp = Timestamp.newBuilder().setSeconds(currentTimeMillis / 1000)
				.setNanos((int) ((currentTimeMillis % 1000) * 1_000_000)).build();

		PayloadData.VehicleData sampleData = PayloadData.VehicleData.newBuilder().setDeviceNo("EV12346")
				.setGps(gpsData).setTimestamp(timestamp).setBatteryVoltage(75.0f).setBatteryCurrent(15.0f)
				.setVehicleSpeed(60.0f).build();

		        
		            byte[] protobufBytes = sampleData.toByteArray();

		            try (FileOutputStream fos = new FileOutputStream("device1data.bin")) {
		                fos.write(protobufBytes);
		                System.out.println("DeviceData has been serialized to device_data.bin");
		            }

		        } catch (Exception e) {
		            e.printStackTrace();
		        }
		    }
		    
		    @PostConstruct
			public static void main1() {
				
				
				  try (FileOutputStream fos = new FileOutputStream("sampleshiva.bin");
				             DataOutputStream dos = new DataOutputStream(fos)) {

				            // Function name and data
				            String functionName = "reverse1"; // Function to call
				            String data = "hello, world!";   // Data to process

				            // Write header
				            dos.writeInt(data.length()); // Length of the data
				            dos.writeInt(functionName.length()); // Length of the function name

				            // Write body
				            dos.write(functionName.getBytes(StandardCharsets.UTF_8)); // Function name
				            dos.write(data.getBytes(StandardCharsets.UTF_8)); // Data
				            System.out.println("sdcfgscysdcfsoygvdvwvwcfw");
				        } catch (Exception e) {
				            e.printStackTrace();
				        }
			}
		    
	// Vehicle_IoT_Stream
	
	public byte[] hexStringToByteArray(String hex) {
	    int len = hex.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
	                             + Character.digit(hex.charAt(i+1), 16));
	    }
	    return data;
	}
	
//	@Bean
//	public Function<Map<String, String>, String> reverse() {
//	    logger.info("JSON data processing function initialized");
//
//	    return (inputData) -> {
//	        try {
//	            logger.info("Received JSON data: " + inputData);
//
//	            // Parse JSON data
//	            String jsonData = inputData.get("data");
//	            ObjectMapper objectMapper = new ObjectMapper();
//
//	            // Assuming your PayloadData.VehicleData has appropriate setters or a constructor
//	            PayloadData.VehicleData payloadData = objectMapper.readValue(jsonData, PayloadData.VehicleData.class);
//
//	            // Convert to DynamoDB item format
//	            Map<String, AttributeValue> itemValues = new HashMap<>();
//	            itemValues.put("deviceNo", new AttributeValue(payloadData.getDeviceNo()));
//	            itemValues.put("batteryVoltage",
//	                    new AttributeValue().withN(String.valueOf(payloadData.getBatteryVoltage())));
//	            itemValues.put("batteryCurrent",
//	                    new AttributeValue().withN(String.valueOf(payloadData.getBatteryCurrent())));
//	            itemValues.put("vehicleSpeed",
//	                    new AttributeValue().withN(String.valueOf(payloadData.getVehicleSpeed())));
//	            itemValues.put("timestamp",
//	                    new AttributeValue().withN(String.valueOf(payloadData.getTimestamp().getSeconds())));
//	            itemValues.put("latitude",
//	                    new AttributeValue().withN(String.valueOf(payloadData.getGps().getLatitude())));
//	            itemValues.put("longitude",
//	                    new AttributeValue().withN(String.valueOf(payloadData.getGps().getLongitude())));
//	            itemValues.put("altitude",
//	                    new AttributeValue().withN(String.valueOf(payloadData.getGps().getAltitude())));
//
//	            logger.info("Processed DynamoDB item: " + itemValues);
//
//	            // Save to DynamoDB
//	            PutItemRequest putItemRequest = new PutItemRequest(DYNAMODB_TABLE_NAME, itemValues);
//	            amazonDynamoDB.putItem(putItemRequest);
//
//	            return "success";
//	        } catch (Exception e) {
//	            logger.error("Error processing JSON data: " + e.getMessage(), e);
//	        }
//	        return "failure";
//	    };
//	}


	
	@Bean
	public Function<Map<String, String>, String> reverse() {
		logger.info("Lambda function started. Byte data received.");

		return (inputData) -> {
			try {
			
				if (inputData == null || !inputData.containsKey("data")) {
					logger.error("Input data is null or missing the 'data' field.");
					return "failure: Missing 'data' field in input";
				}

				String base64EncodedData = inputData.get("data");
				if (base64EncodedData == null || base64EncodedData.isEmpty()) {
					logger.error("Base64 data is null or empty.");
					return "failure: Base64 data is empty";
				}

				// Decode the base64 string to byte array
				byte[] decodedData = Base64.getDecoder().decode(base64EncodedData);
				logger.info("Decoded data length: " + decodedData.length);

				// Parse the byte array into a Protobuf object
				PayloadData.VehicleData payloadData;
				try {
					payloadData = PayloadData.VehicleData.parseFrom(decodedData);
				} catch (InvalidProtocolBufferException e) {
					logger.error("Error parsing Protobuf data: " + e.getMessage());
					return "failure: Invalid Protobuf data";
				}

				// Log the parsed Protobuf data (for debugging)
				logger.info("Parsed Protobuf data: " + payloadData);

				// Prepare the item for DynamoDB
				Map<String, AttributeValue> itemValues = new HashMap<>();
				itemValues.put("deviceNo", new AttributeValue(payloadData.getDeviceNo()));
				itemValues.put("batteryVoltage",
						new AttributeValue().withN(String.valueOf(payloadData.getBatteryVoltage())));
				itemValues.put("batteryCurrent",
						new AttributeValue().withN(String.valueOf(payloadData.getBatteryCurrent())));
				itemValues.put("vehicleSpeed",
						new AttributeValue().withN(String.valueOf(payloadData.getVehicleSpeed())));
				itemValues.put("timestamp",
						new AttributeValue().withN(String.valueOf(payloadData.getTimestamp().getSeconds())));
				itemValues.put("latitude",
						new AttributeValue().withN(String.valueOf(payloadData.getGps().getLatitude())));
				itemValues.put("longitude",
						new AttributeValue().withN(String.valueOf(payloadData.getGps().getLongitude())));
				itemValues.put("altitude",
						new AttributeValue().withN(String.valueOf(payloadData.getGps().getAltitude())));

				// Log the item values before putting it into DynamoDB
				logger.info("Prepared item values for DynamoDB: " + itemValues);

				// Put the item into DynamoDB
				try {
					PutItemRequest putItemRequest = new PutItemRequest(DYNAMODB_TABLE_NAME, itemValues);
					amazonDynamoDB.putItem(putItemRequest);
					logger.info("Data successfully stored in DynamoDB.");
				} catch (AmazonDynamoDBException e) {
					logger.error("Error saving data to DynamoDB: " + e.getMessage());
					return "failure: DynamoDB error";
				}

				return "success";

			} catch (Exception e) {
				logger.error("Unexpected error: " + e.getMessage(), e);
				return "failure: Unexpected error";
			}
		};
	}

	
//	@Bean
//	public Function<Map<String, String>, String> reverse() {
//	    logger.info("Byte data received");
//
//	    return (inputData) -> {
//	        try {
//	            logger.info("Received byte array");
//
//	            // Retrieve the base64-encoded protobuf data
//	            String base64EncodedData = inputData.get("data");
//	            byte[] decodedData = Base64.getDecoder().decode(base64EncodedData);
//
//	            // Parse the decoded byte array into a protobuf object
//	            PayloadData.VehicleData payloadData = PayloadData.VehicleData.parseFrom(decodedData);
//
//	            // Prepare the item for DynamoDB
//	            Map<String, AttributeValue> itemValues = new HashMap<>();
//	            itemValues.put("deviceNo", new AttributeValue(payloadData.getDeviceNo()));
//	            itemValues.put("batteryVoltage", new AttributeValue().withN(String.valueOf(payloadData.getBatteryVoltage())));
//	            itemValues.put("batteryCurrent", new AttributeValue().withN(String.valueOf(payloadData.getBatteryCurrent())));
//	            itemValues.put("vehicleSpeed", new AttributeValue().withN(String.valueOf(payloadData.getVehicleSpeed())));
//	            itemValues.put("timestamp", new AttributeValue().withN(String.valueOf(payloadData.getTimestamp().getSeconds())));
//	            itemValues.put("latitude", new AttributeValue().withN(String.valueOf(payloadData.getGps().getLatitude())));
//	            itemValues.put("longitude", new AttributeValue().withN(String.valueOf(payloadData.getGps().getLongitude())));
//	            itemValues.put("altitude", new AttributeValue().withN(String.valueOf(payloadData.getGps().getAltitude())));
//
//	            // Optionally log the protobuf object as JSON
//	            String jsonData = JsonFormat.printer().print(payloadData);
//	            logger.info("Protobuf converted to JSON: " + jsonData);
//
//	            // Put item in DynamoDB
//	            PutItemRequest putItemRequest = new PutItemRequest(DYNAMODB_TABLE_NAME, itemValues);
//	            amazonDynamoDB.putItem(putItemRequest);
//
//	            logger.info("Data successfully stored in DynamoDB.");
//	            return "success";
//
//	        } catch (Exception e) {
//	            logger.error("Error processing protobuf data: " + e.getMessage());
//	            return "failure";
//	        }
//	    };
//	}
	
//	@Bean
//	public Function<Map<String, String>, String> reverse() {
//		 logger.info("byte data received ");
//		return (inputData) -> {
//			try {
//			 logger.info("Received byte array: " );
////				
//			        
////				  
//				
//			 String base64EncodedData = inputData.get("data");
//			  byte[] decodedData = Base64.getDecoder().decode(base64EncodedData);
//		            
//		            // Decode only if the data is valid Base64
////				   byte[] protobufData = Base64.getDecoder().decode(data);
//		          //  logger.info("Decoded protobuf data: " + Arrays.toString(byteData));
//				
//				PayloadData.VehicleData payloadData = PayloadData.VehicleData.parseFrom(decodedData);
//
//				Map<String, AttributeValue> itemValues = new LinkedHashMap<>();
//				itemValues.put("deviceNo", new AttributeValue(payloadData.getDeviceNo()));
//				itemValues.put("batteryVoltage",
//						new AttributeValue().withN(String.valueOf(payloadData.getBatteryVoltage())));
//				itemValues.put("batteryCurrent",
//						new AttributeValue().withN(String.valueOf(payloadData.getBatteryCurrent())));
//				itemValues.put("vehicleSpeed",
//						new AttributeValue().withN(String.valueOf(payloadData.getVehicleSpeed())));
//				itemValues.put("timestamp",
//						new AttributeValue().withN(String.valueOf(payloadData.getTimestamp().getSeconds())));
//				itemValues.put("latitude",
//						new AttributeValue().withN(String.valueOf(payloadData.getGps().getLatitude())));
//				itemValues.put("longitude",
//						new AttributeValue().withN(String.valueOf(payloadData.getGps().getLongitude())));
//				itemValues.put("altitude",
//						new AttributeValue().withN(String.valueOf(payloadData.getGps().getAltitude())));
//
////                      itemValues.forEach((key, value) -> logger.info(key + ": " + value.toString()));
//
//                      String jsonData = JsonFormat.printer().print(payloadData);
//                     logger.info("Protobuf converted to JSON: " + jsonData);
////                      System.out.println(jsonData);
//				 logger.info("setting-up the values as required , shiva");
//				PutItemRequest putItemRequest = new PutItemRequest(DYNAMODB_TABLE_NAME, itemValues);
//				amazonDynamoDB.putItem(putItemRequest);
//			
//				return "success";
//
//			} catch (Exception e) {
//
//				logger.error("Error processing protobuf data : " + e.getMessage());
//			}
//			return "failure";
//
//		};
//
//	}
	
//	@Bean
//	public Function<String,String> reverse() {
//		 logger.info("reverse called!");
//		    return (input) -> {
//		        String reversed = new StringBuilder(input).reverse().toString();
//		        logger.info("Input: " + input + ", Reversed: " + reversed);
//		        return reversed;
//		    };
//	}

	@PostConstruct
	private void initDynamoDbClient() {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
	}
	
	 	@Bean
	    public Function<String, String> uppercase() {
	        return value -> value.toUpperCase();
	    }

	    @Bean
	    public Function<String, String> reverse1() {
	        return value -> new StringBuilder(value).reverse().toString();
	    }
}
