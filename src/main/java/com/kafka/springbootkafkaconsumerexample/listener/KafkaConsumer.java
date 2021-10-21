package com.kafka.springbootkafkaconsumerexample.listener;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.kafka.springbootkafkaconsumerexample.model.AuditLog;
import com.kafka.springbootkafkaconsumerexample.model.ProductCartLog;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;

@Service
public class KafkaConsumer {
	
	private static Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
	
	@LoadBalanced
	@Bean
	RestTemplate getRestTemplate() {
		return new RestTemplate();
	}

	@Autowired
	RestTemplate restTemplate;
	
	@Autowired
	@Lazy
	private EurekaClient eurekaClient;

    /*@KafkaListener(topics = "Kafka_Example", groupId = "group_id")
    public void consume(String message) {
        System.out.println("Consumed message: " + message);
    }*/
    
    @KafkaListener(topics = "Kafka_Example_json", groupId = "group_json",
    containerFactory = "userKafkaListenerFactory") 
    public String consumeJson(AuditLog logs)
    { 
    	log.info("Consumed JSON Message: " + logs); 
    	try {
			
			saveLog(logs);
		} catch (Exception e) {
			
			log.error("exception::"+e.getMessage());
		}
    	return "consumeJson";
    }
    
    
    @KafkaListener(topics = "Kafka_Place_json", groupId = "group_json",
    	    containerFactory = "userKafkaListenerPlaceFactory") 
    	    public String consumePlace(ProductCartLog logs)
    	    { 
    	    	log.info("Consumed JSON Message: " + logs); 
    	    	try {
    				
    				
    				//if(logs.getService().equals("PlaceOrder"))
    				//{
    					placeOrder(logs);
    					logs.setOperation("orderPlace");
    				//}
    				//saveLog(logs);
    			} catch (Exception e) {
    				
    				log.error("exception::"+e.getMessage());
    			}
    	    	return "consumeJson";
    	    }
    
    public String saveLog(AuditLog logs) throws Exception 
    {
    	
    	try
		{
			InstanceInfo info = eurekaClient.getNextServerFromEureka("LOGSERVICE", false);
			String url = "http://" + info.getHostName() + ":" + info.getPort() + "/addlogger";
			
			URI uri = new URI(url);
			log.info("uri>>"+uri);
			ResponseEntity<String> response = new RestTemplate().postForEntity(uri, logs, String.class);
			if(response!=null && response.getBody()!=null )
			{
				
				return "done";
			}
		}catch (HttpClientErrorException httpClientErrorException) {
			log.info("saveLog - " + httpClientErrorException.getStatusCode().toString());
			log.info("saveLog - " + httpClientErrorException.getResponseBodyAsString());
			throw httpClientErrorException;
		}
		catch (Exception e) {
			log.info(e.getMessage());
			throw e;
		}
		return "done";
    	
    }

    
    public String placeOrder(ProductCartLog logs) throws Exception 
    {
    	
    	log.info("Place Order >>");
    	String resp="Order Not Placed.";
    	try
		{
			InstanceInfo info = eurekaClient.getNextServerFromEureka("ORDERPLACE", false);
			String url = "http://" + info.getHostName() + ":" + info.getPort() + "/orderPlace";
			
			URI uri = new URI(url);
			log.info("placeOrder ::uri>>"+uri);
			
			log.info("Operation : "+logs.getOperation());
			
			HttpHeaders headers = new HttpHeaders();
			headers.set("Authorization", logs.getOperation());
			HttpEntity entity = new HttpEntity(headers);
			ResponseEntity<Integer> response = new RestTemplate().postForEntity(uri, entity, Integer.class);
			log.info("response.getBody() : "+response.getBody());
			if(response!=null && response.getBody()!=null )
			{
				
				resp = "Your Order Number: "+ response.getBody();
			}else
			{
				resp = "Order Not Placed";
			}
		}catch (HttpClientErrorException httpClientErrorException) {
			log.info("saveLog - " + httpClientErrorException.getStatusCode().toString());
			log.info("saveLog - " + httpClientErrorException.getResponseBodyAsString());
			throw httpClientErrorException;
		}
		catch (Exception e) {
			log.info(e.getMessage());
			throw e;
		}
		return resp;
    }
    
    
	/*
	 * @KafkaListener(topics = "Kafka_Example_json", group = "group_json",
	 * containerFactory = "userKafkaListenerFactory") public void consumeJson(User
	 * user) { System.out.println("Consumed JSON Message: " + user); }
	 */
}
