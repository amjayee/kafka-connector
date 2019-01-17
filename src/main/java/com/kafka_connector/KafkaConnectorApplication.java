package com.kafka_connector;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kafka_connector.apis.AppKafkaProducer;
import com.kafka_connector.impl.AppKafkaProducerImpl;


public class KafkaConnectorApplication {
	public static void main(String[] args) {
		//Sample Producer Code - For Testing
		String topicName = "test";
		String key = "Sports";
		String data = "Football";
		Map<String, Object> producerProps = new HashMap<>();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		AppKafkaProducer<String, String> producer = new AppKafkaProducerImpl<String, String>(producerProps);
		producer.send(topicName, key, data,
				(S) -> {
					System.out.println("Produced the Record"+S.getProducerRecord().key());
				});
		
		System.out.println("asd");
	}


}
