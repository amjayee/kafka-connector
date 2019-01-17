/**
 * 
 */
package com.kafka_connector.apis;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.kafka.support.SendResult;

/**
 * @author amit.jayee
 *
 */
public interface AppKafkaProducer<K,V> {

	void send(String topic, K key, V data, Consumer<SendResult<K, V>> consumerFunction);

	<T> void send(String topic, Integer partition, K key, V data, Function<SendResult<K, V>, T> function);
	
	<T> void send(String topic, Integer partition, K key, V data, Map<String, String> headers, Function<SendResult<K, V>, T> function);
	
	void send(String topic, Integer partition, K key, V data, Consumer<SendResult<K, V>> consumerFunction);

	void send(String topic, Integer partition, K key, V data, Map<String, String> headers, Consumer<SendResult<K, V>> consumerFunction);

	<T> void send(String topic, K key, V data, Function<SendResult<K, V>, T> function);

	<T> void send(String topic, V data, Function<SendResult<K, V>, T> function);

	void send(String topic, V data, Consumer<SendResult<K, V>> consumerFunction);

}