/**
 * 
 */
package com.kafka_connector.apis;

import java.util.function.Consumer;
import java.util.function.Function;

import com.kafka_connector.entities.ConsumerEntity;


/**
 * @author amit.jayee
 *
 */
public interface AppKafkaConsumer<K,V> {
	
	public void embarkConsuming(Consumer<ConsumerEntity<K,V>> consumer);

	<T> void embarkConsuming(Function<ConsumerEntity<K,V>, T> function);
	
}
