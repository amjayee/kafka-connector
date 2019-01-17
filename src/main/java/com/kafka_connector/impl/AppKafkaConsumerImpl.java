/**
 * 
 */
package com.kafka_connector.impl;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import com.kafka_connector.apis.AppKafkaConsumer;
import com.kafka_connector.entities.ConsumerEntity;


/**
 * @author amit.jayee
 *
 */
public class AppKafkaConsumerImpl<K,V> implements AppKafkaConsumer<K, V> {

	private ConcurrentMessageListenerContainer<K,V> container;
	private ContainerProperties containerProperties;
	private DefaultKafkaConsumerFactory<K,V> consumerFactory;
	private int concurrency = 1 ;

	public AppKafkaConsumerImpl(Map<String,Object> configs, String... topics) {
		containerProperties = new ContainerProperties(topics);
		consumerFactory = new DefaultKafkaConsumerFactory<K,V>(configs);

	}
	
	public AppKafkaConsumerImpl(Map<String,Object> configs, int concurrency, String... topics) {
		this.concurrency = concurrency;
		containerProperties = new ContainerProperties(topics);
		consumerFactory = new DefaultKafkaConsumerFactory<K,V>(configs);
	}

	public void embarkConsuming(Consumer<ConsumerEntity<K,V>> consumer) {
		containerProperties.setMessageListener(
				(AcknowledgingMessageListener<K, V>) (record, ack) -> {
					ConsumerEntity<K, V> consumerEntity = new ConsumerEntity<K, V>(record,ack);
					consumer.accept(consumerEntity);
				});
		container = new ConcurrentMessageListenerContainer<K,V>(consumerFactory, containerProperties);
		container.setConcurrency(concurrency);
		container.start();
	}

	public <T> void embarkConsuming(Function<ConsumerEntity<K,V>,T> function) {
		containerProperties.setMessageListener(
				(AcknowledgingMessageListener<K, V>) (record, ack) -> {
					ConsumerEntity<K, V> consumerEntity = new ConsumerEntity<K, V>(record, ack);
					function.apply(consumerEntity);
				});
		container = new ConcurrentMessageListenerContainer<K,V>(consumerFactory, containerProperties);
		container.setConcurrency(concurrency);
		container.start();
	}

}
