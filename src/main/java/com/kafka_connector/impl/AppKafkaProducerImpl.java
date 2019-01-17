/**
 * 
 */
package com.kafka_connector.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.SerializationUtils;
import org.springframework.util.concurrent.ListenableFuture;

import com.kafka_connector.apis.AppKafkaProducer;

/**
 * @author amit.jayee
 *
 */
public class AppKafkaProducerImpl<K,V> implements AppKafkaProducer<K,V> {

	private final KafkaTemplate<K, V> kafkaTemplate;

	public AppKafkaProducerImpl(Map<String,Object> configs) {
		this.kafkaTemplate = new KafkaTemplate<>(producerFactory(configs));
	}

	private ProducerFactory<K,V> producerFactory(Map<String,Object> configs) {
		return new DefaultKafkaProducerFactory<K,V>(configs);
	}

	@Override
	public void send(String topic, K key, V data, Consumer<SendResult<K, V>> consumerFunction) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, key, data);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenAccept(consumerFunction);
	}

	
	@Override
	public void send(String topic, Integer partition, K key, V data, Consumer<SendResult<K, V>> consumerFunction) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, key, data);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenAccept(consumerFunction);
	}
	
	
	@Override
	public <T> void send(String topic, K key, V data, Function<SendResult<K, V>, T> function ) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, key, data);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenApply(function);
	}


	@Override
	public <T> void send(String topic, Integer partition, K key, V data, Function<SendResult<K, V>, T> function) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, key, data);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenApply(function);
	}

	@Override
	public <T> void send(String topic, V data, Function<SendResult<K, V>, T> function) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, data);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenApply(function);
	}

	@Override
	public void send(String topic, V data, Consumer<SendResult<K, V>> consumerFunction) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, data);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenAccept(consumerFunction);
	}


	@Override
	public <T> void send(String topic, Integer partition, K key, V data, Map<String, String> headers,
			Function<SendResult<K, V>, T> function) {
		List<Header>  headerLst = getHeaders(headers);
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, key, data, headerLst);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenApply(function);
	}

	@Override
	public void send(String topic, Integer partition, K key, V data, Map<String, String> headers,
			Consumer<SendResult<K, V>> consumerFunction) {
		List<Header>  headerLst = getHeaders(headers);
		ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, key, data, headerLst);
		ListenableFuture<SendResult<K, V>> listenableFuture = this.kafkaTemplate.send(producerRecord);
		CompletableFuture<SendResult<K, V>> completableFuture = listenableFuture.completable();
		completableFuture.thenAccept(consumerFunction);
		
	}

	private List<Header> getHeaders(Map<String, String> headers) {
		List<Header> headerLst = new ArrayList<Header>(headers.size()); 
		for(Map.Entry<String, String> header : headers.entrySet()) {
			Header hdr = new RecordHeader(header.getKey(), SerializationUtils.serialize(header.getValue()));
			headerLst.add(hdr);
		}
		return headerLst;
	}


}
