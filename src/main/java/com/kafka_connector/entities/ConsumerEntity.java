/**
 * 
 */
package com.kafka_connector.entities;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.SerializationUtils;

/**
 * @author amit.jayee
 *
 */
public class ConsumerEntity<K,V> {

	private final K key;
	private final V value;
	private final long offset;
	private final int partition;
	private final String topic;
	private final Acknowledgment ack;
	private final Headers rawHeader;
	private final Map<String, String> headers;
	public ConsumerEntity(ConsumerRecord<K, V> record, Acknowledgment ack) {
		this.key = record.key();
		this.value = record.value();
		this.offset = record.offset();
		this.partition = record.partition();
		this.topic = record.topic();
		this.rawHeader = record.headers();
		this.headers = getHeaders(this.rawHeader);
		this.ack = ack;
	}

	public ConsumerEntity(ConsumerRecord<K, V> record) {
		this.key = record.key();
		this.value = record.value();
		this.offset = record.offset();
		this.partition = record.partition();
		this.topic = record.topic();
		this.rawHeader = record.headers();
		this.headers = getHeaders(this.rawHeader);
		this.ack = null;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public long getOffset() {
		return offset;
	}

	public int getPartition() {
		return partition;
	}

	public String getTopic() {
		return topic;
	}

	public Acknowledgment getAck() {
		return ack;
	}

	public void ack() {
		this.ack.acknowledge();
	}

	public Map<String, String> getHeaders() {
		return headers;
	}
	
	private Map<String, String> getHeaders(Headers rawHeader) {
		Map<String, String> headersMap = new HashMap<String, String>();
		Header[] headerArr = rawHeader.toArray();
		for(int i=0;i<headerArr.length;i++) {
			Header header = headerArr[i];
			headersMap.put(header.key(), (String)SerializationUtils.deserialize(header.value()));
		}
		return headersMap;
	}

	@Override
	public String toString() {
		return "ConsumerEntity [key=" + key + ", value=" + value + ", offset=" + offset + ", partition=" + partition
				+ ", topic=" + topic + ", ack=" + ack + ", headers=" + headers + "]";
	}

}