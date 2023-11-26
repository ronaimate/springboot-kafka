package com.ronaimate.dispatch.handler;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.ronaimate.dispatch.messages.OrderCreated;
import com.ronaimate.dispatch.service.DispatchService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {

	private final DispatchService dispatchService;

	@KafkaListener(id = "orderConsumerClient",
			topics = "order.created",
			groupId = "dispatch.order.created.consumer",
			containerFactory = "kafkaListenerContainerFactory")
	public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) final int partition,
			@Header(KafkaHeaders.RECEIVED_KEY) final String key,
			@Payload final OrderCreated payload) {
		log.info("Received message: partition: {} - key: {} - payload: {}", partition, key, payload);
		try {
			dispatchService.process(key, payload);
		} catch (Exception e) {
			log.error("Processing failure", e);
		}
	}

}
