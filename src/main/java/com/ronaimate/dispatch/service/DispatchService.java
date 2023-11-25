package com.ronaimate.dispatch.service;

import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ronaimate.dispatch.messages.DispatchPreparing;
import com.ronaimate.dispatch.messages.OrderCreated;
import com.ronaimate.dispatch.messages.OrderDispatched;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DispatchService {

	private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";

	private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";

	private final KafkaTemplate<String, Object> kafkaProducer;

	public void process(final OrderCreated orderCreated) throws ExecutionException, InterruptedException {
		kafkaProducer.send(DISPATCH_TRACKING_TOPIC, DispatchPreparing.builder()
				.orderId(orderCreated.orderId())
				.build()).get();

		kafkaProducer.send(ORDER_DISPATCHED_TOPIC, OrderDispatched.builder()
				.orderId(orderCreated.orderId())
				.build()).get();
	}

}
