package com.ronaimate.dispatch.service;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ronaimate.dispatch.client.StockServiceClient;
import com.ronaimate.dispatch.messages.DispatchCompleted;
import com.ronaimate.dispatch.messages.DispatchPreparing;
import com.ronaimate.dispatch.messages.OrderCreated;
import com.ronaimate.dispatch.messages.OrderDispatched;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService {

	private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";

	private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";

	public static final UUID APPLICATION_ID = UUID.randomUUID();

	private final KafkaTemplate<String, Object> kafkaProducer;

	private final StockServiceClient stockServiceClient;

	public void process(final String key, final OrderCreated orderCreated)
			throws ExecutionException, InterruptedException {

		final String available = stockServiceClient.checkAvailability(orderCreated.item());

		if (Boolean.valueOf(available)) {
			kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, DispatchPreparing.builder()
					.orderId(orderCreated.orderId())
					.build()).get();

			kafkaProducer.send(ORDER_DISPATCHED_TOPIC, key, OrderDispatched.builder()
					.orderId(orderCreated.orderId())
					.processedById(APPLICATION_ID)
					.note("Dispatched: " + orderCreated.item())
					.build()).get();

			kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, DispatchCompleted.builder()
					.orderId(orderCreated.orderId())
					.dispatchedDate(LocalDate.now().toString())
					.build()).get();

			log.info("Sent message: key: {} - orderId: {} - processedById: {}", key, orderCreated.orderId(),
					APPLICATION_ID);
		} else {
			log.info("Item {} is unavailable.", orderCreated.item());
		}


	}

}
