package com.ronaimate.dispatch.integration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import com.ronaimate.dispatch.config.DispatchConfiguration;
import com.ronaimate.dispatch.messages.DispatchPreparing;
import com.ronaimate.dispatch.messages.OrderDispatched;

import lombok.extern.slf4j.Slf4j;

import static com.ronaimate.dispatch.util.TestEventData.buildOrderCreatedEvent;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = { DispatchConfiguration.class })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
class OrderDispatchIntegrationTest {

	private final static String ORDER_CREATED_TOPIC = "order.created";

	private final static String ORDER_DISPATCHED_TOPIC = "order.dispatched";

	private final static String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";

	@Autowired
	private KafkaTemplate kafkaTemplate;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Autowired
	private KafkaTestListener testListener;

	@Configuration
	static class TestConfig {

		@Bean
		public KafkaTestListener testListener() {
			return new KafkaTestListener();
		}

	}

	static class KafkaTestListener {

		final AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);

		final AtomicInteger orderDispatchedCounter = new AtomicInteger(0);

		@KafkaListener(groupId = "KafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
		void receiveDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) final String key,
				@Payload final DispatchPreparing payload) {
			log.debug("Receiving DispatchPreparing: key: {} - payload: {}", key, payload);
			assertThat(key, notNullValue());
			assertThat(payload, notNullValue());
			dispatchPreparingCounter.incrementAndGet();
		}

		@KafkaListener(groupId = "KafkaIntegrationTest", topics = ORDER_DISPATCHED_TOPIC)
		void receiveDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) final String key,
				@Payload final OrderDispatched payload) {
			log.debug("Receiving DispatchPreparing: key: {} - payload: {}", key, payload);
			assertThat(key, notNullValue());
			assertThat(payload, notNullValue());
			orderDispatchedCounter.incrementAndGet();
		}

	}

	@BeforeEach
	void setUp() {
		testListener.dispatchPreparingCounter.set(0);
		testListener.orderDispatchedCounter.set(0);

		registry.getAllListenerContainers().forEach(container ->
				ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
	}

	@Test
	void testOrderDispatchFlow() throws Exception {
		final var key = randomUUID().toString();
		sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), buildOrderCreatedEvent(randomUUID(), "my-item"));

		await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MICROSECONDS)
				.until(testListener.dispatchPreparingCounter::get, equalTo(1));
		await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MICROSECONDS)
				.until(testListener.orderDispatchedCounter::get, equalTo(1));
	}

	private void sendMessage(final String topic, final String key, final Object data) throws Exception {
		kafkaTemplate.send(MessageBuilder
				.withPayload(data)
				.setHeader(KafkaHeaders.KEY, key)
				.setHeader(KafkaHeaders.TOPIC, topic)
				.build()).get();
	}

}
