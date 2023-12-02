package com.ronaimate.dispatch.integration;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
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
import com.ronaimate.dispatch.messages.DispatchCompleted;
import com.ronaimate.dispatch.messages.DispatchPreparing;
import com.ronaimate.dispatch.messages.OrderCreated;
import com.ronaimate.dispatch.messages.OrderDispatched;
import com.ronaimate.dispatch.util.TestEventData;

import lombok.extern.slf4j.Slf4j;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static com.ronaimate.dispatch.integration.WiremockUtils.stubWiremock;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = { DispatchConfiguration.class })
@AutoConfigureWireMock(port = 0)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
class OrderDispatchIntegrationTest {

	private final static String ORDER_CREATED_TOPIC = "order.created";

	private final static String ORDER_DISPATCHED_TOPIC = "order.dispatched";

	private final static String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";

	private final static String ORDER_CREATED_DLT_TOPIC = "order.created.DLT";

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

	@KafkaListener(groupId = "KafkaIntegrationTest", topics = { DISPATCH_TRACKING_TOPIC, ORDER_DISPATCHED_TOPIC,
			ORDER_CREATED_DLT_TOPIC })
	static class KafkaTestListener {

		final AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);

		final AtomicInteger orderDispatchedCounter = new AtomicInteger(0);

		final AtomicInteger dispatchCompletedCounter = new AtomicInteger(0);

		final AtomicInteger orderCreatedDLTCounter = new AtomicInteger(0);

		@KafkaHandler
		void receiveDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key,
				@Payload DispatchPreparing payload) {
			log.debug("Received DispatchPreparing key: {} - payload: {}", key, payload);
			assertThat(key, notNullValue());
			assertThat(payload, notNullValue());
			dispatchPreparingCounter.incrementAndGet();
		}

		@KafkaHandler
		void receiveOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) final String key,
				@Payload final OrderDispatched payload) {
			log.debug("Received OrderDispatched key: {} - payload: {}", key, payload);
			assertThat(key, notNullValue());
			assertThat(payload, notNullValue());
			orderDispatchedCounter.incrementAndGet();
		}

		@KafkaHandler
		void receiveDispatchCompleted(@Header(KafkaHeaders.RECEIVED_KEY) final String key,
				@Payload final DispatchCompleted payload) {
			log.debug("Received DispatchCompleted key: {} - payload: {}", key, payload);
			assertThat(key, notNullValue());
			assertThat(payload, notNullValue());
			dispatchCompletedCounter.incrementAndGet();
		}

		@KafkaHandler
		void receiveOrderCreatedDLT(@Header(KafkaHeaders.RECEIVED_KEY) final String key,
				@Payload final OrderCreated payload) {
			log.debug("Received OrderCreated DLT key: {} - payload: {}", key, payload);
			assertThat(key, notNullValue());
			assertThat(payload, notNullValue());
			orderCreatedDLTCounter.incrementAndGet();
		}

	}


	@BeforeEach
	void setUp() {
		testListener.dispatchPreparingCounter.set(0);
		testListener.orderDispatchedCounter.set(0);
		testListener.dispatchCompletedCounter.set(0);
		testListener.orderCreatedDLTCounter.set(0);

		WiremockUtils.reset();

		registry.getListenerContainers()
				.forEach(container -> ContainerTestUtils.waitForAssignment(container,
						Objects.requireNonNull(container.getContainerProperties().getTopics()).length *
								embeddedKafkaBroker.getPartitionsPerTopic()));
	}

	@Test
	public void testOrderDispatchFlow_Success() throws Exception {
		stubWiremock("/api/stock?item=my-item", 200, "true");
		final var orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");

		sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

		await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.dispatchPreparingCounter::get, equalTo(1));
		await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.orderDispatchedCounter::get, equalTo(1));
		await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.dispatchCompletedCounter::get, equalTo(1));
		assertThat(testListener.orderCreatedDLTCounter.get(), equalTo(0));
	}

	@Test
	void testOrderDispatchFlow_NotRetryableException() throws Exception {
		stubWiremock("/api/stock?item=my-item", 400, "Bad Request");
		final var orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");

		sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

		await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.orderCreatedDLTCounter::get, equalTo(1));
		assertThat(testListener.dispatchPreparingCounter.get(), equalTo(0));
		assertThat(testListener.orderDispatchedCounter.get(), equalTo(0));
		assertThat(testListener.dispatchCompletedCounter.get(), equalTo(0));
	}

	@Test
	void testOrderDispatchFlow_RetryThenSuccess() throws Exception {
		stubWiremock("/api/stock?item=my-item", 503, "Service unavailable", "failOnce", STARTED, "succeedNextTime");
		stubWiremock("/api/stock?item=my-item", 200, "true", "failOnce", "succeedNextTime", "succeedNextTime");
		final var orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");

		sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

		await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.dispatchPreparingCounter::get, equalTo(1));
		await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.orderDispatchedCounter::get, equalTo(1));
		await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.dispatchCompletedCounter::get, equalTo(1));
		assertThat(testListener.orderCreatedDLTCounter.get(), equalTo(0));
	}

	@Test
	void testOrderDispatchFlow_RetryUntilFailure() throws Exception {
		stubWiremock("/api/stock?item=my-item", 503, "Service unavailable");
		final var orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");

		sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

		await().atMost(5, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
				.until(testListener.orderCreatedDLTCounter::get, equalTo(1));
		assertThat(testListener.dispatchPreparingCounter.get(), equalTo(0));
		assertThat(testListener.orderDispatchedCounter.get(), equalTo(0));
		assertThat(testListener.dispatchCompletedCounter.get(), equalTo(0));
	}

	private void sendMessage(final String topic, final String key, final Object data) throws Exception {
		kafkaTemplate.send(MessageBuilder
				.withPayload(data)
				.setHeader(KafkaHeaders.KEY, key)
				.setHeader(KafkaHeaders.TOPIC, topic)
				.build()).get();
	}

}
