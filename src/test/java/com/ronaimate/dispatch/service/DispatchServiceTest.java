package com.ronaimate.dispatch.service;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import com.ronaimate.dispatch.messages.DispatchPreparing;
import com.ronaimate.dispatch.messages.OrderDispatched;

import static com.ronaimate.dispatch.util.TestEventData.buildOrderCreatedEvent;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DispatchServiceTest {

	private DispatchService service;

	private KafkaTemplate kafkaProducerMock;

	@BeforeEach
	void setUp() {
		kafkaProducerMock = mock(KafkaTemplate.class);
		service = new DispatchService(kafkaProducerMock);
	}

	@Test
	void process_Success() throws Exception {
		final var key = randomUUID().toString();
		final var testEvent = buildOrderCreatedEvent(randomUUID(), "car");
		when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(
				mock(CompletableFuture.class));
		when(kafkaProducerMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(
				mock(CompletableFuture.class));

		service.process(key, testEvent);

		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
		verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
	}


	@Test
	void process_ProducerThrowsException() {
		final var key = randomUUID().toString();
		when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(
				mock(CompletableFuture.class));
		doThrow(new RuntimeException("order dispatched producer failure")).when(kafkaProducerMock)
				.send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

		final Exception exception = assertThrows(RuntimeException.class,
				() -> service.process(key, buildOrderCreatedEvent(randomUUID(), "car")));

		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
		verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
		assertThat(exception.getMessage(), equalTo("order dispatched producer failure"));
	}

}