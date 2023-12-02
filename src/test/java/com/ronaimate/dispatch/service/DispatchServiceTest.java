package com.ronaimate.dispatch.service;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import com.ronaimate.dispatch.client.StockServiceClient;
import com.ronaimate.dispatch.messages.DispatchCompleted;
import com.ronaimate.dispatch.messages.DispatchPreparing;
import com.ronaimate.dispatch.messages.OrderDispatched;
import com.ronaimate.dispatch.util.TestEventData;

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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DispatchServiceTest {

	private DispatchService service;

	private KafkaTemplate kafkaProducerMock;

	private StockServiceClient stockServiceClientMock;

	@BeforeEach
	void setUp() {
		kafkaProducerMock = mock(KafkaTemplate.class);
		stockServiceClientMock = mock(StockServiceClient.class);
		service = new DispatchService(kafkaProducerMock, stockServiceClientMock);
	}

	@Test
	void process_Success() throws Exception {
		final var key = randomUUID().toString();
		final var testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
		when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(
				mock(CompletableFuture.class));
		when(kafkaProducerMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(
				mock(CompletableFuture.class));
		when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchCompleted.class))).thenReturn(
				mock(CompletableFuture.class));
		when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");

		service.process(key, testEvent);

		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
		verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
		verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.item());
	}


	@Test
	void testProcess_StockUnavailable() throws Exception {
		final var key = randomUUID().toString();
		final var testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
		when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("false");

		service.process(key, testEvent);

		verifyNoInteractions(kafkaProducerMock);
		verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.item());
	}

	@Test
	void testProcess_DispatchTrackingProducerThrowsException() {
		final var key = randomUUID().toString();
		final var testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
		when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
		doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaProducerMock)
				.send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));

		final Exception exception = assertThrows(RuntimeException.class, () -> service.process(key, testEvent));

		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
		verifyNoMoreInteractions(kafkaProducerMock);
		verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.item());
		assertThat(exception.getMessage(), equalTo("dispatch tracking producer failure"));
	}

	@Test
	void testProcess_OrderDispatchedProducerThrowsException() {
		final var key = randomUUID().toString();
		final var testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
		when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(
				mock(CompletableFuture.class));
		when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
		doThrow(new RuntimeException("order dispatched producer failure")).when(kafkaProducerMock)
				.send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

		final Exception exception = assertThrows(RuntimeException.class, () -> service.process(key, testEvent));

		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
		verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
		verifyNoMoreInteractions(kafkaProducerMock);
		verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.item());
		assertThat(exception.getMessage(), equalTo("order dispatched producer failure"));
	}

	@Test
	void testProcess_SecondDispatchTrackingProducerThrowsException() {
		final var key = randomUUID().toString();
		final var testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
		when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(
				mock(CompletableFuture.class));
		when(kafkaProducerMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(
				mock(CompletableFuture.class));
		when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
		doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaProducerMock)
				.send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));

		final Exception exception = assertThrows(RuntimeException.class, () -> service.process(key, testEvent));

		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
		verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
		verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
		verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.item());
		assertThat(exception.getMessage(), equalTo("dispatch tracking producer failure"));
	}

	@Test
	void testProcess_StockServiceClient_ThrowsException() {
		final var key = randomUUID().toString();
		final var testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
		doThrow(new RuntimeException("stock service client failure")).when(stockServiceClientMock)
				.checkAvailability(testEvent.item());

		final Exception exception = assertThrows(RuntimeException.class, () -> service.process(key, testEvent));

		assertThat(exception.getMessage(), equalTo("stock service client failure"));
		verifyNoInteractions(kafkaProducerMock);
		verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.item());
	}

}