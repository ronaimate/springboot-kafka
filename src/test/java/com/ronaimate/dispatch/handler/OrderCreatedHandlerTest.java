package com.ronaimate.dispatch.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ronaimate.dispatch.service.DispatchService;
import com.ronaimate.dispatch.util.TestEventData;
import com.ronaimate.exception.NotRetryableException;
import com.ronaimate.exception.RetryableException;

import static com.ronaimate.dispatch.util.TestEventData.buildOrderCreatedEvent;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class OrderCreatedHandlerTest {

	private OrderCreatedHandler handler;

	private DispatchService dispatchServiceMock;

	@BeforeEach
	void setUp() {
		dispatchServiceMock = mock(DispatchService.class);
		handler = new OrderCreatedHandler(dispatchServiceMock);
	}

	@Test
	void listen_Success() throws Exception {
		final var key = randomUUID().toString();
		final var testEvent = buildOrderCreatedEvent(randomUUID(), "car");

		handler.listen(0, key, testEvent);

		verify(dispatchServiceMock, times(1)).process(key, testEvent);
	}

	@Test
	void listen_ServiceThrowsException() throws Exception {
		final var key = randomUUID().toString();
		final var testEvent = buildOrderCreatedEvent(randomUUID(), "car");
		doThrow(new RuntimeException("Service failure")).when(dispatchServiceMock).process(key, testEvent);

		final Exception exception = assertThrows(NotRetryableException.class, () -> handler.listen(0, key, testEvent));

		assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: Service failure"));
		verify(dispatchServiceMock, times(1)).process(key, testEvent);
	}

	@Test
	void testListen_ServiceThrowsRetryableException() throws Exception {
		final var key = randomUUID().toString();
		final var testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
		doThrow(new RetryableException("Service failure")).when(dispatchServiceMock).process(key, testEvent);

		Exception exception = assertThrows(RuntimeException.class, () -> handler.listen(0, key, testEvent));

		assertThat(exception.getMessage(), equalTo("Service failure"));
		verify(dispatchServiceMock, times(1)).process(key, testEvent);
	}

}