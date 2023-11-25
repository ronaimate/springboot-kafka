package com.ronaimate.dispatch.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ronaimate.dispatch.service.DispatchService;

import static com.ronaimate.dispatch.util.TestEventData.buildOrderCreatedEvent;
import static java.util.UUID.randomUUID;
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
		final var testEvent = buildOrderCreatedEvent(randomUUID(), "car");

		handler.listen(testEvent);

		verify(dispatchServiceMock, times(1)).process(testEvent);
	}

	@Test
	void listen_ServiceThrowsException() throws Exception {
		final var testEvent = buildOrderCreatedEvent(randomUUID(), "car");
		doThrow(new RuntimeException("Service failure")).when(dispatchServiceMock).process(testEvent);

		handler.listen(testEvent);

		verify(dispatchServiceMock, times(1)).process(testEvent);
	}

}