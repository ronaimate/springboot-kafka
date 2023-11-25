package com.ronaimate.dispatch.util;

import java.util.UUID;

import com.ronaimate.dispatch.messages.OrderCreated;

public class TestEventData {

	public static OrderCreated buildOrderCreatedEvent(final UUID orderId, final String item) {
		return OrderCreated.builder()
				.orderId(orderId)
				.item(item)
				.build();
	}

}
