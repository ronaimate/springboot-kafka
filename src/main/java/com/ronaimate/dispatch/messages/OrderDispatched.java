package com.ronaimate.dispatch.messages;

import java.util.UUID;

import lombok.Builder;

@Builder
public record OrderDispatched(UUID orderId) {

}
