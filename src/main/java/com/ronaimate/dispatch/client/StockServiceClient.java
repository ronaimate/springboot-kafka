package com.ronaimate.dispatch.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.ronaimate.exception.RetryableException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class StockServiceClient {

	private final RestTemplate restTemplate;

	@Value("${dispatch.stockServiceEndpoint}")
	private final String stockServiceEndpoint;

	public StockServiceClient(RestTemplate restTemplate,
			@Value("${dispatch.stockServiceEndpoint}") String stockServiceEndpoint) {
		this.restTemplate = restTemplate;
		this.stockServiceEndpoint = stockServiceEndpoint;
	}

	public String checkAvailability(final String item) {
		try {
			ResponseEntity<String> response =
					restTemplate.getForEntity(stockServiceEndpoint + "?item=" + item, String.class);
			if (response.getStatusCode() != HttpStatusCode.valueOf(200)) {
				throw new RuntimeException("error " + response.getStatusCode());
			}
			return response.getBody();
		} catch (HttpServerErrorException e) {
			log.error("Server exception error code: {}", e.getStatusCode(), e);
			throw new RetryableException(e);
		} catch (ResourceAccessException e) {
			log.error("Resource access exception.", e);
			throw new RetryableException(e);
		} catch (Exception e) {
			log.error("Exception thrown: {}", e.getClass().getName(), e);
			throw e;
		}
	}

}
