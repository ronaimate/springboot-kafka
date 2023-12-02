package com.ronaimate.exception;

public class RetryableException extends RuntimeException {

	public RetryableException(String message) {
		super(message);
	}

	public RetryableException(Exception exception) {
		super(exception);
	}

}