package com.training.exceptions;

public class LastnameRequiredException extends RuntimeException {
    public LastnameRequiredException(String message) {
        super(message);
    }
}
