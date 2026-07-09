package com.training.exceptions;

public class FirstnameRequiredException extends RuntimeException {
    public FirstnameRequiredException(String message) {
        super(message);
    }
}
