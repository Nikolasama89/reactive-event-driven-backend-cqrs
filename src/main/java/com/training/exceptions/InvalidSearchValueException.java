package com.training.exceptions;

public class InvalidSearchValueException extends RuntimeException {
    public InvalidSearchValueException(String message) {
        super(message);
    }
}
