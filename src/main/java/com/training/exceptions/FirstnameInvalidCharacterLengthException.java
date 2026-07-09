package com.training.exceptions;

public class FirstnameInvalidCharacterLengthException extends RuntimeException {
    public FirstnameInvalidCharacterLengthException(String message) {
        super(message);
    }
}
