package com.training.exceptions;

public class EmailInvalidCharacterLengthException extends RuntimeException {
    public EmailInvalidCharacterLengthException(String message) {
        super(message);
    }
}
