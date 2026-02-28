package com.itsaur.exceptions;

public class EmailInvalidCharacterLengthException extends RuntimeException {
    public EmailInvalidCharacterLengthException(String message) {
        super(message);
    }
}
