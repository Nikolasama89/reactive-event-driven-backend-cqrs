package com.training.exceptions;

public class CompanyNameInvalidCharacterLengthException extends RuntimeException {
    public CompanyNameInvalidCharacterLengthException(String message) {
        super(message);
    }
}
