package com.training.exceptions;

public class CompanyIdInvalidException extends RuntimeException {
    public CompanyIdInvalidException(String message) {
        super(message);
    }
}
