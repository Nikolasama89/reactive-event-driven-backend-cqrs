package com.training.exceptions;

public class CompanyNameRequiredException extends RuntimeException {
    public CompanyNameRequiredException(String message) {
        super(message);
    }
}
