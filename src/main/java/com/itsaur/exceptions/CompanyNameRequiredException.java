package com.itsaur.exceptions;

public class CompanyNameRequiredException extends RuntimeException {
    public CompanyNameRequiredException(String message) {
        super(message);
    }
}
