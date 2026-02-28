package com.itsaur.exceptions;

public class CompanyVatRequiredException extends RuntimeException {
    public CompanyVatRequiredException(String message) {
        super(message);
    }
}
