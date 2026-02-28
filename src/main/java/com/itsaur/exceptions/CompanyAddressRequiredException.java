package com.itsaur.exceptions;

public class CompanyAddressRequiredException extends RuntimeException {
    public CompanyAddressRequiredException(String message) {
        super(message);
    }
}
