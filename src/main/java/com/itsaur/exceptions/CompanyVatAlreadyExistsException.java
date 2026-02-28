package com.itsaur.exceptions;

public class CompanyVatAlreadyExistsException extends RuntimeException {
    public CompanyVatAlreadyExistsException(String message) {
        super(message);
    }
}
