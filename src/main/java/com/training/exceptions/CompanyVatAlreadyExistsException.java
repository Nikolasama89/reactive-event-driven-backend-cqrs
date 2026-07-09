package com.training.exceptions;

public class CompanyVatAlreadyExistsException extends RuntimeException {
    public CompanyVatAlreadyExistsException(String message) {
        super(message);
    }
}
