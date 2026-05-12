package com.itsaur.event;


import com.itsaur.model.*;

import java.time.OffsetDateTime;

public class EmployeeUpdatedEvent implements DomainEvent{
    private final EmployeeId employeeId;
    private final CompanyId companyId;
    private final FullName fullName;
    private final Email email;
    private final CompanyName companyName;
    private final Address companyAddress;
    private final OffsetDateTime occurredAt;

    public EmployeeUpdatedEvent(
            EmployeeId employeeId,
            CompanyId companyId,
            FullName fullName,
            Email email,
            CompanyName companyName,
            Address companyAddress,
            OffsetDateTime occurredAt
    ) {
        this.employeeId = employeeId;
        this.companyId = companyId;
        this.fullName = fullName;
        this.email = email;
        this.companyName = companyName;
        this.companyAddress = companyAddress;
        this.occurredAt = occurredAt;
    }

    public EmployeeId getEmployeeId() {
        return employeeId;
    }

    public CompanyId getCompanyId() {
        return companyId;
    }

    public FullName getFullName(){ return fullName;}

    public Email getEmail() {
        return email;
    }

    public CompanyName getCompanyName() {
        return companyName;
    }

    public Address getCompanyAddress() {
        return companyAddress;
    }

    public OffsetDateTime getOccurredAt() {
        return occurredAt;
    }

    @Override
    public String aggregateId() {
        return employeeId.toString();
    }
}
