package com.itsaur.event;

public class CompanyCreatedEvent implements DomainEvent{
    private final Long companyId;

    public CompanyCreatedEvent(Long companyId) {
        this.companyId = companyId;
    }

    public Long getCompanyId() {
        return companyId;
    }
}
