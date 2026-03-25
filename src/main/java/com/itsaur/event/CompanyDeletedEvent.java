package com.itsaur.event;

public class CompanyDeletedEvent implements DomainEvent{
    private final Long companyId;

    public CompanyDeletedEvent(Long companyId) {
        this.companyId = companyId;
    }

    public Long getCompanyId() {
        return companyId;
    }
}
