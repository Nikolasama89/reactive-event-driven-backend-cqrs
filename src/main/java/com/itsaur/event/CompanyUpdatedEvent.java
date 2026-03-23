package com.itsaur.event;

public class CompanyUpdatedEvent implements DomainEvent{
    private Long companyId;

    public CompanyUpdatedEvent(Long companyId) {
        this.companyId = companyId;
    }

    public Long getCompanyId() {
        return companyId;
    }
}
