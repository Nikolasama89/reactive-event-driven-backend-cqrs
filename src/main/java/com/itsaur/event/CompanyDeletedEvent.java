package com.itsaur.event;

import com.itsaur.model.CompanyId;

public class CompanyDeletedEvent implements DomainEvent{
    private final CompanyId companyId;

    public CompanyDeletedEvent(CompanyId companyId) {
        this.companyId = companyId;
    }

    public CompanyId getCompanyId() {
        return companyId;
    }
}
