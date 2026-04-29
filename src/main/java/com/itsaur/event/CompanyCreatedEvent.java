package com.itsaur.event;

import com.itsaur.model.Address;
import com.itsaur.model.CompanyId;
import com.itsaur.model.CompanyName;
import com.itsaur.model.Vat;

import java.time.OffsetDateTime;

public class CompanyCreatedEvent implements DomainEvent {
    private final CompanyId companyId;
    private final CompanyName companyName;
    private final Vat vat;
    private final Address address;
    private final OffsetDateTime occurredAt;

    public CompanyCreatedEvent(
            CompanyId companyId,
            CompanyName companyName,
            Vat vat,
            Address address,
            OffsetDateTime occurredAt
    ) {
        this.companyId = companyId;
        this.companyName = companyName;
        this.vat = vat;
        this.address = address;
        this.occurredAt = occurredAt;
    }

    public CompanyId getCompanyId() {
        return companyId;
    }

    public CompanyName getCompanyName() {
        return companyName;
    }

    public Vat getVat() {
        return vat;
    }

    public Address getAddress() {
        return address;
    }

    public OffsetDateTime getOccurredAt() {
        return occurredAt;
    }
}