package com.training.event;

import com.training.model.Address;
import com.training.model.CompanyId;
import com.training.model.CompanyName;
import com.training.model.Vat;

import java.time.OffsetDateTime;

public class CompanyUpdatedEvent implements DomainEvent{
    private final CompanyId companyId;
    private final CompanyName companyName;
    private final Vat vat;
    private final Address address;
    private final OffsetDateTime occurredAt;

    public CompanyUpdatedEvent(
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

    public OffsetDateTime getOccurredAt(){
        return occurredAt;
    }

    @Override
    public String aggregateId() {
        return companyId.toString();
    }
}
