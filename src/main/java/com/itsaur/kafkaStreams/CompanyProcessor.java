package com.itsaur.kafkaStreams;

import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.DomainEvent;
import com.itsaur.model.Company;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class CompanyProcessor implements Processor<String, DomainEvent, Void, Void> {

    private KeyValueStore<String, Company> companiesStore;
    private KeyValueStore<String, String> vatCompaniesStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.companiesStore = context.getStateStore(CompanyTopologyBuilder.COMPANIES_STORE);
        this.vatCompaniesStore = context.getStateStore(CompanyTopologyBuilder.VAT_COMPANIES_STORE);
    }

    @Override
    public void process(Record<String, DomainEvent> record) {

        String companyId = record.key();
        DomainEvent event = record.value();

        if (companyId == null || event == null) return;

        Company currentCompany = companiesStore.get(companyId);
        String oldVat = (currentCompany != null) ? currentCompany.getVat().getValue() : null;

        Company nextStateCompany = applyEvent(currentCompany, event);

        if (nextStateCompany == null){
            companiesStore.delete(companyId);
        } else {
            companiesStore.put(companyId, nextStateCompany);
        }

        String newVat = (nextStateCompany != null) ? nextStateCompany.getVat().getValue() : null;

        if (oldVat != null && !oldVat.equals(newVat)) {
            vatCompaniesStore.delete(oldVat);
        }

        if (newVat != null) {
            vatCompaniesStore.put(newVat, companyId);
        }
    }

    private Company applyEvent(Company current, DomainEvent event) {
        if (event instanceof CompanyCreatedEvent e) {
            if (current != null) return current;
            return Company.construct(
                    e.getCompanyId().getValue(),
                    e.getCompanyName().getValue(),
                    e.getVat().getValue(),
                    e.getAddress().getValue(),
                    e.getOccurredAt(),
                    e.getOccurredAt()
            );
        }
        if (event instanceof CompanyUpdatedEvent e) {
            if (current == null) return null;
            return current.update(
                    e.getCompanyName().getValue(),
                    e.getVat().getValue(),
                    e.getAddress().getValue()
            );
        }
        if (event instanceof CompanyDeletedEvent) {
            return null;
        }
        return current;
    }

    @Override
    public void close() {}
}
