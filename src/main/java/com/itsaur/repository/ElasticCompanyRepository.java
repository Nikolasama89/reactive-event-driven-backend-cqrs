package com.itsaur.repository;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import com.itsaur.model.Company;
import com.itsaur.model.CompanyQueryModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public class ElasticCompanyRepository implements CompanyRepository{

    private final Logger logger = LoggerFactory.getLogger(ElasticCompanyRepository.class );
    private final ElasticsearchAsyncClient client;

    public ElasticCompanyRepository(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Mono<Company> getCompanyById(Long id) {
//        return Mono.fromFuture(() -> client.get(g -> g.index("companies").id(String.valueOf(id)), Company.class)
//        ).flatMap(response -> {
//            if (response.found()){
//                return Mono.just(response.source());
//            }
//            return Mono.empty();
//        });

        return Mono.fromFuture(() ->
                        client.get(g -> g.index("companies").id(String.valueOf(id)), CompanyQueryModel.class)
                )
                .flatMap(response -> {
                    if (response.found()) {
                        CompanyQueryModel qm = response.source();
                        Company company = new Company(
                                qm.getId(),
                                qm.getCompanyName(),
                                qm.getAddress(),
                                qm.getVat(),
                                qm.getCreatedAt(),
                                qm.getUpdatedAt()
                        );
                        return Mono.just(company);
                    } else {
                        return Mono.empty();
                    }
                });
    }

    @Override
    public Mono<Company> createOrUpdateCompany(Long id, Company company) {

        OffsetDateTime now = OffsetDateTime.now();

        if (company.getId() == null) {
            company.setId(System.currentTimeMillis());
        }

        if (company.getCreatedAt() == null) {
            company.setCreatedAt(now);
        }
        company.setUpdatedAt(OffsetDateTime.now());

        CompanyQueryModel qm = new CompanyQueryModel(
                company.getId(),
                company.getCompanyName(),
                company.getVat(),
                company.getAddress(),
                0L, //
                company.getCreatedAt(),
                company.getUpdatedAt()
        );

        return Mono.fromFuture(() -> client.index(i -> i
                .index("companies")
                .id(company.getId().toString())
                .document(qm)
        )).thenReturn(company);
    }

    @Override
    public Mono<Void> deleteCompany(Long id) {
        return Mono.fromFuture(() -> client.delete(i -> i
                        .index("companies").id(String.valueOf(id))))
                .then();
    }

    @Override
    public Mono<Boolean> existsVat(String vat) {

        if (vat == null || vat.isBlank()){
            return Mono.just(false);
        }

        SearchRequest request = SearchRequest.of(s -> s
                .index("companies")
                .size(0)
                .query(q -> q.
                        term(t -> t
                                .field("vat")
                                .value(v -> v
                                        .stringValue(vat))
                        )
                )
        );

        return Mono.fromFuture(client.search(request, Company.class))
                .map(companySearchResponse -> companySearchResponse.hits().total().value() > 0);
    }
}
