package com.itsaur.elastic.repository;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.itsaur.repository.search.SearchComponent;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.repository.CompanyQueryRepository;
import com.itsaur.elastic.search.ElasticSearchCompanyQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ElasticCompanyQueryRepository implements CompanyQueryRepository {

    private final Logger logger = LoggerFactory.getLogger(ElasticCompanyQueryRepository.class );
    private final ElasticsearchAsyncClient client;
    private final ElasticSearchCompanyQueryBuilder queryBuilder;

    public ElasticCompanyQueryRepository(ElasticsearchAsyncClient client, ElasticSearchCompanyQueryBuilder queryBuilder) {
        this.client = client;
        this.queryBuilder = queryBuilder;
    }

    @Override
    public Mono<CompanyQueryModel> getCompanyById(Long id) {
        GetRequest request = GetRequest.of(g -> g
                .index("companies")
                .id(String.valueOf(id)));

        return Mono.fromFuture(() -> client.get(request, CompanyQueryModel.class))
                .filter(GetResult::found)
                .map(GetResult::source);
    }

    @Override
    public Flux<CompanyQueryModel> getAllCompanies() {
        SearchRequest request = SearchRequest.of(s -> s
                .index("companies")
                .query(q -> q.matchAll(m -> m)));
        return Mono.fromFuture(() -> client
                        .search(request, CompanyQueryModel.class))
                .flatMapMany(resp -> Flux.fromIterable(resp.hits().hits()))
                .map(Hit::source);
    }

    @Override
    public Flux<CompanyQueryModel> findPage(Integer page, Integer size) {
        int offset = page * size;
        SearchRequest request = SearchRequest.of(s -> s.index("companies").from(offset).size(size));

        return Mono.fromFuture(() -> client
                        .search(request, CompanyQueryModel.class))
                .flatMapMany(resp ->
                        Flux.fromIterable(resp.hits().hits()))
                .map(Hit::source);
    }

    @Override
    public Mono<Long> countAll() {
        return Mono.fromFuture(() -> client.count(c -> c
                        .index("companies")
                        .query(q -> q.matchAll(m -> m))))
                .map(CountResponse::count);
    }

    @Override
    public Flux<CompanyQueryModel> paginatedCompanyFilterQuery(SearchComponent searchComponent, Integer page, Integer size) {
        int offset = page * size;
        Query query = queryBuilder.build(searchComponent);

        SearchRequest request = SearchRequest.of(s -> s.index("companies")
                .from(offset)
                .size(size)
                .query(query)
        );

        return Mono.fromFuture(() -> client.search(request, CompanyQueryModel.class))
                .flatMapMany(resp -> Flux.fromIterable(resp.hits().hits()))
                .map(Hit::source);
    }

    @Override
    public Mono<Long> countViewCompanies(SearchComponent searchComponent) {
        Query query = queryBuilder.build(searchComponent);

        CountRequest request = CountRequest.of(c -> c
                .index("companies")
                .query(query)
        );

        return Mono.fromFuture(() -> client.count(request))
                .map(CountResponse::count);
    }
}