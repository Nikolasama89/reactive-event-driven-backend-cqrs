package com.itsaur.repository;

import com.itsaur.model.Company;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public class DBCompanyRepository implements CompanyRepository {
    private final ConnectionFactory cf;

    public DBCompanyRepository(ConnectionFactory cf) {
        this.cf = cf;
    }

    @Override
    public Mono<Company> getCompanyById(Long id) {
        String sql = "SELECT id, company_name, vat, address, created_at, updated_at FROM companies WHERE id = $1";
        return Mono.usingWhen(Mono.from(cf.create()),
                conn -> doFindOne(conn, sql, id), Connection::close);
    }

    private Mono<Company> doFindOne(Connection conn, String sql, Long id) {

        return Mono.from(conn.createStatement(sql)
                .bind(0, id)
                .execute())
                .flatMap(result -> Mono.from(result.map((row, meta) ->
                                new Company(
                                        row.get("id", Long.class),
                                        row.get("company_name", String.class),
                                        row.get("vat", String.class),
                                        row.get("address", String.class),
                                        row.get("created_at", OffsetDateTime.class),
                                        row.get("updated_at", OffsetDateTime.class)
                                )
                        ))
                );
    }

    @Override
    public Mono<Company> createOrUpdateCompany(Long id, Company company) {
        if (id == null) {
            String sql = "INSERT INTO companies (company_name, vat, address) VALUES ($1, $2, $3) RETURNING id, company_name, vat, address";
            return Mono.usingWhen(Mono.from(cf.create()), conn -> doCreateCompany(conn, sql, company), Connection::close);
        } else {
            String sql = "UPDATE companies SET company_name = $1, vat = $2, address = $3, updated_at = NOW() WHERE id = $4 RETURNING id, company_name, vat, address, created_at, updated_at";
            return Mono.usingWhen(Mono.from(cf.create()), conn -> Mono.from(conn.createStatement(sql)
                            .bind(0, company.getCompanyName())
                            .bind(1, company.getVat())
                            .bind(2, company.getAddress())
                            .bind(3, id)
                            .execute()).flatMap(result -> Mono.from(result.map((row, meta) -> {

                                Company c = new Company();
                                c.setId(row.get("id", Long.class));
                                c.setCompanyName(row.get("company_name", String.class));
                                c.setVat(row.get("vat", String.class));
                                c.setAddress(row.get("address", String.class));
                                c.setCreatedAt(row.get("created_at", OffsetDateTime.class));
                                c.setUpdatedAt(row.get("updated_at", OffsetDateTime.class));
                                return c;
                            }))
                    ), Connection::close
            );
        }
    }

    private Mono<Company> doCreateCompany(Connection conn, String sql, Company company) {
        return Mono.from(
                        conn.createStatement(sql)
                                .bind(0, company.getCompanyName())
                                .bind(1, company.getVat())
                                .bind(2, company.getAddress())
                                .execute())
                .flatMap(result -> Mono.from(result.map((row, meta) -> {
                    Company c = new Company();
                    c.setId(row.get("id", Long.class));
                    c.setCompanyName(row.get("company_name", String.class));
                    c.setVat(row.get("vat", String.class));
                    c.setAddress(row.get("address", String.class));
                    return c;
                })));
    }


    @Override
    public Mono<Void> deleteCompany(Long id) {
        String sql = "DELETE FROM companies WHERE id = $1";
        return Mono.usingWhen(cf.create(), conn -> Mono.from(conn.createStatement(sql)
                .bind(0, id).execute())
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .map(rows -> rows != null && rows > 0)
                        .then(), Connection::close);
    }

    @Override
    public Mono<Boolean> existsVat(String vat) {
        String sql = "SELECT vat FROM companies WHERE vat = $1";
        return Mono.usingWhen(cf.create(), conn ->
                        Mono.from(conn.createStatement(sql).bind(0, vat).execute()).flatMap(
                                result -> Mono.from(result.map((row, meta) -> row.get(0, String.class)))
                                        .hasElement()),
                Connection::close);
    }
}
