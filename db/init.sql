CREATE TABLE IF NOT EXISTS companies(
    id BIGSERIAL PRIMARY KEY NOT NULL,
    company_name VARCHAR(50) NOT NULL,
    vat VARCHAR(10) NOT NULL,
    address VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

CREATE TABLE IF NOT EXISTS employees(
    id BIGSERIAL PRIMARY KEY NOT NULL,
    firstname VARCHAR(50) NOT NULL,
    lastname VARCHAR(50) NOT NULL,
    email VARCHAR(50) UNIQUE NOT NULL,
    company_id BIGINT REFERENCES companies(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

CREATE OR REPLACE VIEW employee_view AS
SELECT
    e.id,
    e.firstname,
    e.lastname,
    e.email,
    e.company_id,
    c.company_name,
    c.address,
    e.created_at,
    e.updated_at
FROM employees e
INNER JOIN companies c ON e.company_id = c.id;

CREATE OR REPLACE VIEW company_view AS
SELECT
  c.id,
  c.company_name,
  c.vat,
  c.address,
  c.created_at,
  c.updated_at,
  COUNT(e.id) AS total_employees
FROM companies c
LEFT JOIN employees e ON e.company_id = c.id
GROUP BY c.id, c.company_name, c.vat, c.address, c.created_at, c.updated_at;

CREATE OR REPLACE VIEW company_search_view AS
SELECT
  cv.id,
  cv.company_name,
  cv.vat,
  cv.address,
  cv.created_at,
  cv.updated_at,
  cv.total_employees,
  e.firstname,
  e.lastname
FROM company_view cv
LEFT JOIN employees e ON e.company_id = cv.id;