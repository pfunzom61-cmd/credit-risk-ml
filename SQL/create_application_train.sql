
#Create application_train staging table in the database and successfully insert first batch into the database. 
CREATE TABLE application_train (
    sk_id_curr BIGINT PRIMARY KEY,
    target SMALLINT NOT NULL,
    amt_income_total NUMERIC(12,2),
    name_contract_type VARCHAR(50),
    code_gender VARCHAR(3)
);
SELECT COUNT(*) FROM application_train;

