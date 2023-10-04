-- Создаем базу данных
CREATE DATABASE school21;

-- Подключаемся к базе данных
\c school21

-- Создаем таблицу Peers
CREATE TABLE IF NOT EXISTS Personal_Data
(
    Customer_ID            SERIAL PRIMARY KEY,
    Customer_Name          VARCHAR CHECK (Customer_Name ~ '^[А-ЯЁA-Z][а-яёa-z\s\-]*$'),
    Customer_Surname       VARCHAR CHECK (Customer_Surname ~ '^[А-ЯЁA-Z][а-яёa-z\s\-]*$'),
    Customer_Primary_Email VARCHAR CHECK (Customer_Primary_Email ~
                                          '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,3}$'),
    Customer_Primary_Phone VARCHAR CHECK (Customer_Primary_Phone ~ '^\+7\d{10}$')
);

CREATE TABLE IF NOT EXISTS Cards
(
    Customer_Card_ID SERIAL PRIMARY KEY,
    Customer_ID      INT,
    FOREIGN KEY (Customer_ID) REFERENCES Personal_Data (Customer_ID)
);

CREATE TABLE IF NOT EXISTS Groups_SKU
(
    Group_ID   SERIAL PRIMARY KEY,
    Group_Name VARCHAR CHECK (Group_Name ~ '^[A-Za-zА-Яа-я0-9\s\WёЁ]+$')
);

CREATE TABLE IF NOT EXISTS SKU -- Product grid Table (Товарная матрица)
(
    SKU_ID   SERIAL PRIMARY KEY,
    SKU_Name VARCHAR CHECK (SKU_Name ~ '^[A-Za-zА-Яа-я0-9\s\WёЁ]+$'),
    Group_ID INT,
    FOREIGN KEY (Group_ID) REFERENCES Groups_SKU (Group_ID)
);

CREATE TABLE IF NOT EXISTS Stores
(
    Transaction_Store_ID INT,
    SKU_ID               INT,
    SKU_Purchase_Price   DECIMAL,
    SKU_Retail_Price     DECIMAL,
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);

CREATE TABLE IF NOT EXISTS Transactions
(
    Transaction_ID       SERIAL PRIMARY KEY,
    Customer_Card_ID     INT,
    Transaction_Summ     DECIMAL,
    Transaction_DateTime TIMESTAMP,
    Transaction_Store_ID INT,
    FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID)
);

CREATE TABLE IF NOT EXISTS Checks
(
    Transaction_ID INT,
    SKU_ID         INT,
    SKU_Amount     DECIMAL,
    SKU_Summ       DECIMAL,
    SKU_Summ_Paid  DECIMAL,
    SKU_Discount   DECIMAL,
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions (Transaction_ID),
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);


CREATE TABLE IF NOT EXISTS Analysis_Date
(
    Analysis_Formation TIMESTAMP
);


-- Процедура импорта данных в таблицу
CREATE OR REPLACE PROCEDURE import_from_csv(
    IN table_name TEXT,
    IN filename TEXT,
    IN delimiter TEXT
)
    LANGUAGE plpgsql
AS $$
BEGIN
    SET datestyle TO 'ISO, DMY';
    EXECUTE format('COPY %I FROM %L WITH CSV DELIMITER %L', table_name, filename, delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE import_from_tsv(
    IN table_name text,
    IN filename text
)
    LANGUAGE plpgsql
AS $$
BEGIN
    CALL import_from_csv(table_name, filename , E'\t');
END;
$$;


CREATE OR REPLACE PROCEDURE export_to_csv(
    IN table_name text,
    IN filename text,
    IN delimiter text
)
    LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('COPY %I TO %L WITH CSV DELIMITER %L', table_name, filename, delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_to_tsv(
    IN table_name text,
    IN filename text
)
    LANGUAGE plpgsql
AS $$
BEGIN
    CALL export_to_csv(table_name, filename , E'\t');
END;
$$;



CREATE OR REPLACE PROCEDURE import_datasets()
    LANGUAGE plpgsql
AS $$
DECLARE
    path_dir text;
BEGIN
    path_dir := '/mnt/c/Users/sbevz/Documents/git/SQL3_RetailAnalitycs_v1.0-2/datasets/';
--     path_dir := '/Users/amazomic/SQL3_RetailAnalitycs_v1.0-1/datasets/';

    TRUNCATE TABLE personal_data CASCADE;
    TRUNCATE TABLE cards CASCADE;
    TRUNCATE TABLE groups_sku CASCADE;
    TRUNCATE TABLE sku CASCADE;
    TRUNCATE TABLE stores CASCADE;
    TRUNCATE TABLE transactions CASCADE;
    TRUNCATE TABLE checks CASCADE;
    TRUNCATE TABLE analysis_date CASCADE;

    BEGIN
        CALL import_from_tsv('personal_data', path_dir || 'Personal_Data.tsv');
        CALL import_from_tsv('cards', path_dir || 'Cards.tsv');
        CALL import_from_tsv('groups_sku', path_dir || 'Groups_SKU.tsv');
        CALL import_from_tsv('sku', path_dir || 'SKU.tsv');
        CALL import_from_tsv('stores', path_dir || 'Stores.tsv');
        CALL import_from_tsv('transactions', path_dir || 'Transactions.tsv');
        CALL import_from_tsv('checks', path_dir || 'Checks.tsv');
        CALL import_from_tsv('analysis_date', path_dir || 'Date_Of_Analysis_Formation.tsv');
    END;
END;
$$;

CREATE OR REPLACE PROCEDURE import_datasets_mini()
    LANGUAGE plpgsql
AS $$
DECLARE
    path_dir text;
BEGIN
    path_dir := '/mnt/c/Users/sbevz/Documents/git/SQL3_RetailAnalitycs_v1.0-2/datasets/';
--     path_dir := '/Users/amazomic/SQL3_RetailAnalitycs_v1.0-2/datasets/';

    TRUNCATE TABLE personal_data CASCADE;
    TRUNCATE TABLE cards CASCADE;
    TRUNCATE TABLE groups_sku CASCADE;
    TRUNCATE TABLE sku CASCADE;
    TRUNCATE TABLE stores CASCADE;
    TRUNCATE TABLE transactions CASCADE;
    TRUNCATE TABLE checks CASCADE;
    TRUNCATE TABLE analysis_date CASCADE;

    BEGIN
        CALL import_from_tsv('personal_data', path_dir || 'Personal_Data_Mini.tsv');
        CALL import_from_tsv('cards', path_dir || 'Cards_Mini.tsv');
        CALL import_from_tsv('groups_sku', path_dir || 'Groups_SKU_Mini.tsv');
        CALL import_from_tsv('sku', path_dir || 'SKU_Mini.tsv');
        CALL import_from_tsv('stores', path_dir || 'Stores_Mini.tsv');
        CALL import_from_tsv('transactions', path_dir || 'Transactions_Mini.tsv');
        CALL import_from_tsv('checks', path_dir || 'Checks_Mini.tsv');
        CALL import_from_tsv('analysis_date', path_dir || 'Date_Of_Analysis_Formation.tsv');
    END;
END;
$$;


CREATE OR REPLACE PROCEDURE export_datasets_mini()
    LANGUAGE plpgsql
AS $$
DECLARE
    path_dir text;
BEGIN
    path_dir := '/Users/amazomic/SQL3_RetailAnalitycs_v1.0-2/src/';
    BEGIN
        CALL export_to_tsv('personal_data', path_dir || 'Personal_Data_Mini.tsv');
        CALL export_to_tsv('cards', path_dir || 'Cards_Mini.tsv');
        CALL export_to_tsv('groups_sku', path_dir || 'Groups_SKU_Mini.tsv');
        CALL export_to_tsv('sku', path_dir || 'SKU_Mini.tsv');
        CALL export_to_tsv('stores', path_dir || 'Stores_Mini.tsv');
        CALL export_to_tsv('transactions', path_dir || 'Transactions_Mini.tsv');
        CALL export_to_tsv('checks', path_dir || 'Checks_Mini.tsv');
        CALL export_to_tsv('analysis_date', path_dir || 'Date_Of_Analysis_Formation.tsv');
    END;
END;
$$;


CALL import_datasets_mini();
CALL import_datasets();
CALL export_datasets_mini();


