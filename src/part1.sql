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
    Group_Name varchar(255) CHECK (Group_Name ~ '^[A-Za-zА-Яа-я0-9\s\WёЁ]+$')
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
    SKU_Purchase_Price   DECIMAL(10, 2),
    SKU_Retail_Price     DECIMAL(10, 2),
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);

CREATE TABLE IF NOT EXISTS Transactions
(
    Transaction_ID       SERIAL PRIMARY KEY,
    Customer_Card_ID     INT,
    Transaction_Summ     DECIMAL(10, 2),
    Transaction_DateTime TIMESTAMP,
    Transaction_Store_ID INT,
    FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID)
 );

CREATE TABLE IF NOT EXISTS Checks
(
    Transaction_ID INT,
    SKU_ID         INT,
    SKU_Amount     DECIMAL(10, 2),
    SKU_Summ       DECIMAL(10, 2),
    SKU_Summ_Paid  DECIMAL(10, 2),
    SKU_Discount   DECIMAL(10, 2),
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions (Transaction_ID),
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);


CREATE TABLE IF NOT EXISTS Analysis_Date
(
    Analysis_Formation TIMESTAMP
);


-- Процедура импорта данных в таблицу
CREATE OR REPLACE FUNCTION import_from_csv(
    tablename text,
    filename text,
    delimiter text DEFAULT ','
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
    seq_name text;
    max_id   bigint;
BEGIN
    SET datestyle TO 'ISO, DMY';
    EXECUTE format('COPY %I FROM %L WITH CSV DELIMITER %L', tablename, filename, delimiter);
    IF EXISTS (SELECT 1
               FROM information_schema.columns
               WHERE table_name = tablename
                 AND column_name = 'id') THEN
        SELECT pg_get_serial_sequence(tablename, 'id') INTO seq_name;
        EXECUTE format('SELECT MAX(id) FROM %I', tablename) INTO max_id;
        IF max_id IS NOT NULL THEN
            EXECUTE format('SELECT setval(%L, %s)', seq_name, max_id);
        END IF;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION import_from_tsv(
    table_name text,
    filename text
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    PERFORM import_from_csv(table_name, filename, E'\t');
END;
$$;

CREATE OR REPLACE FUNCTION export_to_csv(
    table_name text,
    filename text,
    delimiter char DEFAULT ','
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE format(
            'COPY %I TO %L WITH CSV DELIMITER %L',
            table_name,
            filename,
            delimiter
        );
END;
$$;

CREATE OR REPLACE FUNCTION export_to_tsv(
    table_name text,
    filename text
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    PERFORM export_to_csv(table_name, filename, E'\t');
END;
$$;

-- Заполнения таблиц командами импорта из таблиц:
DO
$$
    DECLARE
        path_dir text;
    BEGIN
        path_dir := '/Users/amazomic/SQL3_RetailAnalitycs_v1.0-1/datasets/';
        -- поменять на свой путь

--         Очищаем другие таблицы перед импортом
        TRUNCATE TABLE personal_data CASCADE;
        TRUNCATE TABLE Cards CASCADE;
        TRUNCATE TABLE Stores CASCADE;
        TRUNCATE TABLE Transactions CASCADE;
        TRUNCATE TABLE Checks CASCADE;
        TRUNCATE TABLE Groups_SKU CASCADE;
        TRUNCATE TABLE SKU CASCADE;


-- TRUNCATE TABLE Analysis_Date CASCADE;

        PERFORM import_from_tsv(
                'personal_data',
                path_dir || 'Personal_Data.tsv'
            );
--
        PERFORM import_from_tsv(
                'cards',
                path_dir || 'Cards.tsv'
            );

        PERFORM import_from_tsv(
                'groups_sku',
                path_dir || 'Groups_SKU.tsv'
            );

        PERFORM import_from_tsv(
                'sku',
                path_dir || 'SKU.tsv'
            );

        PERFORM import_from_tsv(
                'stores',
                path_dir || 'Stores.tsv'
            );

        PERFORM import_from_tsv(
                'transactions',
                path_dir || 'Transactions.tsv'
            );

        PERFORM import_from_tsv(
                'checks',
                path_dir || 'Checks.tsv'
            );


        PERFORM import_from_tsv(
                'analysis_date',
                path_dir || 'Date_Of_Analysis_Formation.tsv'
            );

    END
$$;


DO
$$
    DECLARE
        path_dir text;
    BEGIN
        path_dir := '/Users/amazomic/SQL3_RetailAnalitycs_v1.0-1/datasets/';
        -- поменять на свой путь

--         Очищаем другие таблицы перед импортом
        TRUNCATE TABLE personal_data CASCADE;
        TRUNCATE TABLE Cards CASCADE;
        TRUNCATE TABLE Stores CASCADE;
        TRUNCATE TABLE Transactions CASCADE;
        TRUNCATE TABLE Checks CASCADE;
        TRUNCATE TABLE Groups_SKU CASCADE;
        TRUNCATE TABLE SKU CASCADE;


-- TRUNCATE TABLE Analysis_Date CASCADE;

        PERFORM import_from_tsv(
                'personal_data',
                path_dir || 'Personal_Data_Mini.tsv'
            );
--
        PERFORM import_from_tsv(
                'cards',
                path_dir || 'Cards_Mini.tsv'
            );

        PERFORM import_from_tsv(
                'groups_sku',
                path_dir || 'Groups_SKU_Mini.tsv'
            );

        PERFORM import_from_tsv(
                'sku',
                path_dir || 'SKU_Mini.tsv'
            );

        PERFORM import_from_tsv(
                'stores',
                path_dir || 'Stores_Mini.tsv'
            );

        PERFORM import_from_tsv(
                'transactions',
                path_dir || 'Transactions_Mini.tsv'
            );

        PERFORM import_from_tsv(
                'checks',
                path_dir || 'Checks_Mini.tsv'
            );


        PERFORM import_from_tsv(
                'analysis_date',
                path_dir || 'Date_Of_Analysis_Formation.tsv'
            );

    END
$$;


DO
$$
    DECLARE
        path_dir text;
    BEGIN
        path_dir := '/Users/amazomic/SQL3_RetailAnalitycs_v1.0-1/datasets/';
        -- поменять на свой путь

        -- Очищаем другие таблицы перед импортом
        TRUNCATE TABLE personal_data CASCADE;
        TRUNCATE TABLE Cards CASCADE;
        TRUNCATE TABLE Stores CASCADE;
        TRUNCATE TABLE Transactions CASCADE;
        TRUNCATE TABLE Checks CASCADE;
        TRUNCATE TABLE Groups_SKU CASCADE;
        TRUNCATE TABLE SKU CASCADE;

-- TRUNCATE TABLE Analysis_Date CASCADE;

        PERFORM export_to_tsv(
                'personal_data',
                path_dir || 'Personal_Data.tsv'
            );
--
        PERFORM export_to_tsv(
                'cards',
                path_dir || 'Cards.tsv'
            );

        PERFORM export_to_tsv(
                'groups_sku',
                path_dir || 'Groups_SKU.tsv'
            );

        PERFORM export_to_tsv(
                'sku',
                path_dir || 'SKU.tsv'
            );

        PERFORM export_to_tsv(
                'stores',
                path_dir || 'Stores.tsv'
            );

        PERFORM export_to_tsv(
                'transactions',
                path_dir || 'Transactions.tsv'
            );

        PERFORM export_to_tsv(
                'checks',
                path_dir || 'Checks.tsv'
            );

        PERFORM export_to_tsv(
                'analysis_date',
                path_dir || 'Date_Of_Analysis_Formation.tsv'
            );

    END
$$;