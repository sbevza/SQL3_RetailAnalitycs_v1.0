DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'administrator') THEN
            CREATE ROLE administrator;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
            CREATE USER admin WITH PASSWORD 'admin';
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'visitor') THEN
            CREATE ROLE visitor;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tester') THEN
            CREATE USER tester WITH PASSWORD 'tester';
        END IF;
    END $$;


-- запихиваем админа в роль админа
GRANT administrator TO admin;

-- Назначаем администратору полные права на схему public
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO administrator;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO administrator;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO administrator;
GRANT CREATE ON SCHEMA public TO administrator;

---------------------------------------------------
-- запихиваем тестера в роль посетителей
GRANT "visitor" TO tester;

-- Назначаем право на просмотр таблиц для роли visitors
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;
---------------------------------------------------


-- SELECT current_user;
-- SELECT rolname
-- FROM pg_roles; -- просмотреть все роли




