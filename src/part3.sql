CREATE ROLE administrator;

CREATE USER admin WITH PASSWORD 'admin';
GRANT administrator TO admin;

-- Назначаем администратору полные права на схему public
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO administrator;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO administrator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO administrator;
GRANT CREATE ON SCHEMA public TO administrator;

---------------------------------------------------
CREATE ROLE visitor;

CREATE USER tester WITH PASSWORD 'tester';
GRANT "visitor" TO tester;

-- Назначаем право на просмотр таблиц для роли visitors
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;

---------------------------------------------------


-- SELECT current_user;
-- SELECT rolname
-- FROM pg_roles; -- просмотреть все роли




