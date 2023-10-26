--create etl user
CREATE USER etl WITH PASSWORD 'password';
--grant connect
GRANT CONNECT ON DATABASE adventureworks TO etl;
--grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl;