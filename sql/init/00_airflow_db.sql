-- Airflow's own metadata database, on the same Postgres server as the
-- warehouse but in a separate database so the two never share tables.
-- (Runs first because init scripts run in filename order.)
CREATE DATABASE airflow;
