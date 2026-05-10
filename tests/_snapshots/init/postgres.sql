INSTALL ducklake;
INSTALL postgres;

ATTACH 'ducklake:postgres:dbname=postgres host=localhost port=5433 user=postgres password=postgres' AS my_ducklake (
    DATA_PATH 'data_files_postgres/'
);
USE my_ducklake;
