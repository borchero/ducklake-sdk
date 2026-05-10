INSTALL ducklake;
INSTALL sqlite;

ATTACH 'ducklake:sqlite:metadata.sqlite' AS my_ducklake (DATA_PATH 'data_files_sqlite/');
USE my_ducklake;
