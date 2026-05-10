INSTALL ducklake;
INSTALL mysql;

ATTACH 'ducklake:mysql:db=snapshot host=localhost port=3307 user=root password=root' AS my_ducklake (
    DATA_PATH 'data_files_mysql/'
);
USE my_ducklake;
