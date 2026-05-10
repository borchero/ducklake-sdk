-- MySQL dump 10.13  Distrib 9.5.0, for Linux (aarch64)
--
-- Host: localhost    Database: snapshot
-- ------------------------------------------------------
-- Server version	9.5.0

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `ducklake_column`
--

DROP TABLE IF EXISTS `ducklake_column`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_column` (
  `column_id` bigint DEFAULT NULL,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `column_order` bigint DEFAULT NULL,
  `column_name` text,
  `column_type` text,
  `initial_default` text,
  `default_value` text,
  `nulls_allowed` tinyint(1) DEFAULT NULL,
  `parent_column` bigint DEFAULT NULL,
  `default_value_type` text,
  `default_value_dialect` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_column`
--

LOCK TABLES `ducklake_column` WRITE;
/*!40000 ALTER TABLE `ducklake_column` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_column` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_column_mapping`
--

DROP TABLE IF EXISTS `ducklake_column_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_column_mapping` (
  `mapping_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `type` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_column_mapping`
--

LOCK TABLES `ducklake_column_mapping` WRITE;
/*!40000 ALTER TABLE `ducklake_column_mapping` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_column_mapping` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_column_tag`
--

DROP TABLE IF EXISTS `ducklake_column_tag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_column_tag` (
  `table_id` bigint DEFAULT NULL,
  `column_id` bigint DEFAULT NULL,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `key` text,
  `value` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_column_tag`
--

LOCK TABLES `ducklake_column_tag` WRITE;
/*!40000 ALTER TABLE `ducklake_column_tag` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_column_tag` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_data_file`
--

DROP TABLE IF EXISTS `ducklake_data_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_data_file` (
  `data_file_id` bigint NOT NULL,
  `table_id` bigint DEFAULT NULL,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `file_order` bigint DEFAULT NULL,
  `path` text,
  `path_is_relative` tinyint(1) DEFAULT NULL,
  `file_format` text,
  `record_count` bigint DEFAULT NULL,
  `file_size_bytes` bigint DEFAULT NULL,
  `footer_size` bigint DEFAULT NULL,
  `row_id_start` bigint DEFAULT NULL,
  `partition_id` bigint DEFAULT NULL,
  `encryption_key` text,
  `mapping_id` bigint DEFAULT NULL,
  `partial_max` bigint DEFAULT NULL,
  PRIMARY KEY (`data_file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_data_file`
--

LOCK TABLES `ducklake_data_file` WRITE;
/*!40000 ALTER TABLE `ducklake_data_file` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_data_file` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_delete_file`
--

DROP TABLE IF EXISTS `ducklake_delete_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_delete_file` (
  `delete_file_id` bigint NOT NULL,
  `table_id` bigint DEFAULT NULL,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `data_file_id` bigint DEFAULT NULL,
  `path` text,
  `path_is_relative` tinyint(1) DEFAULT NULL,
  `format` text,
  `delete_count` bigint DEFAULT NULL,
  `file_size_bytes` bigint DEFAULT NULL,
  `footer_size` bigint DEFAULT NULL,
  `encryption_key` text,
  `partial_max` bigint DEFAULT NULL,
  PRIMARY KEY (`delete_file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_delete_file`
--

LOCK TABLES `ducklake_delete_file` WRITE;
/*!40000 ALTER TABLE `ducklake_delete_file` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_delete_file` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_file_column_stats`
--

DROP TABLE IF EXISTS `ducklake_file_column_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_file_column_stats` (
  `data_file_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `column_id` bigint DEFAULT NULL,
  `column_size_bytes` bigint DEFAULT NULL,
  `value_count` bigint DEFAULT NULL,
  `null_count` bigint DEFAULT NULL,
  `min_value` text,
  `max_value` text,
  `contains_nan` tinyint(1) DEFAULT NULL,
  `extra_stats` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_file_column_stats`
--

LOCK TABLES `ducklake_file_column_stats` WRITE;
/*!40000 ALTER TABLE `ducklake_file_column_stats` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_file_column_stats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_file_partition_value`
--

DROP TABLE IF EXISTS `ducklake_file_partition_value`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_file_partition_value` (
  `data_file_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `partition_key_index` bigint DEFAULT NULL,
  `partition_value` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_file_partition_value`
--

LOCK TABLES `ducklake_file_partition_value` WRITE;
/*!40000 ALTER TABLE `ducklake_file_partition_value` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_file_partition_value` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_file_variant_stats`
--

DROP TABLE IF EXISTS `ducklake_file_variant_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_file_variant_stats` (
  `data_file_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `column_id` bigint DEFAULT NULL,
  `variant_path` text,
  `shredded_type` text,
  `column_size_bytes` bigint DEFAULT NULL,
  `value_count` bigint DEFAULT NULL,
  `null_count` bigint DEFAULT NULL,
  `min_value` text,
  `max_value` text,
  `contains_nan` tinyint(1) DEFAULT NULL,
  `extra_stats` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_file_variant_stats`
--

LOCK TABLES `ducklake_file_variant_stats` WRITE;
/*!40000 ALTER TABLE `ducklake_file_variant_stats` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_file_variant_stats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_files_scheduled_for_deletion`
--

DROP TABLE IF EXISTS `ducklake_files_scheduled_for_deletion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_files_scheduled_for_deletion` (
  `data_file_id` bigint DEFAULT NULL,
  `path` text,
  `path_is_relative` tinyint(1) DEFAULT NULL,
  `schedule_start` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_files_scheduled_for_deletion`
--

LOCK TABLES `ducklake_files_scheduled_for_deletion` WRITE;
/*!40000 ALTER TABLE `ducklake_files_scheduled_for_deletion` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_files_scheduled_for_deletion` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_inlined_data_tables`
--

DROP TABLE IF EXISTS `ducklake_inlined_data_tables`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_inlined_data_tables` (
  `table_id` bigint DEFAULT NULL,
  `table_name` text,
  `schema_version` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_inlined_data_tables`
--

LOCK TABLES `ducklake_inlined_data_tables` WRITE;
/*!40000 ALTER TABLE `ducklake_inlined_data_tables` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_inlined_data_tables` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_macro`
--

DROP TABLE IF EXISTS `ducklake_macro`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_macro` (
  `schema_id` bigint DEFAULT NULL,
  `macro_id` bigint DEFAULT NULL,
  `macro_name` text,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_macro`
--

LOCK TABLES `ducklake_macro` WRITE;
/*!40000 ALTER TABLE `ducklake_macro` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_macro` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_macro_impl`
--

DROP TABLE IF EXISTS `ducklake_macro_impl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_macro_impl` (
  `macro_id` bigint DEFAULT NULL,
  `impl_id` bigint DEFAULT NULL,
  `dialect` text,
  `sql` text,
  `type` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_macro_impl`
--

LOCK TABLES `ducklake_macro_impl` WRITE;
/*!40000 ALTER TABLE `ducklake_macro_impl` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_macro_impl` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_macro_parameters`
--

DROP TABLE IF EXISTS `ducklake_macro_parameters`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_macro_parameters` (
  `macro_id` bigint DEFAULT NULL,
  `impl_id` bigint DEFAULT NULL,
  `column_id` bigint DEFAULT NULL,
  `parameter_name` text,
  `parameter_type` text,
  `default_value` text,
  `default_value_type` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_macro_parameters`
--

LOCK TABLES `ducklake_macro_parameters` WRITE;
/*!40000 ALTER TABLE `ducklake_macro_parameters` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_macro_parameters` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_metadata`
--

DROP TABLE IF EXISTS `ducklake_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_metadata` (
  `key` text NOT NULL,
  `value` text NOT NULL,
  `scope` text,
  `scope_id` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_metadata`
--

LOCK TABLES `ducklake_metadata` WRITE;
/*!40000 ALTER TABLE `ducklake_metadata` DISABLE KEYS */;
INSERT INTO `ducklake_metadata` VALUES ('version','0.4',NULL,NULL),('created_by','DuckDB 7dbb2e646f',NULL,NULL),('data_path','data_files_mysql/',NULL,NULL),('encrypted','false',NULL,NULL);
/*!40000 ALTER TABLE `ducklake_metadata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_name_mapping`
--

DROP TABLE IF EXISTS `ducklake_name_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_name_mapping` (
  `mapping_id` bigint DEFAULT NULL,
  `column_id` bigint DEFAULT NULL,
  `source_name` text,
  `target_field_id` bigint DEFAULT NULL,
  `parent_column` bigint DEFAULT NULL,
  `is_partition` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_name_mapping`
--

LOCK TABLES `ducklake_name_mapping` WRITE;
/*!40000 ALTER TABLE `ducklake_name_mapping` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_name_mapping` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_partition_column`
--

DROP TABLE IF EXISTS `ducklake_partition_column`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_partition_column` (
  `partition_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `partition_key_index` bigint DEFAULT NULL,
  `column_id` bigint DEFAULT NULL,
  `transform` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_partition_column`
--

LOCK TABLES `ducklake_partition_column` WRITE;
/*!40000 ALTER TABLE `ducklake_partition_column` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_partition_column` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_partition_info`
--

DROP TABLE IF EXISTS `ducklake_partition_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_partition_info` (
  `partition_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_partition_info`
--

LOCK TABLES `ducklake_partition_info` WRITE;
/*!40000 ALTER TABLE `ducklake_partition_info` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_partition_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_schema`
--

DROP TABLE IF EXISTS `ducklake_schema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_schema` (
  `schema_id` bigint NOT NULL,
  `schema_uuid` text,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `schema_name` text,
  `path` text,
  `path_is_relative` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_schema`
--

LOCK TABLES `ducklake_schema` WRITE;
/*!40000 ALTER TABLE `ducklake_schema` DISABLE KEYS */;
INSERT INTO `ducklake_schema` VALUES (0,'dcb15e71-745a-4aad-a86c-fe9ea983af79',0,NULL,'main','main/',1);
/*!40000 ALTER TABLE `ducklake_schema` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_schema_versions`
--

DROP TABLE IF EXISTS `ducklake_schema_versions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_schema_versions` (
  `begin_snapshot` bigint DEFAULT NULL,
  `schema_version` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_schema_versions`
--

LOCK TABLES `ducklake_schema_versions` WRITE;
/*!40000 ALTER TABLE `ducklake_schema_versions` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_schema_versions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_snapshot`
--

DROP TABLE IF EXISTS `ducklake_snapshot`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_snapshot` (
  `snapshot_id` bigint NOT NULL,
  `snapshot_time` datetime DEFAULT NULL,
  `schema_version` bigint DEFAULT NULL,
  `next_catalog_id` bigint DEFAULT NULL,
  `next_file_id` bigint DEFAULT NULL,
  PRIMARY KEY (`snapshot_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_snapshot`
--

LOCK TABLES `ducklake_snapshot` WRITE;
/*!40000 ALTER TABLE `ducklake_snapshot` DISABLE KEYS */;
INSERT INTO `ducklake_snapshot` VALUES (0,'2026-05-03 11:19:13',0,1,0);
/*!40000 ALTER TABLE `ducklake_snapshot` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_snapshot_changes`
--

DROP TABLE IF EXISTS `ducklake_snapshot_changes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_snapshot_changes` (
  `snapshot_id` bigint NOT NULL,
  `changes_made` text,
  `author` text,
  `commit_message` text,
  `commit_extra_info` text,
  PRIMARY KEY (`snapshot_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_snapshot_changes`
--

LOCK TABLES `ducklake_snapshot_changes` WRITE;
/*!40000 ALTER TABLE `ducklake_snapshot_changes` DISABLE KEYS */;
INSERT INTO `ducklake_snapshot_changes` VALUES (0,'created_schema:\"main\"',NULL,NULL,NULL);
/*!40000 ALTER TABLE `ducklake_snapshot_changes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_sort_expression`
--

DROP TABLE IF EXISTS `ducklake_sort_expression`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_sort_expression` (
  `sort_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `sort_key_index` bigint DEFAULT NULL,
  `expression` text,
  `dialect` text,
  `sort_direction` text,
  `null_order` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_sort_expression`
--

LOCK TABLES `ducklake_sort_expression` WRITE;
/*!40000 ALTER TABLE `ducklake_sort_expression` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_sort_expression` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_sort_info`
--

DROP TABLE IF EXISTS `ducklake_sort_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_sort_info` (
  `sort_id` bigint DEFAULT NULL,
  `table_id` bigint DEFAULT NULL,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_sort_info`
--

LOCK TABLES `ducklake_sort_info` WRITE;
/*!40000 ALTER TABLE `ducklake_sort_info` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_sort_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_table`
--

DROP TABLE IF EXISTS `ducklake_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_table` (
  `table_id` bigint DEFAULT NULL,
  `table_uuid` text,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `schema_id` bigint DEFAULT NULL,
  `table_name` text,
  `path` text,
  `path_is_relative` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_table`
--

LOCK TABLES `ducklake_table` WRITE;
/*!40000 ALTER TABLE `ducklake_table` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_table` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_table_column_stats`
--

DROP TABLE IF EXISTS `ducklake_table_column_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_table_column_stats` (
  `table_id` bigint DEFAULT NULL,
  `column_id` bigint DEFAULT NULL,
  `contains_null` tinyint(1) DEFAULT NULL,
  `contains_nan` tinyint(1) DEFAULT NULL,
  `min_value` text,
  `max_value` text,
  `extra_stats` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_table_column_stats`
--

LOCK TABLES `ducklake_table_column_stats` WRITE;
/*!40000 ALTER TABLE `ducklake_table_column_stats` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_table_column_stats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_table_stats`
--

DROP TABLE IF EXISTS `ducklake_table_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_table_stats` (
  `table_id` bigint DEFAULT NULL,
  `record_count` bigint DEFAULT NULL,
  `next_row_id` bigint DEFAULT NULL,
  `file_size_bytes` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_table_stats`
--

LOCK TABLES `ducklake_table_stats` WRITE;
/*!40000 ALTER TABLE `ducklake_table_stats` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_table_stats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_tag`
--

DROP TABLE IF EXISTS `ducklake_tag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_tag` (
  `object_id` bigint DEFAULT NULL,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `key` text,
  `value` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_tag`
--

LOCK TABLES `ducklake_tag` WRITE;
/*!40000 ALTER TABLE `ducklake_tag` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_tag` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ducklake_view`
--

DROP TABLE IF EXISTS `ducklake_view`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ducklake_view` (
  `view_id` bigint DEFAULT NULL,
  `view_uuid` text,
  `begin_snapshot` bigint DEFAULT NULL,
  `end_snapshot` bigint DEFAULT NULL,
  `schema_id` bigint DEFAULT NULL,
  `view_name` text,
  `dialect` text,
  `sql` text,
  `column_aliases` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ducklake_view`
--

LOCK TABLES `ducklake_view` WRITE;
/*!40000 ALTER TABLE `ducklake_view` DISABLE KEYS */;
/*!40000 ALTER TABLE `ducklake_view` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-05-03  9:19:13
