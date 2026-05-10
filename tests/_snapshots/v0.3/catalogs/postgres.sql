--
-- PostgreSQL database dump
--

\restrict JTDavb5y0yRwIfZeMvdH1YgsDslRQ50R48WjpiOsR9rTaFKSboc51MWkAm78Exa

-- Dumped from database version 18.1 (Debian 18.1-1.pgdg13+2)
-- Dumped by pg_dump version 18.1 (Debian 18.1-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ducklake_column; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_column (
    column_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    table_id bigint,
    column_order bigint,
    column_name character varying,
    column_type character varying,
    initial_default character varying,
    default_value character varying,
    nulls_allowed boolean,
    parent_column bigint
);


ALTER TABLE public.ducklake_column OWNER TO postgres;

--
-- Name: ducklake_column_mapping; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_column_mapping (
    mapping_id bigint,
    table_id bigint,
    type character varying
);


ALTER TABLE public.ducklake_column_mapping OWNER TO postgres;

--
-- Name: ducklake_column_tag; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_column_tag (
    table_id bigint,
    column_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    key character varying,
    value character varying
);


ALTER TABLE public.ducklake_column_tag OWNER TO postgres;

--
-- Name: ducklake_data_file; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_data_file (
    data_file_id bigint NOT NULL,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    file_order bigint,
    path character varying,
    path_is_relative boolean,
    file_format character varying,
    record_count bigint,
    file_size_bytes bigint,
    footer_size bigint,
    row_id_start bigint,
    partition_id bigint,
    encryption_key character varying,
    partial_file_info character varying,
    mapping_id bigint
);


ALTER TABLE public.ducklake_data_file OWNER TO postgres;

--
-- Name: ducklake_delete_file; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_delete_file (
    delete_file_id bigint NOT NULL,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    data_file_id bigint,
    path character varying,
    path_is_relative boolean,
    format character varying,
    delete_count bigint,
    file_size_bytes bigint,
    footer_size bigint,
    encryption_key character varying
);


ALTER TABLE public.ducklake_delete_file OWNER TO postgres;

--
-- Name: ducklake_file_column_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_file_column_stats (
    data_file_id bigint,
    table_id bigint,
    column_id bigint,
    column_size_bytes bigint,
    value_count bigint,
    null_count bigint,
    min_value character varying,
    max_value character varying,
    contains_nan boolean,
    extra_stats character varying
);


ALTER TABLE public.ducklake_file_column_stats OWNER TO postgres;

--
-- Name: ducklake_file_partition_value; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_file_partition_value (
    data_file_id bigint,
    table_id bigint,
    partition_key_index bigint,
    partition_value character varying
);


ALTER TABLE public.ducklake_file_partition_value OWNER TO postgres;

--
-- Name: ducklake_files_scheduled_for_deletion; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_files_scheduled_for_deletion (
    data_file_id bigint,
    path character varying,
    path_is_relative boolean,
    schedule_start timestamp with time zone
);


ALTER TABLE public.ducklake_files_scheduled_for_deletion OWNER TO postgres;

--
-- Name: ducklake_inlined_data_tables; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_inlined_data_tables (
    table_id bigint,
    table_name character varying,
    schema_version bigint
);


ALTER TABLE public.ducklake_inlined_data_tables OWNER TO postgres;

--
-- Name: ducklake_metadata; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_metadata (
    key character varying NOT NULL,
    value character varying NOT NULL,
    scope character varying,
    scope_id bigint
);


ALTER TABLE public.ducklake_metadata OWNER TO postgres;

--
-- Name: ducklake_name_mapping; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_name_mapping (
    mapping_id bigint,
    column_id bigint,
    source_name character varying,
    target_field_id bigint,
    parent_column bigint,
    is_partition boolean
);


ALTER TABLE public.ducklake_name_mapping OWNER TO postgres;

--
-- Name: ducklake_partition_column; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_partition_column (
    partition_id bigint,
    table_id bigint,
    partition_key_index bigint,
    column_id bigint,
    transform character varying
);


ALTER TABLE public.ducklake_partition_column OWNER TO postgres;

--
-- Name: ducklake_partition_info; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_partition_info (
    partition_id bigint,
    table_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint
);


ALTER TABLE public.ducklake_partition_info OWNER TO postgres;

--
-- Name: ducklake_schema; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_schema (
    schema_id bigint NOT NULL,
    schema_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_name character varying,
    path character varying,
    path_is_relative boolean
);


ALTER TABLE public.ducklake_schema OWNER TO postgres;

--
-- Name: ducklake_schema_versions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_schema_versions (
    begin_snapshot bigint,
    schema_version bigint
);


ALTER TABLE public.ducklake_schema_versions OWNER TO postgres;

--
-- Name: ducklake_snapshot; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_snapshot (
    snapshot_id bigint NOT NULL,
    snapshot_time timestamp with time zone,
    schema_version bigint,
    next_catalog_id bigint,
    next_file_id bigint
);


ALTER TABLE public.ducklake_snapshot OWNER TO postgres;

--
-- Name: ducklake_snapshot_changes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_snapshot_changes (
    snapshot_id bigint NOT NULL,
    changes_made character varying,
    author character varying,
    commit_message character varying,
    commit_extra_info character varying
);


ALTER TABLE public.ducklake_snapshot_changes OWNER TO postgres;

--
-- Name: ducklake_table; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_table (
    table_id bigint,
    table_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_id bigint,
    table_name character varying,
    path character varying,
    path_is_relative boolean
);


ALTER TABLE public.ducklake_table OWNER TO postgres;

--
-- Name: ducklake_table_column_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_table_column_stats (
    table_id bigint,
    column_id bigint,
    contains_null boolean,
    contains_nan boolean,
    min_value character varying,
    max_value character varying,
    extra_stats character varying
);


ALTER TABLE public.ducklake_table_column_stats OWNER TO postgres;

--
-- Name: ducklake_table_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_table_stats (
    table_id bigint,
    record_count bigint,
    next_row_id bigint,
    file_size_bytes bigint
);


ALTER TABLE public.ducklake_table_stats OWNER TO postgres;

--
-- Name: ducklake_tag; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_tag (
    object_id bigint,
    begin_snapshot bigint,
    end_snapshot bigint,
    key character varying,
    value character varying
);


ALTER TABLE public.ducklake_tag OWNER TO postgres;

--
-- Name: ducklake_view; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ducklake_view (
    view_id bigint,
    view_uuid uuid,
    begin_snapshot bigint,
    end_snapshot bigint,
    schema_id bigint,
    view_name character varying,
    dialect character varying,
    sql character varying,
    column_aliases character varying
);


ALTER TABLE public.ducklake_view OWNER TO postgres;

--
-- Data for Name: ducklake_column; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column) FROM stdin;
\.


--
-- Data for Name: ducklake_column_mapping; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_column_mapping (mapping_id, table_id, type) FROM stdin;
\.


--
-- Data for Name: ducklake_column_tag; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_column_tag (table_id, column_id, begin_snapshot, end_snapshot, key, value) FROM stdin;
\.


--
-- Data for Name: ducklake_data_file; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id) FROM stdin;
\.


--
-- Data for Name: ducklake_delete_file; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key) FROM stdin;
\.


--
-- Data for Name: ducklake_file_column_stats; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats) FROM stdin;
\.


--
-- Data for Name: ducklake_file_partition_value; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value) FROM stdin;
\.


--
-- Data for Name: ducklake_files_scheduled_for_deletion; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_files_scheduled_for_deletion (data_file_id, path, path_is_relative, schedule_start) FROM stdin;
\.


--
-- Data for Name: ducklake_inlined_data_tables; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_inlined_data_tables (table_id, table_name, schema_version) FROM stdin;
\.


--
-- Data for Name: ducklake_metadata; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_metadata (key, value, scope, scope_id) FROM stdin;
version	0.3	\N	\N
created_by	DuckDB b8a06e4a22	\N	\N
data_path	data_files_postgres/	\N	\N
encrypted	false	\N	\N
\.


--
-- Data for Name: ducklake_name_mapping; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_name_mapping (mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) FROM stdin;
\.


--
-- Data for Name: ducklake_partition_column; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform) FROM stdin;
\.


--
-- Data for Name: ducklake_partition_info; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_partition_info (partition_id, table_id, begin_snapshot, end_snapshot) FROM stdin;
\.


--
-- Data for Name: ducklake_schema; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) FROM stdin;
0	e44f4b1a-db58-4957-99f9-64fb7f89530a	0	\N	main	main/	t
\.


--
-- Data for Name: ducklake_schema_versions; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_schema_versions (begin_snapshot, schema_version) FROM stdin;
0	0
\.


--
-- Data for Name: ducklake_snapshot; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) FROM stdin;
0	2026-05-03 09:18:41.443401+00	0	1	0
\.


--
-- Data for Name: ducklake_snapshot_changes; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_snapshot_changes (snapshot_id, changes_made, author, commit_message, commit_extra_info) FROM stdin;
0	created_schema:"main"	\N	\N	\N
\.


--
-- Data for Name: ducklake_table; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_table (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) FROM stdin;
\.


--
-- Data for Name: ducklake_table_column_stats; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) FROM stdin;
\.


--
-- Data for Name: ducklake_table_stats; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) FROM stdin;
\.


--
-- Data for Name: ducklake_tag; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value) FROM stdin;
\.


--
-- Data for Name: ducklake_view; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ducklake_view (view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases) FROM stdin;
\.


--
-- Name: ducklake_data_file ducklake_data_file_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ducklake_data_file
    ADD CONSTRAINT ducklake_data_file_pkey PRIMARY KEY (data_file_id);


--
-- Name: ducklake_delete_file ducklake_delete_file_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ducklake_delete_file
    ADD CONSTRAINT ducklake_delete_file_pkey PRIMARY KEY (delete_file_id);


--
-- Name: ducklake_schema ducklake_schema_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ducklake_schema
    ADD CONSTRAINT ducklake_schema_pkey PRIMARY KEY (schema_id);


--
-- Name: ducklake_snapshot_changes ducklake_snapshot_changes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ducklake_snapshot_changes
    ADD CONSTRAINT ducklake_snapshot_changes_pkey PRIMARY KEY (snapshot_id);


--
-- Name: ducklake_snapshot ducklake_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ducklake_snapshot
    ADD CONSTRAINT ducklake_snapshot_pkey PRIMARY KEY (snapshot_id);


--
-- PostgreSQL database dump complete
--

\unrestrict JTDavb5y0yRwIfZeMvdH1YgsDslRQ50R48WjpiOsR9rTaFKSboc51MWkAm78Exa

