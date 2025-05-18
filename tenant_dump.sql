--
-- PostgreSQL database dump
--

-- Dumped from database version 16.8 (Ubuntu 16.8-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.8 (Ubuntu 16.8-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: postgres_fdw; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;


--
-- Name: moodys_server; Type: SERVER; Schema: -; Owner: -
--

CREATE SERVER moodys_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    dbname 'moodys',
    host 'localhost',
    port '5432'
);


--
-- Name: USER MAPPING postgres SERVER moodys_server; Type: USER MAPPING; Schema: -; Owner: -
--

CREATE USER MAPPING FOR postgres SERVER moodys_server OPTIONS (
    password 'your_new_password',
    "user" 'postgres'
);


--
-- PostgreSQL database dump complete
--

