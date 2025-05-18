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
-- Name: EXTENSION postgres_fdw; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgres_fdw IS 'foreign-data wrapper for remote PostgreSQL servers';


--
-- Name: moodys_server; Type: SERVER; Schema: -; Owner: -
--

CREATE SERVER moodys_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    dbname 'moodys_restore_test',
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
-- Name: companies_foreign; Type: FOREIGN TABLE; Schema: public; Owner: -
--

CREATE FOREIGN TABLE public.companies_foreign (
    id integer,
    name character varying(100),
    rating character varying(10),
    last_updated timestamp without time zone
)
SERVER moodys_server
OPTIONS (
    schema_name 'public',
    table_name 'companies'
);


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: customer_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.customer_transactions (
    id integer NOT NULL,
    customer_id integer,
    transaction_date timestamp without time zone,
    amount numeric(10,2),
    description text
);


--
-- Name: customer_transactions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.customer_transactions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: customer_transactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.customer_transactions_id_seq OWNED BY public.customer_transactions.id;


--
-- Name: customer_transactions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customer_transactions ALTER COLUMN id SET DEFAULT nextval('public.customer_transactions_id_seq'::regclass);


--
-- PostgreSQL database dump complete
--

