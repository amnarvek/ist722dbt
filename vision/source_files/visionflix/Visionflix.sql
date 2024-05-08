CREATE WAREHOUSE VISIONWAREHOUSE;

DROP DATABASE VISION;

CREATE DATABASE IF NOT EXISTS VISION;

USE DATABASE VISION;

CREATE SCHEMA IF NOT EXISTS VISIONFLIX;

USE SCHEMA VISIONFLIX;
-- Created by Vertabelo (http://vertabelo.com)
-- Last modification date: 2024-04-12 19:02:17.256

-- tables
-- Table: account_activity
-- Table: account_activity
CREATE TABLE IF NOT EXISTS account_activity (
    activity_id INT NOT NULL,
    login_time TIMESTAMP NOT NULL,
    logout_time TIMESTAMP NULL,
    account_id INT NOT NULL,
    CONSTRAINT account_activity_pk PRIMARY KEY (activity_id)
);

-- Table: account_billing
CREATE TABLE IF NOT EXISTS account_billing (
    ab_id int  NOT NULL,
    ab_date datetime  NOT NULL,
    ab_account_id int  NOT NULL,
    ab_plan_id int  NOT NULL,
    CONSTRAINT account_billing_pk PRIMARY KEY (ab_id)
);

-- Table: account_titles
CREATE TABLE IF NOT EXISTS account_titles (
    at_id int  NOT NULL,
    at_account_id int  NOT NULL,
    at_title_id varchar(20)  NOT NULL,
    at_queued_date datetime  NOT NULL,
    at_remove_queued_date datetime  NULL,
    at_rating int  NULL,
    CONSTRAINT account_titles_pk PRIMARY KEY (at_id)
);

-- Table: accounts
CREATE TABLE IF NOT EXISTS accounts (
    account_id int  NOT NULL,
    account_email varchar(200)  NOT NULL,
    account_password varchar(100)  NOT NULL,
    CONSTRAINT accounts_pk PRIMARY KEY (account_id)
);

-- Table: cast
CREATE TABLE IF NOT EXISTS casting (
    cast_people_id int  NOT NULL,
    cast_title_id varchar(20)  NOT NULL,
    CONSTRAINT cast_pk PRIMARY KEY (cast_people_id,cast_title_id)
);

-- Table: directors
CREATE TABLE IF NOT EXISTS directors (
    director_people_id int  NOT NULL,
    director_title_id varchar(20)  NOT NULL,
    CONSTRAINT directors_pk PRIMARY KEY (director_people_id,director_title_id)
);

-- Table: genres
CREATE TABLE IF NOT EXISTS genres (
    genre_name varchar(200)  NOT NULL,
    CONSTRAINT genres_pk PRIMARY KEY (genre_name)
);

-- Table: people
CREATE TABLE IF NOT EXISTS people (
    people_id int  NOT NULL,
    people_name varchar(200)  NOT NULL,
    CONSTRAINT people_pk PRIMARY KEY (people_id)
);

-- Table: plans
CREATE TABLE IF NOT EXISTS plans (
    plan_id int  NOT NULL,
    plan_name varchar(50)  NOT NULL,
    plan_price decimal(5,2)  NOT NULL,
    plan_current boolean  NOT NULL,
    discount decimal(5,2)  NULL,
    CONSTRAINT plans_pk PRIMARY KEY (plan_id)
);

-- Table: title_genres
CREATE TABLE IF NOT EXISTS title_genres (
    tg_genre_name varchar(200)  NOT NULL,
    tg_title_id varchar(20)  NOT NULL,
    CONSTRAINT title_genres_pk PRIMARY KEY (tg_genre_name,tg_title_id)
);

-- Table: titles
CREATE TABLE IF NOT EXISTS titles (
    title_id varchar(20)  NOT NULL,
    title_name varchar(300)  NOT NULL,
    title_type varchar(20)  NOT NULL,
    title_synopsis varchar(50000)  NOT NULL,
    title_avg_rating decimal(3,2)  NOT NULL,
    title_release_year int  NOT NULL,
    title_runtime int  NOT NULL,
    title_rating varchar(20) NOT NULL,
    title_bluray_available boolean NOT NULL,
    title_date_modified date NOT NULL,
    CONSTRAINT titles_pk PRIMARY KEY (title_id)
);

-- foreign keys
-- Reference: account_activity_accounts (table: account_activity)
ALTER TABLE account_activity ADD CONSTRAINT account_activity_accounts
    FOREIGN KEY (account_id)
    REFERENCES accounts (account_id)
;

-- Reference: account_billing_accounts (table: account_billing)
ALTER TABLE account_billing ADD CONSTRAINT account_billing_accounts
    FOREIGN KEY (ab_account_id)
    REFERENCES accounts (account_id)
;

-- Reference: account_billing_plans (table: account_billing)
ALTER TABLE account_billing ADD CONSTRAINT account_billing_plans
    FOREIGN KEY (ab_plan_id)
    REFERENCES plans (plan_id)
;

-- Reference: account_titles_accounts (table: account_titles)
ALTER TABLE account_titles ADD CONSTRAINT account_titles_accounts
    FOREIGN KEY (at_account_id)
    REFERENCES accounts (account_id)
;

-- Reference: account_titles_titles (table: account_titles)
ALTER TABLE account_titles ADD CONSTRAINT account_titles_titles
    FOREIGN KEY (at_title_id)
    REFERENCES titles (title_id)
;

-- Reference: genres_title_genres (table: title_genres)
ALTER TABLE title_genres ADD CONSTRAINT genres_title_genres
    FOREIGN KEY (tg_genre_name)
    REFERENCES genres (genre_name)
;

-- Reference: people_cast (table: cast)
ALTER TABLE casting ADD CONSTRAINT people_cast
    FOREIGN KEY (cast_people_id)
    REFERENCES people (people_id)
;

-- Reference: people_directors (table: directors)
ALTER TABLE directors ADD CONSTRAINT people_directors
    FOREIGN KEY (director_people_id)
    REFERENCES people (people_id)
;

-- Reference: titles_cast (table: cast)
ALTER TABLE casting ADD CONSTRAINT titles_cast
    FOREIGN KEY (cast_title_id)
    REFERENCES titles (title_id)
;

-- Reference: titles_directors (table: directors)
ALTER TABLE directors ADD CONSTRAINT titles_directors
    FOREIGN KEY (director_title_id)
    REFERENCES titles (title_id)
;

-- Reference: titles_title_genres (table: title_genres)
ALTER TABLE title_genres ADD CONSTRAINT titles_title_genres
    FOREIGN KEY (tg_title_id)
    REFERENCES titles (title_id)
;

-- sequences
-- Sequence: account_activity_seq
CREATE SEQUENCE account_activity_seq
      START WITH 1
      INCREMENT BY 1;

-- Sequence: account_billing_seq
CREATE SEQUENCE account_billing_seq
      START WITH 1
      INCREMENT BY 1;

-- Sequence: account_titles_seq
CREATE SEQUENCE account_titles_seq
      START WITH 1
      INCREMENT BY 1;

-- Sequence: accounts_seq
CREATE SEQUENCE accounts_seq
      START WITH 1
      INCREMENT BY 1;

-- Sequence: people_seq
CREATE SEQUENCE people_seq
      START WITH 1
      INCREMENT BY 1;

-- Sequence: plans_seq
CREATE SEQUENCE plans_seq
      START WITH 1
      INCREMENT BY 1;

-- End of

