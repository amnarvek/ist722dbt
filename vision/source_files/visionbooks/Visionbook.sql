-- Created by Vertabelo (http://vertabelo.com)
-- Last modification date: 2024-04-12 21:16:29.437


-- tables
-- Table: Authors
CREATE warehouse visionwarehouse
CREATE database VISION
USE database VISION
CREATE schema VISIONBOOKS
USE schema VISIONBOOKS
DROP TABLE Authors
CREATE TABLE Authors (
    au_id varchar(20)  NOT NULL,
    au_lname varchar(40)  NOT NULL,
    au_fname varchar(20)  NOT NULL,
    phone char(12)  NOT NULL,
    address varchar(40)  NULL,
    city varchar(20)  NULL,
    state char(2)  NULL,
    zip char(5)  NULL,
    contract boolean  NOT NULL,
    CONSTRAINT au_id PRIMARY KEY (au_id)
);

-- Table: Discounts
CREATE TABLE Discounts (
    discount_id varchar(40)  NOT NULL,
    sales_id char(4)  NOT NULL,
    lowqty smallint  NULL,
    highqty smallint  NULL,
    discount decimal(4,2)  NOT NULL,
    CONSTRAINT Discounts_pk PRIMARY KEY (discount_id)
);

-- Table: Publishers
CREATE TABLE Publishers (
    pub_id char(4)  NOT NULL,
    pub_name varchar(40)  NULL,
    city varchar(20)  NULL,
    state char(2)  NULL,
    country varchar(30)  NULL,
    CONSTRAINT Publishers_pk PRIMARY KEY (pub_id)
);

-- Table: Reader

CREATE TABLE Reader (
    reader_id varchar(40)  NOT NULL,
    reader_email varchar(40)  NOT NULL,
    reader_password varchar(20)  NOT NULL,
    CONSTRAINT Reader_pk PRIMARY KEY (reader_id)
);

DROP TABLE ReaderSales;

-- Table: Readersales
CREATE or replace TABLE Readersales (
    sales_id varchar(40)  NOT NULL,
    reader_id char(40)  NOT NULL,
    CONSTRAINT Readersales_pk PRIMARY KEY (sales_id)
);

DROP TABLE Sales;
-- Table: Sales
-- TRUNCATE TABLE Sales;

CREATE TABLE Sales (
    ord_date datetime  NOT NULL,
    payterms varchar(12)  NOT NULL,
    qty smallint  NOT NULL,
    sales_id char(40) NOT NULL,
    title_id varchar(40) NOT NULL,
    
    CONSTRAINT Sales_pk PRIMARY KEY (sales_id)
);

-- Table: Titleauthors
CREATE TABLE Titleauthors (
    au_id varchar(20)  NOT NULL,
    title_id varchar(40)  NOT NULL,
    au_ord smallint  NOT NULL,
    royaltyper int  NOT NULL,
    CONSTRAINT Titleauthors_pk PRIMARY KEY (au_id,title_id)
);
DROP TABLE Titles
-- Table: Titles
CREATE TABLE Titles (
    title_id varchar(40)  NOT NULL,
    title varchar(80)  NOT NULL,
    type char(12)  NOT NULL,
    pub_id char(4)  NOT NULL,
    price number(19,4)  NULL,
    advance number(19,4)  NULL,
    royalty int  NULL,
    ytd_sales int NULL,
    notes varchar(200)  NULL,
    pubtime datetime  NOT NULL,
    CONSTRAINT Titles_pk PRIMARY KEY (title_id)
);

-- foreign keys
-- Reference: Discounts_Sales (table: Discounts)
ALTER TABLE Discounts ADD CONSTRAINT Discounts_Sales
    FOREIGN KEY (sales_id)
    REFERENCES Sales (sales_id)
;

-- Reference: Readersales_Reader (table: Readersales)
ALTER TABLE Readersales ADD CONSTRAINT Readersales_Reader
    FOREIGN KEY (reader_id)
    REFERENCES Reader (reader_id)
;

-- Reference: Readersales_Sales (table: Readersales)
ALTER TABLE Readersales ADD CONSTRAINT Readersales_Sales
    FOREIGN KEY (sales_id)
    REFERENCES Sales (sales_id)
;

-- Reference: Sales_Titles (table: Sales)
ALTER TABLE Sales ADD CONSTRAINT Sales_Titles
    FOREIGN KEY (title_id)
    REFERENCES Titles (title_id)
;

-- Reference: Titleauthors_Authors (table: Titleauthors)
ALTER TABLE Titleauthors ADD CONSTRAINT Titleauthors_Authors
    FOREIGN KEY (au_id)
    REFERENCES Authors (au_id)
;

-- Reference: Titleauthors_Titles (table: Titleauthors)
ALTER TABLE Titleauthors ADD CONSTRAINT Titleauthors_Titles
    FOREIGN KEY (title_id)
    REFERENCES Titles (title_id)
;

-- Reference: Titles_Publishers (table: Titles)
ALTER TABLE Titles ADD CONSTRAINT Titles_Publishers
    FOREIGN KEY (pub_id)
    REFERENCES Publishers (pub_id)
;

-- End of file.
