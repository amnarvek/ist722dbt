create database if not exists vision;
create schema if not exists visionmusic;

use database vision; 
use schema visionmusic;

CREATE or REPLACE TABLE Tracks (
    track_id int  NOT NULL,
    artist_id int  NOT NULL,
    track_name varchar(50)  NOT NULL,
    track_duration TIMESTAMP  NOT NULL,
    track_popularity INT  NULL,    
    track_danceability FLOAT  NULL,
    track_energy FLOAT  NULL,
    track_bpm FLOAT NULL,
    track_loudness FLOAT NULL,
    track_liveness FLOAT NULL,
    track_genre VARCHAR(20) NULL,
    CONSTRAINT Tracks_pk PRIMARY KEY (track_id)

);

CREATE or REPLACE TABLE Artists (
    artist_id int  NOT NULL,
    artist_name varchar(20)  NOT NULL,
    artist_popularity FLOAT  NULL,
    artist_followers FLOAT  NULL,
    no_of_tracks INT NULL,
    one_billion_streaming INT NULL,
    hundred_million_streaming INT NULL,
    CONSTRAINT Artists_pk PRIMARY KEY (artist_id)
);


CREATE or REPLACE TABLE music_customers (
    customer_id int  NOT NULL,
    email varchar(50)  NOT NULL,
    CONSTRAINT customers_pk PRIMARY KEY (customer_id)
);

CREATE or REPLACE TABLE Membership_Billing (
    billing_id int  NOT NULL,
    membership_date DATE NOT NULL,
    customer_id int  NOT NULL,
    membership_id int  NOT NULL,
    CONSTRAINT Membership_Billing_pk PRIMARY KEY (billing_id)
);

CREATE or REPLACE TABLE Albums (
    album_id int  NOT NULL,
    artist_id int  NOT NULL,
    album_name varchar(50)  NOT NULL,
    release_date DATE  NULL,
    CONSTRAINT Albums_pk PRIMARY KEY (album_id)
);

CREATE or REPLACE TABLE Playlists (
    playlist_id int  NOT NULL,
    customer_id int  NOT NULL,
    playlist_name varchar(50)  NOT NULL,
    CONSTRAINT Playlists_pk PRIMARY KEY (playlist_id)
);

CREATE or REPLACE TABLE Playlist_Tracks (
    playlist_id int  NOT NULL,
    track_id int  NOT NULL,
    like_date_time TIMESTAMP  NOT NULL,
    CONSTRAINT Playlist_Tracks_pk PRIMARY KEY (playlist_id, track_id)
);

CREATE or REPLACE TABLE Membership (
    membership_id int  NOT NULL,
    membership_name VARCHAR(20)  NOT NULL,
    price DECIMAL(10, 2)  NOT NULL,
    expiry_date DATE NOT NULL,
    CONSTRAINT Membership_pk PRIMARY KEY (membership_id)
);

CREATE or REPLACE TABLE Likes (
    customer_id int  NOT NULL,
    track_id int  NOT NULL,
    like_date_time TIMESTAMP  NOT NULL,
    CONSTRAINT Likes_pk PRIMARY KEY (customer_id, track_id)
);

CREATE or REPLACE TABLE Followers (
    customer_id int  NOT NULL,
    artist_id int  NOT NULL,
    CONSTRAINT Followers_pk PRIMARY KEY (customer_id, artist_id)
);

CREATE or REPLACE TABLE Songs_Played (
    track_id int  NOT NULL,
    date_time TIMESTAMP  NOT NULL,
    CONSTRAINT Songs_Played_PK PRIMARY KEY (track_id, date_time)
);

-- Table: plans
CREATE TABLE IF NOT EXISTS music_plans (
    plan_id int  NOT NULL,
    plan_name varchar(50)  NOT NULL,
    plan_price decimal(5,2)  NOT NULL,
    plan_current boolean  NOT NULL,
    discount decimal(5,2)  NULL,
    CONSTRAINT plans_pk PRIMARY KEY (plan_id)
);

-- foreign keys
ALTER TABLE Tracks ADD CONSTRAINT Tracks_Artists
    FOREIGN KEY (artist_id)
    REFERENCES Artists (artist_id);

ALTER TABLE Albums ADD CONSTRAINT Albums_Artists
    FOREIGN KEY (artist_id)
    REFERENCES Artists (artist_id);

ALTER TABLE Playlists ADD CONSTRAINT Playlists_music_customers
    FOREIGN KEY (customer_id)
    REFERENCES music_customers (customer_id);

ALTER TABLE Likes ADD CONSTRAINT Likes_Tracks
    FOREIGN KEY (track_id)
    REFERENCES Tracks (track_id);

ALTER TABLE membership_billing ADD CONSTRAINT Membership_billing_membership
    FOREIGN KEY (membership_id)
    REFERENCES Membership (membership_id);

ALTER TABLE membership_billing ADD CONSTRAINT Membership_billing_customer
    FOREIGN KEY (customer_id)
    REFERENCES music_customers (customer_id);


