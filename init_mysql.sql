
USE staging_db;

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(100) PRIMARY KEY,
    gender VARCHAR(10),
    title VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    dob_date VARCHAR(50),
    dob_age INT,
    registered_date VARCHAR(50),
    registered_age INT,
    phone VARCHAR(50),
    cell VARCHAR(50),
    nat VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS logins (
    user_id VARCHAR(100),
    username VARCHAR(100),
    password VARCHAR(100),
    salt VARCHAR(50),
    md5 VARCHAR(100),
    sha1 VARCHAR(100),
    sha256 VARCHAR(100),
    PRIMARY KEY(user_id)
);

CREATE TABLE IF NOT EXISTS locations (
    user_id VARCHAR(100),
    street_number INT,
    street_name VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(100),
    postcode VARCHAR(50),
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    timezone_offset VARCHAR(10),
    timezone_desc VARCHAR(255),
    PRIMARY KEY(user_id)
);

CREATE TABLE IF NOT EXISTS purchases (
    purchase_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100),
    product VARCHAR(255),
    unit_price DECIMAL(10,2),
    quantity INT,
    total DECIMAL(10,2),
    timestamp DATETIME
);
