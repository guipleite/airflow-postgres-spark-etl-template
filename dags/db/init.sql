CREATE DATABASE template_db;
\c template_db
CREATE SCHEMA IF NOT EXISTS bronze_layer;
CREATE TABLE IF NOT EXISTS  bronze_layer.beer_reviews (
    "id" INT,
    "image" TEXT,
    "name" TEXT,
    "price" FLOAT,
    "average" FLOAT,
    "reviews" INT,
    "date" timestamp
);

CREATE SCHEMA IF NOT EXISTS silver_layer;
