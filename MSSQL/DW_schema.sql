
CREATE TABLE date_dim (
    date_key INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    day_of_week VARCHAR(10)
);

CREATE TABLE property_type_dim (
    property_type_key INT,
    property_subtype_key INT,
    property_type VARCHAR(50),
    property_subtype VARCHAR(50),
    PRIMARY KEY (property_type_key, property_subtype_key)
);


CREATE TABLE region_dim (
    region_key INT PRIMARY KEY,
    region VARCHAR(50),
    area VARCHAR(50),
    ward VARCHAR(50)
);



CREATE TABLE real_estate_fact (
    ad_id INT PRIMARY KEY,
    property_type_key INT,
    property_subtype_key INT,
    size FLOAT,
    region_key INT,
    date_key INT,
    FOREIGN KEY (property_type_key, property_subtype_key) REFERENCES property_type_dim(property_type_key, property_subtype_key),
    FOREIGN KEY (region_key) REFERENCES region_dim(region_key),
    FOREIGN KEY (date_key) REFERENCES date_dim(date_key)
);
