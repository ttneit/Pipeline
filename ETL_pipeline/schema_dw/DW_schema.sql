
CREATE TABLE date_dim (
    date_key INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    day_of_week NVARCHAR(10)
);

CREATE TABLE property_type_dim (
    property_type_key INT,
    property_subtype_key INT,
    property_type NVARCHAR(50),
    property_subtype NVARCHAR(50),
    PRIMARY KEY (property_type_key, property_subtype_key)
);


CREATE TABLE region_dim (
    region_key INT PRIMARY KEY IDENTITY(1,1),
    region NVARCHAR(50),
    area NVARCHAR(50),
    ward NVARCHAR(50)
);

CREATE TABLE type_dim (
    type_key INT PRIMARY KEY,
    type_name NVARCHAR(50)
)


CREATE TABLE real_estate_fact (
    property_type_key INT,
    property_subtype_key INT,
    type_key INT,
    region_key INT,
    date_key INT,
    number_of_ad INT,
    avg_size FLOAT,
    avg_price_per_m2 FLOAT,
    avg_price FLOAT,
	PRIMARY KEY (property_type_key,property_subtype_key,type_key,region_key,date_key),
    FOREIGN KEY (property_type_key, property_subtype_key) REFERENCES property_type_dim(property_type_key, property_subtype_key),
    FOREIGN KEY (region_key) REFERENCES region_dim(region_key),
    FOREIGN KEY (date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (type_key) REFERENCES type_dim(type_key),
);
