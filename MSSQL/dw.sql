CREATE PROCEDURE insert_date_dim @start_date DATE, @end_date DATE
AS
BEGIN
    DECLARE @date_key INT;
    DECLARE @cur_date DATE;
    DECLARE @cur_date_key INT;
    
    SET @cur_date = @start_date;
    
    SELECT @cur_date_key = ISNULL(MAX(date_key), 0) + 1 FROM date_dim;
    
    WHILE (@cur_date <= @end_date)
    BEGIN
        INSERT INTO date_dim (date_key, date, day, month, year, day_of_week)
        VALUES (
            @cur_date_key,
            @cur_date,
            DAY(@cur_date),
            MONTH(@cur_date),
            YEAR(@cur_date),
            DATENAME(WEEKDAY, @cur_date)
        );
        
        SET @cur_date_key = @cur_date_key + 1;
        SET @cur_date = DATEADD(DAY, 1, @cur_date);
    END
END;


EXEC insert_date_dim '2024-01-01', '2024-07-30';


CREATE PROCEDURE insert_region_dim 
AS
BEGIN
    INSERT INTO region_dim (region, area, ward)
    SELECT DISTINCT region_name, area_name, ward_name
    FROM sell_house_data
    ORDER BY area_name, ward_name, region_name;
END;
EXEC insert_region_dim;


INSERT INTO property_type_dim (property_type_key, property_subtype_key, property_type, property_subtype) 
VALUES 
(1, 1, N'Nhà ở', N'Nhà mặt phố, mặt tiền'),
(1, 2, N'Nhà ở', N'Nhà biệt thự'),
(1, 3, N'Nhà ở', N'Nhà ngõ, hẻm'),
(1, 4, N'Nhà ở', N'Nhà phố liền kề');
INSERT INTO property_type_dim (property_type_key, property_subtype_key, property_type, property_subtype) 
VALUES 
(2, 1, N'Căn hộ/Chung cư', N'Chung cư'),
(2, 2, N'Căn hộ/Chung cư', N'Căn hộ dịch vụ, mini'),
(2, 3, N'Căn hộ/Chung cư', N'Duplex'),
(2, 4, N'Căn hộ/Chung cư', N'Penthouse'),
(2, 5, N'Căn hộ/Chung cư', N'Tập thể, cư xá'),
(2, 6, N'Căn hộ/Chung cư', N'Officetel');

INSERT INTO property_type_dim (property_type_key, property_subtype_key, property_type, property_subtype) 
VALUES 
(4, 1, N'Đất', N'Đất thổ cư'),
(4, 2, N'Đất', N'Đất nền dự án'),
(4, 3, N'Đất', N'Đất công nghiệp'),
(4, 4, N'Đất', N'Đất nông nghiệp');

INSERT INTO property_type_dim (property_type_key, property_subtype_key, property_type, property_subtype) 
VALUES 
(3, 1, N'Văn phòng, Mặt bằng kinh doanh', N'Shophouse'),
(3, 2, N'Văn phòng, Mặt bằng kinh doanh', N'Officetel'),
(3, 3, N'Văn phòng, Mặt bằng kinh doanh', N'Văn phòng'),
(3, 4, N'Văn phòng, Mặt bằng kinh doanh', N'Mặt bằng kinh doanh');


INSERT INTO type_dim (type_key, type_name) 
VALUES 
(1, 'Mua bán'),
(2, 'Thuê');