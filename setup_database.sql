-- Kiểm tra xem cơ sở dữ liệu đã tồn tại chưa, nếu chưa thì tạo mới
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'testdb')
BEGIN
    CREATE DATABASE testdb;
END;
GO

-- Chuyển sang ngữ cảnh của cơ sở dữ liệu online_store
USE testdb;
GO

-- Tạo bảng Người bán (Sellers)
CREATE TABLE Sellers (
    SellerID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100),
    Email NVARCHAR(100),
    PhoneNumber VARCHAR(20),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE()
);
GO

-- Tạo bảng Khách hàng (Customers)
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100),
    Email NVARCHAR(100),
    PhoneNumber VARCHAR(20),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE()
);
GO

-- Tạo bảng Danh mục sản phẩm (ProductCategories)
CREATE TABLE ProductCategories (
    CategoryID INT PRIMARY KEY IDENTITY(1,1),
    CategoryName NVARCHAR(50) NOT NULL,
    CategoryDescription NVARCHAR(MAX),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE()
);
GO

-- Tạo bảng Sản phẩm (Products)
CREATE TABLE Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100),
    Description NVARCHAR(MAX),
    Price DECIMAL(18, 2),
    CategoryID INT, -- Liên kết đến bảng ProductCategories
    SellerID INT,
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (CategoryID) REFERENCES ProductCategories(CategoryID),
    FOREIGN KEY (SellerID) REFERENCES Sellers(SellerID)
);
GO

-- Tạo bảng Trạng thái đơn hàng (OrderStatus)
CREATE TABLE OrderStatus (
    StatusID INT PRIMARY KEY IDENTITY(1,1),
    StatusName NVARCHAR(50) NOT NULL,
    StatusDescription NVARCHAR(MAX),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE()
);
GO

-- Tạo bảng Đơn hàng (Orders)
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    OrderNumber NVARCHAR(50),
    TotalAmount DECIMAL(18, 2),
    StatusID INT DEFAULT 1, -- Liên kết đến bảng OrderStatus
    CustomerID INT,
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (StatusID) REFERENCES OrderStatus(StatusID)
);
GO

-- Tạo bảng Lý do (Reasons) cho việc hủy/trả hàng
CREATE TABLE Reasons (
    ReasonID INT PRIMARY KEY IDENTITY(1,1),
    OrderID INT NOT NULL, -- Liên kết đến bảng Orders
    ReasonType NVARCHAR(20) NOT NULL CHECK (ReasonType IN ('Cancellation', 'Return')),
    ReasonDescription NVARCHAR(MAX) NOT NULL,
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID)
);
GO

-- Tạo bảng Chi tiết đơn hàng (OrderItems)
CREATE TABLE OrderItems (
    OrderItemID INT PRIMARY KEY IDENTITY(1,1),
    OrderID INT,
    ProductID INT,
    Quantity INT,
    CurrentPrice DECIMAL(18, 2),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
GO

-- Tạo bảng Giỏ hàng (ShoppingCarts)
CREATE TABLE ShoppingCarts (
    CartID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);
GO

-- Tạo bảng Chi tiết giỏ hàng (CartItems)
CREATE TABLE CartItems (
    CartItemID INT PRIMARY KEY IDENTITY(1,1),
    CartID INT,
    ProductID INT,
    Quantity INT,
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (CartID) REFERENCES ShoppingCarts(CartID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
GO

-- Tạo bảng Phương thức thanh toán (PaymentMethods)
CREATE TABLE PaymentMethods (
    PaymentMethodID INT PRIMARY KEY IDENTITY(1,1),
    MethodName NVARCHAR(50) NOT NULL,
    MethodDescription NVARCHAR(MAX),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE()
);
GO

-- Tạo bảng Thanh toán (Payments)
CREATE TABLE Payments (
    PaymentID INT PRIMARY KEY IDENTITY(1,1),
    OrderID INT,
    PaymentMethodID INT, -- Liên kết đến bảng PaymentMethods
    Amount DECIMAL(18, 2),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (PaymentMethodID) REFERENCES PaymentMethods(PaymentMethodID)
);
GO

-- Tạo bảng Đánh giá (Reviews)
CREATE TABLE Reviews (
    ReviewID INT PRIMARY KEY IDENTITY(1,1),
    ProductID INT,
    CustomerID INT,
    Rating INT,
    Comment NVARCHAR(MAX),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);
GO

-- Tạo bảng Địa chỉ (Addresses)
CREATE TABLE Addresses (
    AddressID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    AddressLine NVARCHAR(100),
    City NVARCHAR(50),
    State NVARCHAR(50),
    ZipCode VARCHAR(20),
    Country NVARCHAR(50),
    IsBillingAddress BIT,
    IsShippingAddress BIT,
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);
GO

-- Tạo bảng Tồn kho (Inventory)
CREATE TABLE Inventory (
    InventoryID INT PRIMARY KEY IDENTITY(1,1),
    InventoryName NVARCHAR(50),
    ProductID INT,
    QuantityInStock INT,
    ReorderThreshold INT,
    UnitCost DECIMAL(18, 2),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    UpdatedAt DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
GO

--- TRIGGERS ĐỂ CẬP NHẬT CỘT 'UpdatedAt' ---

CREATE TRIGGER trg_Sellers_Update
ON Sellers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Sellers AS T INNER JOIN inserted AS I ON T.SellerID = I.SellerID;
END;
GO

CREATE TRIGGER trg_Customers_Update
ON Customers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Customers AS T INNER JOIN inserted AS I ON T.CustomerID = I.CustomerID;
END;
GO

CREATE TRIGGER trg_ProductCategories_Update
ON ProductCategories
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM ProductCategories AS T INNER JOIN inserted AS I ON T.CategoryID = I.CategoryID;
END;
GO

CREATE TRIGGER trg_Products_Update
ON Products
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Products AS T INNER JOIN inserted AS I ON T.ProductID = I.ProductID;
END;
GO

CREATE TRIGGER trg_OrderStatus_Update
ON OrderStatus
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM OrderStatus AS T INNER JOIN inserted AS I ON T.StatusID = I.StatusID;
END;
GO

CREATE TRIGGER trg_Orders_Update
ON Orders
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Orders AS T INNER JOIN inserted AS I ON T.OrderID = I.OrderID;
END;
GO

CREATE TRIGGER trg_Reasons_Update
ON Reasons
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Reasons AS T INNER JOIN inserted AS I ON T.ReasonID = I.ReasonID;
END;
GO

CREATE TRIGGER trg_OrderItems_Update
ON OrderItems
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM OrderItems AS T INNER JOIN inserted AS I ON T.OrderItemID = I.OrderItemID;
END;
GO

CREATE TRIGGER trg_ShoppingCarts_Update
ON ShoppingCarts
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM ShoppingCarts AS T INNER JOIN inserted AS I ON T.CartID = I.CartID;
END;
GO

CREATE TRIGGER trg_CartItems_Update
ON CartItems
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM CartItems AS T INNER JOIN inserted AS I ON T.CartItemID = I.CartItemID;
END;
GO

CREATE TRIGGER trg_PaymentMethods_Update
ON PaymentMethods
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM PaymentMethods AS T INNER JOIN inserted AS I ON T.PaymentMethodID = I.PaymentMethodID;
END;
GO

CREATE TRIGGER trg_Payments_Update
ON Payments
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Payments AS T INNER JOIN inserted AS I ON T.PaymentID = I.PaymentID;
END;
GO

CREATE TRIGGER trg_Reviews_Update
ON Reviews
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Reviews AS T INNER JOIN inserted AS I ON T.ReviewID = I.ReviewID;
END;
GO

CREATE TRIGGER trg_Addresses_Update
ON Addresses
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Addresses AS T INNER JOIN inserted AS I ON T.AddressID = I.AddressID;
END;
GO

CREATE TRIGGER trg_Inventory_Update
ON Inventory
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE T SET UpdatedAt = GETDATE() FROM Inventory AS T INNER JOIN inserted AS I ON T.InventoryID = I.InventoryID;
END;
GO

-- Bật CDC cho toàn bộ database (chạy nếu chưa bật)
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'testdb') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- Bật CDC cho từng bảng
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Sellers', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Customers', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ProductCategories', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Products', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'OrderStatus', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Orders', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Reasons', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'OrderItems', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ShoppingCarts', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'CartItems', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'PaymentMethods', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Payments', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Reviews', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Addresses', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Inventory', @role_name = NULL;
GO