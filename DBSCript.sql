-- Table to map folders to destination tables
CREATE TABLE dbo.FolderConfig (
    ConfigID INT IDENTITY(1,1) PRIMARY KEY,
    FolderPath NVARCHAR(500) NOT NULL,
    TargetTableName NVARCHAR(128) NOT NULL,
    IsActive BIT DEFAULT 1
);

-- Insert folder mappings
INSERT INTO dbo.FolderConfig (FolderPath, TargetTableName) VALUES
('', 'RootData'),              -- Files in root folder
('Sales', 'SalesData'),        -- Files in Sales folder
('Inventory', 'InventoryData'); -- Files in Inventory folder

-- Verify
SELECT * FROM dbo.FolderConfig;

-- Table for root folder data (customers, products)
CREATE TABLE dbo.RootData (
    LoadID INT IDENTITY(1,1) PRIMARY KEY,
    SourceFile NVARCHAR(255),
    LoadTimestamp DATETIME DEFAULT GETDATE(),
    Col1 NVARCHAR(500),
    Col2 NVARCHAR(500),
    Col3 NVARCHAR(500),
    Col4 NVARCHAR(500),
    Col5 NVARCHAR(500)
);

-- Table for Sales folder data
CREATE TABLE dbo.SalesData (
    LoadID INT IDENTITY(1,1) PRIMARY KEY,
    SourceFile NVARCHAR(255),
    LoadTimestamp DATETIME DEFAULT GETDATE(),
    Col1 NVARCHAR(500),
    Col2 NVARCHAR(500),
    Col3 NVARCHAR(500),
    Col4 NVARCHAR(500),
    Col5 NVARCHAR(500),
    Col6 NVARCHAR(500),
    Col7 NVARCHAR(500)
);

-- Table for Inventory folder data
CREATE TABLE dbo.InventoryData (
    LoadID INT IDENTITY(1,1) PRIMARY KEY,
    SourceFile NVARCHAR(255),
    LoadTimestamp DATETIME DEFAULT GETDATE(),
    Col1 NVARCHAR(500),
    Col2 NVARCHAR(500),
    Col3 NVARCHAR(500),
    Col4 NVARCHAR(500),
    Col5 NVARCHAR(500),
    Col6 NVARCHAR(500)
);

-- Table to log all processing
CREATE TABLE dbo.ProcessingLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    FileName NVARCHAR(255),
    FolderPath NVARCHAR(500),
    TargetTable NVARCHAR(128),
    OriginalRows INT,
    CleanRows INT,
    DirtyRows INT,
    Status NVARCHAR(50),
    ProcessedAt DATETIME DEFAULT GETDATE()
);