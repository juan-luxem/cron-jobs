CREATE TABLE OfeVtaHidroHorMDA (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Codigo NVARCHAR(50) NULL, -- Or NOT NULL if it's always present. Is it unique?
    Estatus_Asignacion NVARCHAR(10) NULL,
    Hora TINYINT NULL,
    Limite_De_Despacho_Maximo_MW DECIMAL(18, 4) NULL,
    Limite_De_Despacho_Minimo_MW DECIMAL(18, 4) NULL,
    Reserva_Rodante_10_Min_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_Rodante_10_Min_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit from header
    Reserva_No_Rodante_10_Min_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_No_Rodante_10_Min_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit
    Reserva_Rodante_Suplementaria_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_Rodante_Suplementaria_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit
    Reserva_No_Rodante_Suplementaria_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_No_Rodante_Suplementaria_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit
    Reserva_Regulacion_Secundaria_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_Regulacion_Secundaria_Pesos_Por_MW DECIMAL(18, 4) NULL -- Clarified unit
);

CREATE TABLE OfeVtaHidroHorMTR (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Codigo NVARCHAR(50) NULL, -- Or NOT NULL if it's always present. Is it unique?
    Estatus_Asignacion NVARCHAR(10) NULL,
    Hora TINYINT NULL,
    Limite_De_Despacho_Maximo_MW DECIMAL(18, 4) NULL,
    Limite_De_Despacho_Minimo_MW DECIMAL(18, 4) NULL,
    Reserva_Rodante_10_Min_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_Rodante_10_Min_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit from header
    Reserva_No_Rodante_10_Min_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_No_Rodante_10_Min_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit
    Reserva_Rodante_Suplementaria_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_Rodante_Suplementaria_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit
    Reserva_No_Rodante_Suplementaria_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_No_Rodante_Suplementaria_Pesos_Por_MW DECIMAL(18, 4) NULL, -- Clarified unit
    Reserva_Regulacion_Secundaria_MW DECIMAL(18, 4) NULL,
    Costo_Reserva_Regulacion_Secundaria_Pesos_Por_MW DECIMAL(18, 4) NULL -- Clarified unit
);

CREATE TABLE OfertaVentaNoDespachadaHorariaMDA (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    FechaOperacion DATE NOT NULL, -- To store the date from the filename, e.g., '2025-03-09'
    Codigo NVARCHAR(50) NOT NULL,
    Hora TINYINT NOT NULL,
    Potencia_Media_MW INT NOT NULL, -- Or INT if always whole numbers. Or NOT NULL DEFAULT 0 if 0 is always supplied.
    Sistema CHAR(3) NOT NULL, -- Assuming this is a fixed length code
    
    -- Optional: Add a unique constraint if this combination should be unique
    CONSTRAINT UQ_OfeVtaNoDespHor_OperacionCodigoHora UNIQUE (FechaOperacion, Codigo, Hora)
);

CREATE TABLE OfertaVentaNoDespachadaHorariaMTR (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    FechaOperacion DATE NOT NULL, -- To store the date from the filename, e.g., '2025-03-09'
    Codigo NVARCHAR(50) NOT NULL,
    Hora TINYINT NOT NULL,
    Potencia_Media_MW INT NOT NULL, -- Or INT if always whole numbers. Or NOT NULL DEFAULT 0 if 0 is always supplied.
    Sistema CHAR(3) NOT NULL, -- Assuming this is a fixed length code
    
    -- Optional: Add a unique constraint if this combination should be unique
    CONSTRAINT UQ_OfeVtaNoDespHor_OperacionCodigoHora UNIQUE (FechaOperacion, Codigo, Hora)
);

CREATE TABLE ProgramacionGeneracionGIHorariaMDA (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    FechaOperacion DATE NOT NULL, -- To store the date from the filename, e.g., '2025-03-09'
    Codigo NVARCHAR(50) NOT NULL,
    Hora TINYINT NOT NULL,
    Potencia_Media_MW DECIMAL(18, 4) NULL, -- Example 0.002. (18,4) allows for this.
                                         -- Consider (18,3) if max 3 decimal places.
    Sistema CHAR(3) NOT NULL, -- Assuming this is a fixed length code
    
    -- Ensures that for a given date, code, and hour, there's only one entry
    CONSTRAINT UQ_ProgGenGI_OperacionCodigoHora UNIQUE (FechaOperacion, Codigo, Hora)
);

CREATE TABLE ProgramacionGeneracionGIHorariaMTR (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    FechaOperacion DATE NOT NULL, -- To store the date from the filename, e.g., '2025-03-09'
    Codigo NVARCHAR(50) NOT NULL,
    Hora TINYINT NOT NULL,
    Potencia_Media_MW DECIMAL(18, 4) NULL, -- Example 0.002. (18,4) allows for this.
                                         -- Consider (18,3) if max 3 decimal places.
    Sistema CHAR(3) NOT NULL, -- Assuming this is a fixed length code
    
    -- Ensures that for a given date, code, and hour, there's only one entry
    CONSTRAINT UQ_ProgGenGI_OperacionCodigoHora UNIQUE (FechaOperacion, Codigo, Hora)
);

CREATE TABLE DemandaRealBalance (
    ID INT IDENTITY(0,1) PRIMARY KEY,
    DiaOperacion DATE NOT NULL,          -- The date the data in the CSV pertains to (e.g., FechaReferenciaLiq - 42 days)
    Sistema NVARCHAR(49) NOT NULL,
    Area CHAR(3) NOT NULL,
    Hora TINYINT NOT NULL,
    Generacion_MWh DECIMAL(17, 5) NULL,
    Importacion_Total_MWh DECIMAL(17, 5) NULL,
    Exportacion_Total_MWh DECIMAL(17, 5) NULL,
    Intercambio_Neto_Entre_Gerencias_MWh DECIMAL(17, 5) NULL, -- Store '---' as NULL
    Estimacion_Demanda_Por_Balance_MWh DECIMAL(17, 5) NULL,
    Liq TINYINT NOT NULL,                  -- Liquidation version (0, 1, 2, or 3) relative to FechaReferenciaLiq
    FechaPublicacion DATE NOT NULL,      -- The "anchor" or "current" date for this Liq 0-3 set. For Liq 0, FechaOperacion == FechaReferenciaLiq.
    FechaCreacion DATETIME2 NULL,    -- Optional: Actual timestamp when the script downloaded/inserted this record.
    FechaActualizacion DATETIME2 NULL, -- Optional: Actual timestamp when the record was last updated.

    CONSTRAINT CK_LiqRange CHECK (Liq IN (0, 1, 2, 3)), -- Ensures Liq is within the expected range

    -- Updated unique constraint. This ensures that for a given operational day's data,
    -- for a specific Liq version, and belonging to a specific Liq Reference Date, the entry is unique.
    CONSTRAINT UQ_DemandaRealBalance_OperacionLiqRefUnica UNIQUE (DiaOperacion, Sistema, Area, Hora, Liq, FechaPublicacion)
);

-- Optional: Add indexes for common query patterns
-- CREATE INDEX IX_DemandaRealBalance_FechaReferenciaLiq ON DemandaRealBalance(FechaReferenciaLiq, Liq);
-- CREATE INDEX IX_DemandaRealBalance_FechaOperacion ON DemandaRealBalance(FechaOperacion);
-- Explanation of SQL Changes:FechaReferenciaLiq DATE NOT NULL: This is the crucial new column. It represents the date that was considered "day 0" for the set of Liq 0, 1, 2, 3 files.FechaDescargaScript DATETIME2 NULL: Changed FechaDescarga to FechaDescargaScript and made it a DATETIME2 for more precision if needed, and also made it NULLable if you decide it's purely for audit and not always populated.CONSTRAINT UQ_DemandaRealBalance_OperacionLiqRefUnica UNIQUE (FechaOperacion, Sistema, Area, Hora, Liq, FechaReferenciaLiq): The unique constraint now includes FechaReferenciaLiq. This correctly models that for a specific FechaOperacion, you could have multiple Liq entries if they belong to different FechaReferenciaLiq batches. More importantly, for a given FechaReferenciaLiq and a Liq number, the FechaOperacion, Sistema, Area, Hora combination must be unique.

-- Demanda

SELECT *  FROM Demanda ORDER BY FechaOperacion

SELECT FechaOperacion, COUNT(FechaOperacion) FROM Demanda GROUP BY FechaOperacion ORDER BY FechaOperacion