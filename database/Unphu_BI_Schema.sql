-- ========================================================================
-- DATABASE SCHEMA PARA UNPHU-BI (SQL SERVER)
-- ========================================================================
-- Este script crea la base de datos y las tablas necesarias para almacenar
-- la información consumida desde la API de la universidad, facilitando el
-- análisis y de inteligencia de negocios (BI).
-- ========================================================================

-- 1. Crear Base de Datos
IF NOT EXISTS (SELECT name FROM master.sys.databases WHERE name = N'UnphuBI_DB')
BEGIN
    CREATE DATABASE [UnphuBI_DB];
END
GO

USE [UnphuBI_DB];
GO

-- ========================================================================
-- DIMENSIONES (Catálogos estáticos / maestros)
-- ========================================================================

-- Tabla: Dim_Periodo (Semestres y Años)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_Periodo]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Dim_Periodo] (
        [IdPeriodo] INT NOT NULL PRIMARY KEY,
        [Ano] INT NOT NULL,
        [NumeroPeriodo] INT NOT NULL, -- Ej: 1, 2, 3
        [Descripcion] NVARCHAR(100) NULL, -- Ej: "Enero-Abril 2025"
        [EsPeriodoActual] BIT DEFAULT 0,
        [FechaInicio] DATE NULL,
        [FechaFin] DATE NULL
    );
END
GO

-- Tabla: Dim_Carrera
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_Carrera]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Dim_Carrera] (
        [IdCarrera] INT NOT NULL PRIMARY KEY,
        [NombreCarrera] NVARCHAR(200) NOT NULL,
        [Facultad] NVARCHAR(200) NULL
    );
END
GO

-- Tabla: Dim_Asignatura (Materias del pensum)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_Asignatura]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Dim_Asignatura] (
        [IdAsignatura] INT NOT NULL PRIMARY KEY,
        [Codigo] NVARCHAR(20) NOT NULL,
        [Descripcion] NVARCHAR(200) NOT NULL,
        [Creditos] INT NOT NULL,
        [Area] NVARCHAR(100) NULL
    );
END
GO

-- Tabla: Dim_Estudiante (Datos de la Persona)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_Estudiante]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Dim_Estudiante] (
        [IdPersona] INT NOT NULL PRIMARY KEY,
        [Matricula] NVARCHAR(50) NOT NULL UNIQUE,
        [NombreCompleto] NVARCHAR(200) NOT NULL,
        [EmailInstitucional] NVARCHAR(150) NULL,
        [IdCarreraActiva] INT NULL,
        FOREIGN KEY ([IdCarreraActiva]) REFERENCES [dbo].[Dim_Carrera]([IdCarrera])
    );
END
GO


-- ========================================================================
-- TABLAS DE HECHOS (Transaccionales / Historial)
-- ========================================================================

-- Tabla: Fact_Calificaciones (Historial de notas por estudiante y materia)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Fact_Calificaciones]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Fact_Calificaciones] (
        [IdCalificacion] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [IdPersona] INT NOT NULL,
        [IdCarrera] INT NOT NULL,
        [IdAsignatura] INT NOT NULL,
        [IdPeriodo] INT NOT NULL,
        
        [Estatus] NVARCHAR(50) NOT NULL, -- Ej: "Completada", "Pendiente", "Retirada"
        [NotaLiteral] NVARCHAR(10) NULL, -- Ej: A, B, C, F, R
        [NotaNumerica] DECIMAL(5,2) NULL,
        [PuntosHonor] DECIMAL(5,2) NULL,
        [Aprobada] BIT NOT NULL DEFAULT 0,
        
        [IndiceSemestral] DECIMAL(4,2) NULL,
        [IndiceAcumulado] DECIMAL(4,2) NULL,
        [FechaRegistro] DATETIME DEFAULT GETDATE(),

        FOREIGN KEY ([IdPersona]) REFERENCES [dbo].[Dim_Estudiante]([IdPersona]),
        FOREIGN KEY ([IdCarrera]) REFERENCES [dbo].[Dim_Carrera]([IdCarrera]),
        FOREIGN KEY ([IdAsignatura]) REFERENCES [dbo].[Dim_Asignatura]([IdAsignatura]),
        FOREIGN KEY ([IdPeriodo]) REFERENCES [dbo].[Dim_Periodo]([IdPeriodo])
    );
    
    -- Índices para mejorar rendimiento de queries analíticos
    CREATE NONCLUSTERED INDEX IX_FactCalificaciones_Persona ON [dbo].[Fact_Calificaciones] ([IdPersona]);
    CREATE NONCLUSTERED INDEX IX_FactCalificaciones_Periodo ON [dbo].[Fact_Calificaciones] ([IdPeriodo]);
END
GO

-- Tabla: Fact_Inscripciones (Materias oficialmente inscritas y seleccionadas actuales)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Fact_Inscripciones]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Fact_Inscripciones] (
        [IdInscripcion] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [IdPersona] INT NOT NULL,
        [IdPeriodo] INT NOT NULL,
        [IdAsignatura] INT NOT NULL,
        
        [Tipo] NVARCHAR(50) NOT NULL, -- Ej: "Oficial", "Prematricula" o "Seleccionada"
        [Seccion] NVARCHAR(20) NULL,
        [Profesor] NVARCHAR(200) NULL,
        [FechaRegistro] DATETIME DEFAULT GETDATE(),

        FOREIGN KEY ([IdPersona]) REFERENCES [dbo].[Dim_Estudiante]([IdPersona]),
        FOREIGN KEY ([IdPeriodo]) REFERENCES [dbo].[Dim_Periodo]([IdPeriodo]),
        FOREIGN KEY ([IdAsignatura]) REFERENCES [dbo].[Dim_Asignatura]([IdAsignatura])
    );
END
GO


-- ========================================================================
-- VISTAS PARA ANÁLISIS (DASHBOARDS BI)
-- ========================================================================

-- Vista: Rendimiento Estudiantil (Créditos Aprobados vs Evaluados)
GO
CREATE OR ALTER VIEW [dbo].[vw_RendimientoEstudiantil] AS
SELECT 
    E.Matricula,
    E.NombreCompleto,
    C.NombreCarrera,
    COUNT(F.IdAsignatura) AS TotalMateriasCursadas,
    SUM(CASE WHEN F.Aprobada = 1 THEN A.Creditos ELSE 0 END) AS CreditosAprobados,
    SUM(CASE WHEN F.Estatus IN ('Completada', 'Fallada') THEN A.Creditos ELSE 0 END) AS CreditosEvaluados,
    MAX(F.IndiceAcumulado) AS IndiceAcumuladoActual
FROM [dbo].[Fact_Calificaciones] F
JOIN [dbo].[Dim_Estudiante] E ON F.IdPersona = E.IdPersona
JOIN [dbo].[Dim_Carrera] C ON F.IdCarrera = C.IdCarrera
JOIN [dbo].[Dim_Asignatura] A ON F.IdAsignatura = A.IdAsignatura
GROUP BY 
    E.Matricula, E.NombreCompleto, C.NombreCarrera;
GO

-- Vista: Demanda de Asignaturas (Materias Pendientes y Prematriculadas)
CREATE OR ALTER VIEW [dbo].[vw_DemandaAsignaturas] AS
SELECT 
    A.Codigo,
    A.Descripcion AS Materia,
    A.Creditos,
    P.Ano,
    P.NumeroPeriodo,
    COUNT(I.IdPersona) AS TotalEstudiantesInteresados,
    I.Tipo AS EstatusSeleccion
FROM [dbo].[Fact_Inscripciones] I
JOIN [dbo].[Dim_Asignatura] A ON I.IdAsignatura = A.IdAsignatura
JOIN [dbo].[Dim_Periodo] P ON I.IdPeriodo = P.IdPeriodo
GROUP BY 
    A.Codigo, A.Descripcion, A.Creditos, P.Ano, P.NumeroPeriodo, I.Tipo;
GO
