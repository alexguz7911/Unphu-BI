-- ========================================================================
-- DATABASE SCHEMA PARA UNPHU-BI (POSTGRESQL)
-- ========================================================================

-- Nota: En PostgreSQL crea la base de datos "UnphuBI_DB" primero o usa la default.
-- Conéctate a esa base de datos antes de ejecutar este script.

-- ========================================================================
-- DIMENSIONES (Catálogos estáticos / maestros)
-- ========================================================================

-- Tabla: Dim_Periodo (Semestres y Años)
CREATE TABLE IF NOT EXISTS Dim_Periodo (
    IdPeriodo INT NOT NULL PRIMARY KEY,
    Ano INT NOT NULL,
    NumeroPeriodo INT NOT NULL, -- Ej: 1, 2, 3
    Descripcion VARCHAR(100) NULL, -- Ej: "Enero-Abril 2025"
    EsPeriodoActual BOOLEAN DEFAULT FALSE,
    FechaInicio DATE NULL,
    FechaFin DATE NULL
);

-- Tabla: Dim_Carrera
CREATE TABLE IF NOT EXISTS Dim_Carrera (
    IdCarrera INT NOT NULL PRIMARY KEY,
    NombreCarrera VARCHAR(200) NOT NULL,
    Facultad VARCHAR(200) NULL
);

-- Tabla: Dim_Asignatura (Materias del pensum)
CREATE TABLE IF NOT EXISTS Dim_Asignatura (
    IdAsignatura INT NOT NULL PRIMARY KEY,
    Codigo VARCHAR(20) NOT NULL,
    Descripcion VARCHAR(200) NOT NULL,
    Creditos INT NOT NULL,
    Area VARCHAR(100) NULL
);

-- Tabla: Dim_Estudiante (Datos de la Persona)
CREATE TABLE IF NOT EXISTS Dim_Estudiante (
    IdPersona INT NOT NULL PRIMARY KEY,
    Matricula VARCHAR(50) NOT NULL UNIQUE,
    NombreCompleto VARCHAR(200) NOT NULL,
    EmailInstitucional VARCHAR(150) NULL,
    IdCarreraActiva INT NULL,
    FOREIGN KEY (IdCarreraActiva) REFERENCES Dim_Carrera(IdCarrera)
);


-- ========================================================================
-- TABLAS DE HECHOS (Transaccionales / Historial)
-- ========================================================================

-- Tabla: Fact_Calificaciones (Historial de notas por estudiante y materia)
CREATE TABLE IF NOT EXISTS Fact_Calificaciones (
    IdCalificacion SERIAL PRIMARY KEY,
    IdPersona INT NOT NULL,
    IdCarrera INT NOT NULL,
    IdAsignatura INT NOT NULL,
    IdPeriodo INT NOT NULL,
    
    Estatus VARCHAR(50) NOT NULL, -- Ej: "Completada", "Pendiente", "Retirada"
    NotaLiteral VARCHAR(10) NULL, -- Ej: A, B, C, F, R
    NotaNumerica DECIMAL(5,2) NULL,
    PuntosHonor DECIMAL(5,2) NULL,
    Aprobada SMALLINT NOT NULL DEFAULT 0,
    
    IndiceSemestral DECIMAL(4,2) NULL,
    IndiceAcumulado DECIMAL(4,2) NULL,
    FechaRegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (IdPersona) REFERENCES Dim_Estudiante(IdPersona),
    FOREIGN KEY (IdCarrera) REFERENCES Dim_Carrera(IdCarrera),
    FOREIGN KEY (IdAsignatura) REFERENCES Dim_Asignatura(IdAsignatura),
    FOREIGN KEY (IdPeriodo) REFERENCES Dim_Periodo(IdPeriodo)
);

-- Índices para mejorar rendimiento de queries analíticos
CREATE INDEX IF NOT EXISTS IX_FactCalificaciones_Persona ON Fact_Calificaciones (IdPersona);
CREATE INDEX IF NOT EXISTS IX_FactCalificaciones_Periodo ON Fact_Calificaciones (IdPeriodo);


-- Tabla: Fact_Inscripciones (Materias oficialmente inscritas y seleccionadas actuales)
CREATE TABLE IF NOT EXISTS Fact_Inscripciones (
    IdInscripcion SERIAL PRIMARY KEY,
    IdPersona INT NOT NULL,
    IdPeriodo INT NOT NULL,
    IdAsignatura INT NOT NULL,
    
    Tipo VARCHAR(50) NOT NULL, -- Ej: "Oficial", "Prematricula" o "Seleccionada"
    Seccion VARCHAR(20) NULL,
    Profesor VARCHAR(200) NULL,
    FechaRegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (IdPersona) REFERENCES Dim_Estudiante(IdPersona),
    FOREIGN KEY (IdPeriodo) REFERENCES Dim_Periodo(IdPeriodo),
    FOREIGN KEY (IdAsignatura) REFERENCES Dim_Asignatura(IdAsignatura)
);

-- ========================================================================
-- VISTAS PARA ANÁLISIS (DASHBOARDS BI)
-- ========================================================================

-- Vista: Rendimiento Estudiantil (Créditos Aprobados vs Evaluados)
CREATE OR REPLACE VIEW vw_RendimientoEstudiantil AS
SELECT 
    E.Matricula,
    E.NombreCompleto,
    C.NombreCarrera,
    COUNT(F.IdAsignatura) AS TotalMateriasCursadas,
    SUM(CASE WHEN F.Aprobada = 1 THEN A.Creditos ELSE 0 END) AS CreditosAprobados,
    SUM(CASE WHEN F.Estatus IN ('Completada', 'Fallada') THEN A.Creditos ELSE 0 END) AS CreditosEvaluados,
    MAX(F.IndiceAcumulado) AS IndiceAcumuladoActual
FROM Fact_Calificaciones F
JOIN Dim_Estudiante E ON F.IdPersona = E.IdPersona
JOIN Dim_Carrera C ON F.IdCarrera = C.IdCarrera
JOIN Dim_Asignatura A ON F.IdAsignatura = A.IdAsignatura
GROUP BY 
    E.Matricula, E.NombreCompleto, C.NombreCarrera;

-- Vista: Demanda de Asignaturas (Materias Pendientes y Prematriculadas)
CREATE OR REPLACE VIEW vw_DemandaAsignaturas AS
SELECT 
    A.Codigo,
    A.Descripcion AS Materia,
    A.Creditos,
    P.Ano,
    P.NumeroPeriodo,
    COUNT(I.IdPersona) AS TotalEstudiantesInteresados,
    I.Tipo AS EstatusSeleccion
FROM Fact_Inscripciones I
JOIN Dim_Asignatura A ON I.IdAsignatura = A.IdAsignatura
JOIN Dim_Periodo P ON I.IdPeriodo = P.IdPeriodo
GROUP BY 
    A.Codigo, A.Descripcion, A.Creditos, P.Ano, P.NumeroPeriodo, I.Tipo;
