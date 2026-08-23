# 🔄 DBComparer

<div align="center">

**Herramienta profesional de sincronización de bases de datos**

[![Delphi](https://img.shields.io/badge/Delphi-10.4%2B-red?style=flat-square&logo=delphi)](https://www.embarcadero.com/products/delphi)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)
[![UniDAC](https://img.shields.io/badge/Devart-UniDAC-green?style=flat-square)](https://www.devart.com/unidac/)

*Compara, sincroniza y migra esquemas y datos entre dos bases de datos de un mismo motor con un solo comando.*

[Español](#-español) • [English](#-english) • [Français](#-français) • [Deutsch](#-deutsch) • [中文](#-中文) • [한국어](#-한국어) • [العربية](#-العربية) • [Hrvatski](#-hrvatski)

</div>

---

<details>
<summary><strong>🇪🇸 Español (Original)</strong></summary>
 # 🔄 DBComparer

<div align="center">

**Herramienta profesional de sincronización de bases de datos**

[![Delphi](https://img.shields.io/badge/Delphi-10.4%2B-red?style=flat-square&logo=delphi)](https://www.embarcadero.com/products/delphi)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)
[![UniDAC](https://img.shields.io/badge/Devart-UniDAC-green?style=flat-square)](https://www.devart.com/unidac/)

*Compara, sincroniza y migra esquemas y datos entre dos bases de datos de un mismo motor con un solo comando.*

[Características](#-características) • [Instalación](#-compilación) • [Uso](#-uso-rápido) • [Ejemplos](#-ejemplos-por-motor) • [Licencia](#-licencia)

</div>

---

## 📋 Tabla de Contenidos

- [Descripción](#-descripción)
- [Características](#-características)
- [Bases de Datos Soportadas](#-bases-de-datos-soportadas)
- [Requisitos](#-requisitos-importantes)
- [Compilación](#-compilación)
- [Uso Rápido](#-uso-rápido)
- [Ejemplos por Motor](#-ejemplos-por-motor)
- [Opciones Avanzadas](#-opciones-de-línea-de-comandos)
- [Casos de Uso](#-casos-de-uso-comunes)
- [Licencia](#-licencia)
- [Disclaimer](#-disclaimer)

---

## 🎯 Descripción

**DBComparer** es una suite de herramientas de línea de comandos desarrollada en **Delphi** que permite a DBAs y desarrolladores:

- ✅ Comparar esquemas entre bases de datos heterogéneas
- ✅ Generar scripts DDL de sincronización automática
- ✅ Sincronizar datos de forma inteligente (INSERT/UPDATE/DELETE)
- ✅ Mantener entornos (Dev ➡️ QA ➡️ Prod) actualizados
- ✅ Automatizar despliegues con seguridad y control

---

## ✨ Características

### 🏗️ **Sincronización de Estructura (Schema Diff)**

| Elemento | Funcionalidad |
|----------|---------------|
| **Tablas** | Creación, nuevas columnas, modificación de tipos y nulabilidad |
| **Índices** | Primary Keys, Unique, índices secundarios |
| **Vistas** | Comparación y recreación automática |
| **Procedimientos** | Stored Procedures y Funciones |
| **Triggers** | Sincronización opcional con `--with-triggers` |
| **Secuencias** | Generadores y Secuencias (estrategia "crear si no existe") |

### 📊 **Sincronización de Datos (Data Diff)**

| Modo | Descripción | Opción |
|------|-------------|--------|
| **Copia Simple** | Volcado masivo de datos (`INSERT`) | `--with-data` |
| **Sincronización Inteligente** | Comparación por PK: `INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **Seguridad y Control**

- 🛡️ **Modo Seguro** (`--nodelete`): Evita borrados accidentales
- 🎯 **Lista Blanca** (`--include-tables`): Sincroniza solo tablas específicas
- 🚫 **Lista Negra** (`--exclude-tables`): Excluye tablas del proceso
- 📝 **Scripts SQL**: Genera archivos `.sql` para revisión antes de ejecutar

---

## 🗄️ Bases de Datos Soportadas

<div align="center">

| Base de Datos | Versiones Soportadas | Ejecutable | Características / Notas |
| :--- | :--- | :--- | :--- |
| ![MySQL](https://img.shields.io/badge/MySQL-005C84?style=flat-square&logo=mysql&logoColor=white) | 5.7+ / MariaDB 10+ | `DBComparer.exe` | Modo manual dirigido a MariaDB 10.2 con `--mariadb10` |
| ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat-square&logo=postgresql&logoColor=white) | 9.6+ | `DBComparerPostGre.exe` | Soporte de esquemas |
| ![Oracle](https://img.shields.io/badge/Oracle-F80000?style=flat-square&logo=oracle&logoColor=white) | 11g+ | `DBComparerOracle.exe` | TNS Names, Propietarios |
| ![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=flat-square&logo=microsoft-sql-server&logoColor=white) | 2012+ | `DBComparerSQLServer.exe` | Columnas Identity |
| ![Firebird](https://img.shields.io/badge/Firebird-FF6600?style=flat-square&logo=firebird&logoColor=white) | 2.5+ / InterBase | `DBComparerInterbase.exe` | Generadores, Dialectos |

</div>

---

## ⚠️ Requisitos Importantes

### 📦 Dependencias

Este proyecto utiliza **[Devart UniDAC](https://www.devart.com/unidac/)** para conectividad universal.

> **⚖️ Nota sobre Licencias:**
> 
> - El código fuente de **DBComparer** se distribuye bajo licencia **MIT** (uso libre).
> - Para **compilar** el proyecto necesitas una **licencia comercial válida de Devart UniDAC**.
> - Los archivos fuente de UniDAC (`.dcu`, `.pas`) **NO** están incluidos en este repositorio.

### 🖥️ Requisitos de Sistema

- **Delphi**: 10.4 Sydney o superior (11 Alexandria, 12 Athens)
- **Devart UniDAC**: Instalado en el IDE de Delphi
- **Windows**: 7/8/10/11 (32-bit o 64-bit)

---

## 🛠️ Compilación

1. **Clona el repositorio:**
   ```bash
   git clone https://github.com/alexdelphiAthn/DBComparer.git
   cd DBComparer
   ```

2. **Abre el proyecto en Delphi:**
   - Selecciona el archivo `.dpr` del motor deseado:
     - `DBComparer.dpr` → MySQL/MariaDB
     - `DBComparerPostGre.dpr` → PostgreSQL
     - `DBComparerOracle.dpr` → Oracle
     - `DBComparerSQLServer.dpr` → SQL Server
     - `DBComparerInterbase.dpr` → InterBase/Firebird

3. **Compila el proyecto:**
   - Modo: **Release**
   - Plataforma: **Win32** o **Win64**

4. **Ejecutable generado:**
   ```
   DBComparer\Win32\Release\DBComparer.exe
   ```

---

## 🚀 Uso Rápido

### Sintaxis General

```bash
# Por defecto se escribe en stdout; use --output o redirija con >
Ejecutable.exe <Origen> <Destino> [Opciones] --output=archivo_script.sql
Ejecutable.exe <Origen> <Destino> [Opciones] > archivo_script.sql
```

### Formato de Conexión

```
servidor:puerto\base_datos usuario\password 
```

### Ejemplo Básico

```bash
# Sincronizar solo estructura
DBComparer.exe localhost:3306\produccion root\pass localhost:3306\desarrollo root\pass > script_only_estructure.sql

# Sincronizar estructura + datos
DBComparer.exe localhost:3306\produccion root\pass localhost:3306\desarrollo root\pass --with-data-diff > script_data_structure.sql
```

---

## 💡 Ejemplos por Motor

### 🐬 MySQL / MariaDB

```bash
# Sincronización completa con datos
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > script_incremental.sql

# Solo tablas específicas
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=usuarios,productos > script_withproducts.sql

# Excluir tablas de logs
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logs,auditoria > script_withnolog.sql

# Destino MariaDB 10.2: compatibilidad con --nodelete y vistas seleccionadas intactas
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom
```

### 🐘 PostgreSQL

```bash
# Formato: servidor:puerto\base\esquema
DBComparerPostGre.exe localhost:5432\ventas\public postgres\pass localhost:5432\ventas\test postgres\pass > script.sql

# Esquema personalizado
DBComparerPostGre.exe prod-server:5432\erp\contabilidad admin\pass dev-server:5432\erp\contabilidad admin\pass > script_schem.sql
```

### 🔴 Oracle Database

```bash
# Conexión directa: host:port/SID user/pass@Owner
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > script.sql

# Usando TNS Names
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > script_tns.sql

# Con Owner específico
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > script_owner.sql
```

### 🟦 Microsoft SQL Server

```bash
# Protege tablas, columnas, índices y registros sobrantes frente a borrados
DBComparerSQLServer.exe sqlserver:1433\Produccion sa\pass sqlserver:1433\Desarrollo sa\pass --nodelete > script_safe.sql

# Con triggers y datos
DBComparerSQLServer.exe sqlserver:1433\Produccion sa\pass sqlserver:1433\Desarrollo sa\pass --with-triggers --with-data-diff > script_withdata.sql
```

### 🔥 InterBase / Firebird

```bash
# Servidor remoto
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\scrips\update_script.sql

# Archivo local (embebido)
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\script.sql
```

---

## ⚙️ Opciones de Línea de Comandos

| Opción | Descripción |
|--------|-------------|
| `--nodelete` | 🛡️ **Modo Seguro**: No elimina tablas, columnas, índices ni registros en destino |
| `--with-triggers` | 🔫 Incluye comparación y recreación de Triggers |
| `--with-data` | 📥 Copia masiva de datos (solo `INSERT`) |
| `--with-data-diff` | 🔄 Sincronización inteligente por PK (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **Lista Blanca**: Solo procesa las tablas especificadas |
| `--exclude-tables=t1,t2` | ❌ **Lista Negra**: Excluye las tablas especificadas |
| `--mariadb10` | 🐬 Activa explícitamente la generación SQL compatible con MariaDB 10.2; no hay detección automática de versión. Solo `DBComparer.exe` |
| `--preserve-views=v1,v2` | Conserva las vistas indicadas: no las elimina ni las recrea. Los nombres no distinguen mayúsculas/minúsculas; si una vista no existe en destino, tampoco se crea |
| `--output=archivo.sql` | Guarda el script generado directamente en un archivo |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | Codificación usada con `--output`; el valor predeterminado es `utf8bom` |

`--preserve-views`, `--output` y `--encoding` están disponibles en todos los ejecutables de DBComparer.

`--with-data` y `--with-data-diff` son mutuamente excluyentes. El primero genera un `INSERT` por cada fila de origen sin compararla con destino. El segundo compara por clave primaria; omite las tablas existentes sin PK y, combinado con `--nodelete`, no genera los `DELETE` de registros sobrantes. Los filtros `--include-tables` y `--exclude-tables` se aplican tanto a la estructura como a los datos de las tablas; si una tabla aparece en ambas listas, prevalece `--exclude-tables`.

### Opciones comunes de vistas y salida

`--preserve-views` omite por completo cada vista indicada: no la elimina, no la recrea y, si falta en destino, no la crea. La comparación de nombres no distingue mayúsculas/minúsculas. Incluye en la lista todas las vistas relacionadas que deban permanecer intactas.

Sin `--output`, el SQL se escribe en la salida estándar. `--encoding` solo controla el archivo creado con `--output`; al redirigir con `>`, la codificación depende de la consola. `unicode` genera UTF-16 LE y `ansi` usa la página de códigos ANSI configurada en Windows.

### Compatibilidad con MariaDB 10.2

`--mariadb10` es un modo opcional y manual para generar un script más compatible con MariaDB 10.2. Al activarlo, DBComparer:

- Usa `IF NOT EXISTS` al crear tablas y al añadir columnas e índices secundarios; las claves primarias usan una comprobación dinámica. Mantiene la posición de las columnas con `FIRST`/`AFTER`.
- Incluye los índices en las tablas nuevas, conserva su motor y collation, y deriva el juego de caracteres de esa collation; si faltan metadatos, usa `InnoDB` y `utf8mb4_spanish_ci`.
- Normaliza `CURRENT_TIMESTAMP()`, `utf8mb3`, las collations `utf8mb4_uca1400_ai_ci` y `utf8mb3_uca1400_ai_ci`, y determinados `CAST(... AS CHAR CHARSET UTF8MB4)`.
- Reproduce columnas generadas `VIRTUAL`/`STORED`; en este modo también normaliza sus expresiones.
- Ajusta `SQL_NOTES`, `FOREIGN_KEY_CHECKS`, `SQL_MODE` y `SET NAMES` para ejecutar el script; restaura los tres primeros al finalizar.
- Respeta el `SQL_MODE` de origen al recrear procedimientos y funciones. Los triggers siguen requiriendo `--with-triggers`.

El modo mejora la idempotencia del DDL de creación, pero no hace que todo el script sea inocuo o repetible: todavía puede contener `DROP`, `MODIFY`, cambios de datos y recreaciones. `--mariadb10` no implica `--nodelete`; incluso con `--nodelete`, las vistas, rutinas y triggers modificados pueden eliminarse y recrearse. Revisa el SQL generado y realiza un backup antes de ejecutarlo.

La conversión a `utf8mb4_spanish_ci` puede cambiar las reglas de comparación y ordenación respecto al origen. Además, `SET NAMES` no se restaura al final del script, aunque sí se restauran `SQL_NOTES`, `FOREIGN_KEY_CHECKS` y `SQL_MODE`.

### Combinaciones Útiles

```bash
# Protege tablas, columnas, índices y registros sobrantes frente a borrados
--nodelete --with-data-diff

# Sincronización completa con triggers
--with-triggers --with-data-diff

# Solo migrar datos de tablas maestras
--include-tables=clientes,productos,categorias --with-data

# MariaDB 10.2 con --nodelete, conservando vistas gestionadas externamente
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

# SQL Server: conservar una vista y guardar el script sin BOM
DBComparerSQLServer.exe servidor\origen usuario\clave servidor\destino usuario\clave --preserve-views=vw_legacy --output=script.sql --encoding=utf8nobom
```

---

## 🎯 Casos de Uso Comunes

### 1. **Despliegue Dev ➡️ Producción**
```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > script.sql
```

### 2. **Clonar Estructura sin Datos**
```bash
DBComparer.exe source:3306\db user\pass target:3306\db user\pass > script.sql
```

### 3. **Replicar Tablas Maestras**
```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=paises,provincias,categorias --with-data > script.sql
```

### 4. **Sincronización Continua (CI/CD)**
```bash
# En un script de Jenkins/GitLab CI
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > script.sql
```

### 5. **Migración entre Motores Diferentes**
```bash
# MySQL ➡️ PostgreSQL (requiere exportar/importar manualmente)
# DBComparer genera los scripts DDL compatibles
```

---

## 📄 Licencia

Este proyecto está licenciado bajo la **Licencia MIT**.

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

Se concede permiso, de forma gratuita, a cualquier persona que obtenga una copia
de este software y archivos de documentación asociados (el "Software"), para 
utilizar el Software sin restricción...
```

Consulta el archivo [LICENSE](LICENSE) para más detalles.

---

## ⚠️ Disclaimer

> **Este software se proporciona "TAL CUAL", sin garantía de ningún tipo**, expresa o implícita, incluyendo, pero no limitándose a, las garantías de comercialización, idoneidad para un propósito particular y no infracción.
>
> **⚠️ RECOMENDACIÓN CRÍTICA:**
> 
> Realiza **copias de seguridad completas** de tu base de datos de destino antes de ejecutar cualquier script de sincronización en **entornos de producción**.

---

## 🤝 Contribuciones

¡Las contribuciones son bienvenidas! Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

---

## 📞 Soporte

- 🐛 **Issues**: [GitHub Issues](https://github.com/alexdelphiAthn/DBComparer/issues)
- 📧 **Email**: alejandro.laorden@protonmail.com
- 💬 **Discusiones**: [GitHub Discussions](https://github.com/alexdelphiAthn/DBComparer/discussions)

---

<div align="center">

**Hecho con ❤️ usando Delphi y Devart UniDAC**

⭐ Si este proyecto te resulta útil, ¡considera darle una estrella en GitHub!

</div>
</details>

<details>
<summary><strong>🇺🇸 English</strong></summary>

## 📋 Table of Contents
# 🔄 DBComparer

<div align="center">

**Professional Database Synchronization Tool**

*Compare, synchronize, and migrate schemas and data between two databases of the same engine with a single command.*

[Features](https://www.google.com/search?q=%23-features) • [Installation](https://www.google.com/search?q=%23-compilation) • [Usage](https://www.google.com/search?q=%23-quick-usage) • [Examples](https://www.google.com/search?q=%23-examples-by-engine) • [License](https://www.google.com/search?q=%23-license)

</div>

---

## 📋 Table of Contents

* [Description](https://www.google.com/search?q=%23-description)
* [Features](https://www.google.com/search?q=%23-features)
* [Supported Databases](https://www.google.com/search?q=%23-supported-databases)
* [Requirements](https://www.google.com/search?q=%23-important-requirements)
* [Compilation](https://www.google.com/search?q=%23-compilation)
* [Quick Usage](https://www.google.com/search?q=%23-quick-usage)
* [Examples by Engine](https://www.google.com/search?q=%23-examples-by-engine)
* [Advanced Options](https://www.google.com/search?q=%23-command-line-options)
* [Use Cases](https://www.google.com/search?q=%23-common-use-cases)
* [License](https://www.google.com/search?q=%23-license)
* [Disclaimer](https://www.google.com/search?q=%23-disclaimer)

---

## 🎯 Description

**DBComparer** is a command-line tool suite developed in **Delphi** that allows DBAs and developers to:

* ✅ Compare schemas between heterogeneous databases
* ✅ Generate automatic synchronization DDL scripts
* ✅ Intelligently synchronize data (INSERT/UPDATE/DELETE)
* ✅ Keep environments (Dev ➡️ QA ➡️ Prod) up to date
* ✅ Automate deployments with safety and control

---

## ✨ Features

### 🏗️ **Structure Synchronization (Schema Diff)**

| Element | Functionality |
| --- | --- |
| **Tables** | Creation, new columns, type modification, and nullability |
| **Indexes** | Primary Keys, Unique, secondary indexes |
| **Views** | Comparison and automatic recreation |
| **Procedures** | Stored Procedures and Functions |
| **Triggers** | Optional synchronization with `--with-triggers` |
| **Sequences** | Generators and Sequences ("create if not exists" strategy) |

### 📊 **Data Synchronization (Data Diff)**

| Mode | Description | Option |
| --- | --- | --- |
| **Simple Copy** | Bulk data dump (`INSERT`) | `--with-data` |
| **Smart Sync** | PK-based comparison: `INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **Security and Control**

* 🛡️ **Safe Mode** (`--nodelete`): Prevents accidental deletions
* 🎯 **Whitelist** (`--include-tables`): Sync only specific tables
* 🚫 **Blacklist** (`--exclude-tables`): Exclude tables from the process
* 📝 **SQL Scripts**: Generates `.sql` files for review before execution

---

## 🗄️ Supported Databases

<div align="center">

| Engine | Version | Executable | Special Features |
| --- | --- | --- | --- |
| MySQL / MariaDB | 5.7+ / MariaDB 10+ | `DBComparer.exe` | Manual mode targeting MariaDB 10.2 with `--mariadb10` |
| PostgreSQL | 9.6+ | `DBComparerPostGre.exe` | Schema support |
| Oracle | 11g+ | `DBComparerOracle.exe` | TNS Names, Owners |
| SQL Server | 2012+ | `DBComparerSQLServer.exe` | Identity columns |
| Firebird / InterBase | 2.5+ / InterBase | `DBComparerInterbase.exe` | Generators, Dialects |

</div>

---

## ⚠️ Important Requirements

### 📦 Dependencies

This project uses **[Devart UniDAC](https://www.devart.com/unidac/)** for universal connectivity.

> **⚖️ License Note:**
> * The source code of **DBComparer** is distributed under the **MIT** license (free use).
> * To **compile** the project, you need a **valid commercial license for Devart UniDAC**.
> * UniDAC source files (`.dcu`, `.pas`) are **NOT** included in this repository.
> 
> 

### 🖥️ System Requirements

* **Delphi**: 10.4 Sydney or higher (11 Alexandria, 12 Athens)
* **Devart UniDAC**: Installed in the Delphi IDE
* **Windows**: 7/8/10/11 (32-bit or 64-bit)

---

## 🛠️ Compilation

1. **Clone the repository:**
```bash
git clone https://github.com/alexdelphiAthn/DBComparer.git
cd DBComparer

```


2. **Open the project in Delphi:**
* Select the `.dpr` file for the desired engine:
* `DBComparer.dpr` → MySQL/MariaDB
* `DBComparerPostGre.dpr` → PostgreSQL
* `DBComparerOracle.dpr` → Oracle
* `DBComparerSQLServer.dpr` → SQL Server
* `DBComparerInterbase.dpr` → InterBase/Firebird




3. **Compile the project:**
* Mode: **Release**
* Platform: **Win32** or **Win64**


4. **Generated executable:**
```
DBComparer\Win32\Release\DBComparer.exe

```



---

## 🚀 Quick Usage

### General Syntax

```bash
# By default the script is written to stdout; use --output or redirect with >
Executable.exe <Source> <Target> [Options] --output=script_file.sql
Executable.exe <Source> <Target> [Options] > script_file.sql

```

### Connection Format

```
server:port\database user\password 

```

### Basic Example

```bash
# Sync structure only
DBComparer.exe localhost:3306\production root\pass localhost:3306\development root\pass > script_only_structure.sql

# Sync structure + data
DBComparer.exe localhost:3306\production root\pass localhost:3306\development root\pass --with-data-diff > script_data_structure.sql

```

---

## 💡 Examples by Engine

### 🐬 MySQL / MariaDB

```bash
# Full sync with data
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > script_incremental.sql

# Only specific tables
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=users,products > script_withproducts.sql

# Exclude log tables
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logs,audit > script_withnolog.sql

# MariaDB 10.2 target: compatibility with --nodelete and selected views preserved
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom

```

### 🐘 PostgreSQL

```bash
# Format: server:port\database\schema
DBComparerPostGre.exe localhost:5432\sales\public postgres\pass localhost:5432\sales\test postgres\pass > script.sql

# Custom schema
DBComparerPostGre.exe prod-server:5432\erp\accounting admin\pass dev-server:5432\erp\accounting admin\pass > script_schem.sql

```

### 🔴 Oracle Database

```bash
# Direct connection: host:port/SID user/pass@Owner
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > script.sql

# Using TNS Names
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > script_tns.sql

# With specific Owner
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > script_owner.sql

```

### 🟦 Microsoft SQL Server

```bash
# Protect target-only tables, columns, indexes, and rows from deletion
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --nodelete > script_safe.sql

# With triggers and data
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --with-triggers --with-data-diff > script_withdata.sql

```

### 🔥 InterBase / Firebird

```bash
# Remote server
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\scripts\update_script.sql

# Local file (embedded)
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\script.sql

```

---

## ⚙️ Command Line Options

| Option | Description |
| --- | --- |
| `--nodelete` | 🛡️ **Safe Mode**: Does not delete tables, columns, indexes, or records in target |
| `--with-triggers` | 🔫 Includes comparison and recreation of Triggers |
| `--with-data` | 📥 Bulk data copy (`INSERT` only) |
| `--with-data-diff` | 🔄 Smart sync by PK (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **Whitelist**: Only process specified tables |
| `--exclude-tables=t1,t2` | ❌ **Blacklist**: Exclude specified tables |
| `--mariadb10` | 🐬 Opt-in SQL generation compatible with MariaDB 10.2; the server version is not auto-detected. `DBComparer.exe` only |
| `--preserve-views=v1,v2` | Skips the listed views: they are neither dropped nor recreated, and missing views are not created. Names are case-insensitive |
| `--output=file.sql` | Saves the generated script directly to a file |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | Output encoding used with `--output`; defaults to `utf8bom` |

`--preserve-views`, `--output`, and `--encoding` are available in every DBComparer executable.

`--with-data` and `--with-data-diff` are mutually exclusive. The first generates one `INSERT` per source row without comparing it with the target. The second compares rows by primary key; existing tables without a PK are skipped and, when combined with `--nodelete`, no `DELETE` statements are generated for target-only rows. The `--include-tables` and `--exclude-tables` filters apply to both table schema and table data; if a table appears in both lists, `--exclude-tables` takes precedence.

### Common view and output options

`--preserve-views` skips every listed view completely: it is not dropped, recreated, or created when missing from the target. Names are compared case-insensitively. Include every related view that must remain untouched.

Without `--output`, SQL is written to standard output. `--encoding` only controls the file created by `--output`; when redirecting with `>`, encoding is controlled by the console. `unicode` produces UTF-16 LE, while `ansi` uses the configured Windows ANSI code page.

### MariaDB 10.2 compatibility

`--mariadb10` is an optional, manually enabled mode that generates a script better suited to MariaDB 10.2. When enabled, DBComparer:

- Uses `IF NOT EXISTS` for table creation and for adding columns and secondary indexes; primary keys use a dynamic existence check. Column order is retained with `FIRST`/`AFTER`.
- Includes indexes in new tables, preserves their engine and collation, and derives the character set from that collation; missing metadata falls back to `InnoDB` and `utf8mb4_spanish_ci`.
- Normalizes `CURRENT_TIMESTAMP()`, `utf8mb3`, the `utf8mb4_uca1400_ai_ci` and `utf8mb3_uca1400_ai_ci` collations, and selected `CAST(... AS CHAR CHARSET UTF8MB4)` expressions.
- Reproduces `VIRTUAL`/`STORED` generated columns and also normalizes their expressions in this mode.
- Adjusts `SQL_NOTES`, `FOREIGN_KEY_CHECKS`, `SQL_MODE`, and `SET NAMES` for script execution; the first three are restored at the end.
- Preserves the source `SQL_MODE` while recreating procedures and functions. Triggers still require `--with-triggers`.

This mode improves idempotency for creation DDL, but it does not make the entire script harmless or fully repeatable: it may still contain `DROP`, `MODIFY`, data changes, and object recreation. `--mariadb10` does not imply `--nodelete`; even with `--nodelete`, changed views, routines, and triggers may be dropped and recreated. Review the generated SQL and make a backup before running it.

Converting to `utf8mb4_spanish_ci` may change comparison and sorting rules relative to the source. Also, `SET NAMES` is not restored at the end of the script, although `SQL_NOTES`, `FOREIGN_KEY_CHECKS`, and `SQL_MODE` are restored.

### Useful Combinations

```bash
# Protect target-only tables, columns, indexes, and rows from deletion
--nodelete --with-data-diff

# Full sync with triggers
--with-triggers --with-data-diff

# Migrate only data for master tables
--include-tables=customers,products,categories --with-data

# MariaDB 10.2 with --nodelete while preserving externally managed views
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

```

---

## 🎯 Common Use Cases

### 1. **Deployment Dev ➡️ Production**

```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > script.sql

```

### 2. **Clone Structure without Data**

```bash
DBComparer.exe source:3306\db user\pass target:3306\db user\pass > script.sql

```

### 3. **Replicate Master Tables**

```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=countries,provinces,categories --with-data > script.sql

```

### 4. **Continuous Synchronization (CI/CD)**

```bash
# In a Jenkins/GitLab CI script
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > script.sql

```

### 5. **Migration between Different Engines**

```bash
# MySQL ➡️ PostgreSQL (requires manual export/import)
# DBComparer generates compatible DDL scripts

```

---

## 📄 License

This project is licensed under the **MIT License**.

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction...

```

See the [LICENSE](https://www.google.com/search?q=LICENSE) file for more details.

---

## ⚠️ Disclaimer

> **This software is provided "AS IS", without warranty of any kind**, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose and noninfringement.
> **⚠️ CRITICAL RECOMMENDATION:**
> Perform **full backups** of your target database before running any synchronization scripts in **production environments**.
> 
</details>

<details>
<summary><strong>🇫🇷 Français</strong></summary>

# 🔄 DBComparer

<div align="center">

**Outil professionnel de synchronisation de bases de données**

*Comparez, synchronisez et migrez des schémas et des données entre deux bases de données du même moteur en une seule commande.*

[Fonctionnalités](https://www.google.com/search?q=%23-fonctionnalit%C3%A9s) • [Installation](https://www.google.com/search?q=%23-compilation) • [Utilisation](https://www.google.com/search?q=%23-utilisation-rapide) • [Exemples](https://www.google.com/search?q=%23-exemples-par-moteur) • [Licence](https://www.google.com/search?q=%23-licence)

</div>

---

## 📋 Table des matières

* [Description](https://www.google.com/search?q=%23-description)
* [Fonctionnalités](https://www.google.com/search?q=%23-fonctionnalit%C3%A9s)
* [Bases de données supportées](https://www.google.com/search?q=%23-bases-de-donn%C3%A9es-support%C3%A9es)
* [Prérequis](https://www.google.com/search?q=%23-pr%C3%A9requis-importants)
* [Compilation](https://www.google.com/search?q=%23-compilation)
* [Utilisation rapide](https://www.google.com/search?q=%23-utilisation-rapide)
* [Exemples par moteur](https://www.google.com/search?q=%23-exemples-par-moteur)
* [Options avancées](https://www.google.com/search?q=%23-options-de-ligne-de-commande)
* [Cas d'utilisation](https://www.google.com/search?q=%23-cas-dutilisation-courants)
* [Licence](https://www.google.com/search?q=%23-licence)
* [Avertissement](https://www.google.com/search?q=%23-avertissement)

---

## 🎯 Description

**DBComparer** est une suite d'outils en ligne de commande développée en **Delphi** qui permet aux DBA et aux développeurs de :

* ✅ Comparer des schémas entre des bases de données hétérogènes
* ✅ Générer des scripts DDL de synchronisation automatique
* ✅ Synchroniser intelligemment les données (INSERT/UPDATE/DELETE)
* ✅ Maintenir les environnements (Dev ➡️ QA ➡️ Prod) à jour
* ✅ Automatiser les déploiements avec sécurité et contrôle

---

## ✨ Fonctionnalités

### 🏗️ **Synchronisation de la structure (Schema Diff)**

| Élément | Fonctionnalité |
| --- | --- |
| **Tables** | Création, nouvelles colonnes, modification des types et nullabilité |
| **Index** | Clés primaires, Uniques, index secondaires |
| **Vues** | Comparaison et recréation automatique |
| **Procédures** | Procédures stockées et fonctions |
| **Déclencheurs** | Synchronisation optionnelle avec `--with-triggers` |
| **Séquences** | Générateurs et Séquences (stratégie "créer si n'existe pas") |

### 📊 **Synchronisation des données (Data Diff)**

| Mode | Description | Option |
| --- | --- | --- |
| **Copie simple** | Copie massive (`INSERT`) | `--with-data` |
| **Sync intelligente** | Comparaison par PK : `INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **Sécurité et Contrôle**

* 🛡️ **Mode sans échec** (`--nodelete`) : Empêche les suppressions accidentelles
* 🎯 **Liste blanche** (`--include-tables`) : Synchronise uniquement les tables spécifiques
* 🚫 **Liste noire** (`--exclude-tables`) : Exclut les tables du processus
* 📝 **Scripts SQL** : Génère des fichiers `.sql` pour révision avant exécution

---

## 🗄️ Bases de données supportées

<div align="center">

| Moteur | Version | Exécutable | Fonctionnalités spéciales |
| --- | --- | --- | --- |
| MySQL / MariaDB | 5.7+ / MariaDB 10+ | `DBComparer.exe` | Mode manuel pour MariaDB 10.2 avec `--mariadb10` |
| PostgreSQL | 9.6+ | `DBComparerPostGre.exe` | Support des schémas |
| Oracle | 11g+ | `DBComparerOracle.exe` | TNS Names, Propriétaires |
| SQL Server | 2012+ | `DBComparerSQLServer.exe` | Colonnes d'identité |
| Firebird / InterBase | 2.5+ / InterBase | `DBComparerInterbase.exe` | Générateurs, Dialectes |

</div>

---

## ⚠️ Prérequis Importants

### 📦 Dépendances

Ce projet utilise **[Devart UniDAC](https://www.devart.com/unidac/)** pour la connectivité universelle.

> **⚖️ Note sur la licence :**
> * Le code source de **DBComparer** est distribué sous licence **MIT** (utilisation libre).
> * Pour **compiler** le projet, vous avez besoin d'une **licence commerciale valide pour Devart UniDAC**.
> * Les fichiers sources d'UniDAC (`.dcu`, `.pas`) ne sont **PAS** inclus dans ce dépôt.
> 
> 

### 🖥️ Configuration système

* **Delphi** : 10.4 Sydney ou supérieur (11 Alexandria, 12 Athens)
* **Devart UniDAC** : Installé dans l'IDE Delphi
* **Windows** : 7/8/10/11 (32-bit ou 64-bit)

---

## 🛠️ Compilation

1. **Cloner le dépôt :**
```bash
git clone https://github.com/alexdelphiAthn/DBComparer.git
cd DBComparer

```


2. **Ouvrir le projet dans Delphi :**
* Sélectionnez le fichier `.dpr` pour le moteur souhaité :
* `DBComparer.dpr` → MySQL/MariaDB
* `DBComparerPostGre.dpr` → PostgreSQL
* `DBComparerOracle.dpr` → Oracle
* `DBComparerSQLServer.dpr` → SQL Server
* `DBComparerInterbase.dpr` → InterBase/Firebird




3. **Compiler le projet :**
* Mode : **Release**
* Plateforme : **Win32** ou **Win64**


4. **Exécutable généré :**
```
DBComparer\Win32\Release\DBComparer.exe

```



---

## 🚀 Utilisation rapide

### Syntaxe générale

```bash
# Par défaut, le script est écrit sur stdout ; utilisez --output ou redirigez avec >
Executable.exe <Source> <Cible> [Options] --output=fichier_script.sql
Executable.exe <Source> <Cible> [Options] > fichier_script.sql

```

### Format de connexion

```
serveur:port\base_de_données utilisateur\motdepasse 

```

### Exemple de base

```bash
# Synchroniser la structure uniquement
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass > script_structure_seule.sql

# Synchroniser la structure + les données
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass --with-data-diff > script_data_structure.sql

```

---

## 💡 Exemples par moteur

### 🐬 MySQL / MariaDB

```bash
# Synchronisation complète avec données
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > script_incremental.sql

# Seulement des tables spécifiques
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=utilisateurs,produits > script_avecproduits.sql

# Exclure les tables de logs
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logs,audit > script_sanslog.sql

# Compatibilité MariaDB 10.2 avec --nodelete et certaines vues préservées
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom

```

### 🐘 PostgreSQL

```bash
# Format : serveur:port\base\schema
DBComparerPostGre.exe localhost:5432\ventes\public postgres\pass localhost:5432\ventes\test postgres\pass > script.sql

# Schéma personnalisé
DBComparerPostGre.exe prod-server:5432\erp\compta admin\pass dev-server:5432\erp\compta admin\pass > script_schema.sql

```

### 🔴 Oracle Database

```bash
# Connexion directe : hôte:port/SID utilisateur/pass@Propriétaire
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > script.sql

# Utilisation des noms TNS
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > script_tns.sql

# Avec un Propriétaire spécifique
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > script_owner.sql

```

### 🟦 Microsoft SQL Server

```bash
# Protège les tables, colonnes, index et enregistrements excédentaires contre la suppression
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Developpement sa\pass --nodelete > script_safe.sql

# Avec déclencheurs et données
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Developpement sa\pass --with-triggers --with-data-diff > script_withdata.sql

```

### 🔥 InterBase / Firebird

```bash
# Serveur distant
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\scripts\update_script.sql

# Fichier local (embarqué)
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\script.sql

```

---

## ⚙️ Options de ligne de commande

| Option | Description |
| --- | --- |
| `--nodelete` | 🛡️ **Mode sans échec** : Ne supprime pas de tables, colonnes, index ou enregistrements dans la cible |
| `--with-triggers` | 🔫 Inclut la comparaison et la recréation des Déclencheurs |
| `--with-data` | 📥 Copie massive des données (`INSERT` uniquement) |
| `--with-data-diff` | 🔄 Sync intelligente par PK (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **Liste blanche** : Traite uniquement les tables spécifiées |
| `--exclude-tables=t1,t2` | ❌ **Liste noire** : Exclut les tables spécifiées |
| `--mariadb10` | Active explicitement la génération SQL compatible avec MariaDB 10.2 ; aucune détection automatique. Uniquement avec `DBComparer.exe` |
| `--preserve-views=v1,v2` | Ignore les vues indiquées : elles ne sont ni supprimées ni recréées, et les vues absentes ne sont pas créées. Noms insensibles à la casse |
| `--output=fichier.sql` | Enregistre directement le script généré dans un fichier |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | Encodage utilisé avec `--output` ; `utf8bom` par défaut |

`--preserve-views`, `--output` et `--encoding` sont disponibles dans tous les exécutables DBComparer.

Pour les effets techniques et les limites, voir [MariaDB 10.2 compatibility](#mariadb-102-compatibility) (en anglais).

### Combinaisons utiles

```bash
# Protège les tables, colonnes, index et enregistrements excédentaires contre la suppression
--nodelete --with-data-diff

# Sync complète avec déclencheurs
--with-triggers --with-data-diff

# Migrer uniquement les données des tables maîtresses
--include-tables=clients,produits,categories --with-data

# MariaDB 10.2 avec --nodelete et les vues gérées séparément préservées
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

```

---

## 🎯 Cas d'utilisation courants

### 1. **Déploiement Dev ➡️ Production**

```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > script.sql

```

### 2. **Cloner la structure sans les données**

```bash
DBComparer.exe source:3306\db user\pass target:3306\db user\pass > script.sql

```

### 3. **Répliquer les tables maîtresses**

```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=pays,provinces,categories --with-data > script.sql

```

### 4. **Synchronisation continue (CI/CD)**

```bash
# Dans un script Jenkins/GitLab CI
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > script.sql

```

### 5. **Migration entre moteurs différents**

```bash
# MySQL ➡️ PostgreSQL (nécessite un export/import manuel)
# DBComparer génère les scripts DDL compatibles

```

---

## 📄 Licence

Ce projet est sous **Licence MIT**.

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

L'autorisation est accordée, gratuitement, à toute personne obtenant une copie
de ce logiciel et des fichiers de documentation associés (le "Logiciel"), de traiter
le Logiciel sans restriction...

```

Voir le fichier [LICENSE](https://www.google.com/search?q=LICENSE) pour plus de détails.

---

## ⚠️ Avertissement

> **Ce logiciel est fourni "TEL QUEL", sans garantie d'aucune sorte**, expresse ou implicite, y compris, mais sans s'y limiter, les garanties de qualité marchande, d'adéquation à un usage particulier et d'absence de contrefaçon.
> **⚠️ RECOMMANDATION CRITIQUE :**
> Effectuez des **sauvegardes complètes** de votre base de données cible avant d'exécuter des scripts de synchronisation dans des **environnements de production**.

</details>

<details>
<summary><strong>🇩🇪 Deutsch</strong></summary>
# 🔄 DBComparer

<div align="center">

**Professionelles Tool zur Datenbanksynchronisation**

*Vergleichen, synchronisieren und migrieren Sie Schemata und Daten zwischen zwei Datenbanken derselben Engine mit einem einzigen Befehl.*

[Funktionen](https://www.google.com/search?q=%23-funktionen) • [Installation](https://www.google.com/search?q=%23-kompilierung) • [Verwendung](https://www.google.com/search?q=%23-schnellstart) • [Beispiele](https://www.google.com/search?q=%23-beispiele-nach-engine) • [Lizenz](https://www.google.com/search?q=%23-lizenz)

</div>

---

## 📋 Inhaltsverzeichnis

* [Beschreibung](https://www.google.com/search?q=%23-beschreibung)
* [Funktionen](https://www.google.com/search?q=%23-funktionen)
* [Unterstützte Datenbanken](https://www.google.com/search?q=%23-unterst%C3%BCtzte-datenbanken)
* [Anforderungen](https://www.google.com/search?q=%23-wichtige-anforderungen)
* [Kompilierung](https://www.google.com/search?q=%23-kompilierung)
* [Schnellstart](https://www.google.com/search?q=%23-schnellstart)
* [Beispiele nach Engine](https://www.google.com/search?q=%23-beispiele-nach-engine)
* [Erweiterte Optionen](https://www.google.com/search?q=%23-befehlszeilenoptionen)
* [Anwendungsfälle](https://www.google.com/search?q=%23-h%C3%A4ufige-anwendungsf%C3%A4lle)
* [Lizenz](https://www.google.com/search?q=%23-lizenz)
* [Haftungsausschluss](https://www.google.com/search?q=%23-haftungsausschluss)

---

## 🎯 Beschreibung

**DBComparer** ist eine in **Delphi** entwickelte Befehlszeilen-Tool-Suite, die DBAs und Entwicklern Folgendes ermöglicht:

* ✅ Schemata zwischen heterogenen Datenbanken vergleichen
* ✅ Automatische Synchronisations-DDL-Skripte generieren
* ✅ Daten intelligent synchronisieren (INSERT/UPDATE/DELETE)
* ✅ Umgebungen (Dev ➡️ QA ➡️ Prod) aktuell halten
* ✅ Deployments mit Sicherheit und Kontrolle automatisieren

---

## ✨ Funktionen

### 🏗️ **Struktursynchronisation (Schema Diff)**

| Element | Funktionalität |
| --- | --- |
| **Tabellen** | Erstellung, neue Spalten, Änderung von Typen und Nullbarkeit |
| **Indizes** | Primärschlüssel, Unique, sekundäre Indizes |
| **Ansichten** | Vergleich und automatische Neuerstellung |
| **Prozeduren** | Gespeicherte Prozeduren und Funktionen |
| **Trigger** | Optionale Synchronisation mit `--with-triggers` |
| **Sequenzen** | Generatoren und Sequenzen (Strategie "erstellen, wenn nicht vorhanden") |

### 📊 **Datensynchronisation (Data Diff)**

| Modus | Beschreibung | Option |
| --- | --- | --- |
| **Einfache Kopie** | Massendaten-Dump (`INSERT`) | `--with-data` |
| **Smart Sync** | PK-basierter Vergleich: `INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **Sicherheit und Kontrolle**

* 🛡️ **Sicherer Modus** (`--nodelete`): Verhindert versehentliches Löschen
* 🎯 **Whitelist** (`--include-tables`): Synchronisiert nur bestimmte Tabellen
* 🚫 **Blacklist** (`--exclude-tables`): Schließt Tabellen vom Prozess aus
* 📝 **SQL-Skripte**: Generiert `.sql`-Dateien zur Überprüfung vor der Ausführung

---

## 🗄️ Unterstützte Datenbanken

<div align="center">

| Engine | Version | Ausführbare Datei | Besondere Funktionen |
| --- | --- | --- | --- |
| MySQL / MariaDB | 5.7+ / MariaDB 10+ | `DBComparer.exe` | Manueller MariaDB-10.2-Modus mit `--mariadb10` |
| PostgreSQL | 9.6+ | `DBComparerPostGre.exe` | Schema-Unterstützung |
| Oracle | 11g+ | `DBComparerOracle.exe` | TNS Names, Owner |
| SQL Server | 2012+ | `DBComparerSQLServer.exe` | Identity-Spalten |
| Firebird / InterBase | 2.5+ / InterBase | `DBComparerInterbase.exe` | Generatoren, Dialekte |

</div>

---

## ⚠️ Wichtige Anforderungen

### 📦 Abhängigkeiten

Dieses Projekt verwendet **[Devart UniDAC](https://www.devart.com/unidac/)** für universelle Konnektivität.

> **⚖️ Lizenzhinweis:**
> * Der Quellcode von **DBComparer** wird unter der **MIT**-Lizenz (freie Nutzung) verbreitet.
> * Um das Projekt zu **kompilieren**, benötigen Sie eine **gültige kommerzielle Lizenz für Devart UniDAC**.
> * UniDAC-Quelldateien (`.dcu`, `.pas`) sind **NICHT** in diesem Repository enthalten.
> 
> 

### 🖥️ Systemanforderungen

* **Delphi**: 10.4 Sydney oder höher (11 Alexandria, 12 Athens)
* **Devart UniDAC**: In der Delphi IDE installiert
* **Windows**: 7/8/10/11 (32-Bit oder 64-Bit)

---

## 🛠️ Kompilierung

1. **Repository klonen:**
```bash
git clone https://github.com/alexdelphiAthn/DBComparer.git
cd DBComparer

```


2. **Projekt in Delphi öffnen:**
* Wählen Sie die `.dpr`-Datei für die gewünschte Engine:
* `DBComparer.dpr` → MySQL/MariaDB
* `DBComparerPostGre.dpr` → PostgreSQL
* `DBComparerOracle.dpr` → Oracle
* `DBComparerSQLServer.dpr` → SQL Server
* `DBComparerInterbase.dpr` → InterBase/Firebird




3. **Projekt kompilieren:**
* Modus: **Release**
* Plattform: **Win32** oder **Win64**


4. **Generierte ausführbare Datei:**
```
DBComparer\Win32\Release\DBComparer.exe

```



---

## 🚀 Schnellstart

### Allgemeine Syntax

```bash
# Standardmäßig wird das Skript auf stdout geschrieben; verwenden Sie --output oder >
Executable.exe <Quelle> <Ziel> [Optionen] --output=skript_datei.sql
Executable.exe <Quelle> <Ziel> [Optionen] > skript_datei.sql

```

### Verbindungsformat

```
server:port\datenbank benutzer\passwort 

```

### Grundlegendes Beispiel

```bash
# Nur Struktur synchronisieren
DBComparer.exe localhost:3306\produktion root\pass localhost:3306\entwicklung root\pass > script_only_structure.sql

# Struktur + Daten synchronisieren
DBComparer.exe localhost:3306\produktion root\pass localhost:3306\entwicklung root\pass --with-data-diff > script_data_structure.sql

```

---

## 💡 Beispiele nach Engine

### 🐬 MySQL / MariaDB

```bash
# Vollständige Synchronisation mit Daten
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > script_incremental.sql

# Nur bestimmte Tabellen
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=benutzer,produkte > script_withproducts.sql

# Log-Tabellen ausschließen
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logs,audit > script_withnolog.sql

# MariaDB-10.2-Kompatibilität mit --nodelete und ausgewählten Views
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom

```

### 🐘 PostgreSQL

```bash
# Format: server:port\datenbank\schema
DBComparerPostGre.exe localhost:5432\verkauf\public postgres\pass localhost:5432\verkauf\test postgres\pass > script.sql

# Benutzerdefiniertes Schema
DBComparerPostGre.exe prod-server:5432\erp\buchhaltung admin\pass dev-server:5432\erp\buchhaltung admin\pass > script_schem.sql

```

### 🔴 Oracle Database

```bash
# Direkte Verbindung: host:port/SID user/pass@Owner
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > script.sql

# Verwendung von TNS Names
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > script_tns.sql

# Mit spezifischem Owner
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > script_owner.sql

```

### 🟦 Microsoft SQL Server

```bash
# Schützt zusätzliche Tabellen, Spalten, Indizes und Datensätze vor dem Löschen
DBComparerSQLServer.exe sqlserver:1433\Produktion sa\pass sqlserver:1433\Entwicklung sa\pass --nodelete > script_safe.sql

# Mit Triggern und Daten
DBComparerSQLServer.exe sqlserver:1433\Produktion sa\pass sqlserver:1433\Entwicklung sa\pass --with-triggers --with-data-diff > script_withdata.sql

```

### 🔥 InterBase / Firebird

```bash
# Remote-Server
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\scripts\update_script.sql

# Lokale Datei (Embedded)
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\script.sql

```

---

## ⚙️ Befehlszeilenoptionen

| Option | Beschreibung |
| --- | --- |
| `--nodelete` | 🛡️ **Sicherer Modus**: Löscht keine Tabellen, Spalten, Indizes oder Datensätze im Ziel |
| `--with-triggers` | 🔫 Beinhaltet Vergleich und Neuerstellung von Triggern |
| `--with-data` | 📥 Massenkopie von Daten (nur `INSERT`) |
| `--with-data-diff` | 🔄 Smart Sync per PK (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **Whitelist**: Verarbeitet nur angegebene Tabellen |
| `--exclude-tables=t1,t2` | ❌ **Blacklist**: Schließt angegebene Tabellen aus |
| `--mariadb10` | Aktiviert explizit die SQL-Erzeugung für MariaDB 10.2; keine automatische Erkennung. Nur für `DBComparer.exe` |
| `--preserve-views=v1,v2` | Überspringt die angegebenen Views: Sie werden weder gelöscht noch neu erstellt; fehlende Views werden ebenfalls nicht erstellt. Groß-/Kleinschreibung wird ignoriert |
| `--output=datei.sql` | Speichert das erzeugte Skript direkt in einer Datei |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | Mit `--output` verwendete Ausgabekodierung; Standard ist `utf8bom` |

`--preserve-views`, `--output` und `--encoding` sind in allen DBComparer-Programmen verfügbar.

Technische Auswirkungen und Einschränkungen finden Sie unter [MariaDB 10.2 compatibility](#mariadb-102-compatibility) (Englisch).

### Nützliche Kombinationen

```bash
# Schützt zusätzliche Tabellen, Spalten, Indizes und Datensätze vor dem Löschen
--nodelete --with-data-diff

# Vollständige Sync mit Triggern
--with-triggers --with-data-diff

# Nur Daten von Stammtabellen migrieren
--include-tables=kunden,produkte,kategorien --with-data

# MariaDB 10.2 mit --nodelete und Beibehaltung extern verwalteter Views
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

```

---

## 🎯 Häufige Anwendungsfälle

### 1. **Deployment Dev ➡️ Produktion**

```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > script.sql

```

### 2. **Struktur klonen ohne Daten**

```bash
DBComparer.exe source:3306\db user\pass target:3306\db user\pass > script.sql

```

### 3. **Stammtabellen replizieren**

```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=laender,provinzen,kategorien --with-data > script.sql

```

### 4. **Kontinuierliche Synchronisation (CI/CD)**

```bash
# In einem Jenkins/GitLab CI-Skript
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > script.sql

```

### 5. **Migration zwischen verschiedenen Engines**

```bash
# MySQL ➡️ PostgreSQL (erfordert manuellen Export/Import)
# DBComparer generiert kompatible DDL-Skripte

```

---

## 📄 Lizenz

Dieses Projekt ist unter der **MIT-Lizenz** lizenziert.

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction...

```

Siehe die Datei [LICENSE](https://www.google.com/search?q=LICENSE) für weitere Details.

---

## ⚠️ Haftungsausschluss

> **Diese Software wird "WIE BESEHEN" bereitgestellt, ohne jegliche Garantie**, weder ausdrücklich noch stillschweigend, einschließlich, aber nicht beschränkt auf die Garantien der Marktgängigkeit, Eignung für einen bestimmten Zweck und Nichtverletzung.
> **⚠️ KRITISCHE EMPFEHLUNG:**
> Führen Sie **vollständige Backups** Ihrer Zieldatenbank durch, bevor Sie Synchronisationsskripte in **Produktionsumgebungen** ausführen.
</details>

<details>
<summary><strong>🇨🇳 中文</strong></summary>
# 🔄 DBComparer

<div align="center">

**专业数据库同步工具**

*只需一条命令，即可比较、同步和迁移同一引擎的两个数据库之间的架构和数据。*

[特性](https://www.google.com/search?q=%23-%E7%89%B9%E6%80%A7) • [安装](https://www.google.com/search?q=%23-%E7%BC%96%E8%AF%91) • [用法](https://www.google.com/search?q=%23-%E5%BF%AB%E9%80%9F%E4%BD%BF%E7%94%A8) • [示例](https://www.google.com/search?q=%23-%E6%8C%89%E5%BC%95%E6%93%8E%E7%A4%BA%E4%BE%8B) • [许可证](https://www.google.com/search?q=%23-%E8%AE%B8%E5%8F%AF%E8%AF%81)

</div>

---

## 📋 目录

* [描述](https://www.google.com/search?q=%23-%E6%8F%8F%E8%BF%B0)
* [特性](https://www.google.com/search?q=%23-%E7%89%B9%E6%80%A7)
* [支持的数据库](https://www.google.com/search?q=%23-%E6%94%AF%E6%8C%81%E7%9A%84%E6%95%B0%E6%8D%AE%E5%BA%93)
* [要求](https://www.google.com/search?q=%23-%E9%87%8D%E8%A6%81%E8%A6%81%E6%B1%82)
* [编译](https://www.google.com/search?q=%23-%E7%BC%96%E8%AF%91)
* [快速使用](https://www.google.com/search?q=%23-%E5%BF%AB%E9%80%9F%E4%BD%BF%E7%94%A8)
* [按引擎示例](https://www.google.com/search?q=%23-%E6%8C%89%E5%BC%95%E6%93%8E%E7%A4%BA%E4%BE%8B)
* [高级选项](https://www.google.com/search?q=%23-%E5%91%BD%E4%BB%A4%E8%A1%8C%E9%80%89%E9%A1%B9)
* [用例](https://www.google.com/search?q=%23-%E5%B8%B8%E8%A7%81%E7%94%A8%E4%BE%8B)
* [许可证](https://www.google.com/search?q=%23-%E8%AE%B8%E5%8F%AF%E8%AF%81)
* [免责声明](https://www.google.com/search?q=%23-%E5%85%8D%E8%B4%A3%E5%A3%B0%E6%98%8E)

---

## 🎯 描述

**DBComparer** 是一个用 **Delphi** 开发的命令行工具套件，允许 DBA 和开发人员：

* ✅ 比较异构数据库之间的架构
* ✅ 生成自动同步 DDL 脚本
* ✅ 智能同步数据 (INSERT/UPDATE/DELETE)
* ✅ 保持环境 (Dev ➡️ QA ➡️ Prod) 更新
* ✅ 安全可控地自动化部署

---

## ✨ 特性

### 🏗️ **结构同步 (Schema Diff)**

| 元素 | 功能 |
| --- | --- |
| **表** | 创建、新列、类型修改和可空性 |
| **索引** | 主键 (Primary Keys)、唯一键、二级索引 |
| **视图** | 比较和自动重建 |
| **存储过程** | 存储过程和函数 |
| **触发器** | 可选同步，使用 `--with-triggers` |
| **序列** | 生成器和序列 ("create if not exists" 策略) |

### 📊 **数据同步 (Data Diff)**

| 模式 | 描述 | 选项 |
| --- | --- | --- |
| **简单复制** | 批量数据转储 (`INSERT`) | `--with-data` |
| **智能同步** | 基于主键的比较：`INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **安全与控制**

* 🛡️ **安全模式** (`--nodelete`)：防止意外删除
* 🎯 **白名单** (`--include-tables`)：仅同步特定表
* 🚫 **黑名单** (`--exclude-tables`)：从进程中排除表
* 📝 **SQL 脚本**：生成 `.sql` 文件以供执行前审查

---

## 🗄️ 支持的数据库

<div align="center">

| 引擎 | 版本 | 可执行文件 | 特殊功能 |
| --- | --- | --- | --- |
| MySQL / MariaDB | 5.7+ / MariaDB 10+ | `DBComparer.exe` | 使用 `--mariadb10` 的 MariaDB 10.2 手动模式 |
| PostgreSQL | 9.6+ | `DBComparerPostGre.exe` | 架构支持 |
| Oracle | 11g+ | `DBComparerOracle.exe` | TNS Names, Owners |
| SQL Server | 2012+ | `DBComparerSQLServer.exe` | 标识列 (Identity) |
| Firebird / InterBase | 2.5+ / InterBase | `DBComparerInterbase.exe` | 生成器, 方言 |

</div>

---

## ⚠️ 重要要求

### 📦 依赖项

本项目使用 **[Devart UniDAC](https://www.devart.com/unidac/)** 进行通用连接。

> **⚖️ 许可证说明：**
> * **DBComparer** 的源代码根据 **MIT** 许可证分发（免费使用）。
> * 要 **编译** 项目，您需要 **有效的 Devart UniDAC 商业许可证**。
> * UniDAC 源文件 (`.dcu`, `.pas`) **不**包含在此存储库中。
> 
> 

### 🖥️ 系统要求

* **Delphi**: 10.4 Sydney 或更高版本 (11 Alexandria, 12 Athens)
* **Devart UniDAC**: 已安装在 Delphi IDE 中
* **Windows**: 7/8/10/11 (32位或64位)

---

## 🛠️ 编译

1. **克隆存储库：**
```bash
git clone https://github.com/alexdelphiAthn/DBComparer.git
cd DBComparer

```


2. **在 Delphi 中打开项目：**
* 选择所需引擎的 `.dpr` 文件：
* `DBComparer.dpr` → MySQL/MariaDB
* `DBComparerPostGre.dpr` → PostgreSQL
* `DBComparerOracle.dpr` → Oracle
* `DBComparerSQLServer.dpr` → SQL Server
* `DBComparerInterbase.dpr` → InterBase/Firebird




3. **编译项目：**
* 模式：**Release**
* 平台：**Win32** 或 **Win64**


4. **生成的可执行文件：**
```
DBComparer\Win32\Release\DBComparer.exe

```



---

## 🚀 快速使用

### 通用语法

```bash
# 默认写入标准输出；可使用 --output 或通过 > 重定向
Executable.exe <源> <目标> [选项] --output=脚本文件.sql
Executable.exe <源> <目标> [选项] > 脚本文件.sql

```

### 连接格式

```
服务器:端口\数据库 用户名\密码 

```

### 基本示例

```bash
# 仅同步结构
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass > script_only_structure.sql

# 同步结构 + 数据
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass --with-data-diff > script_data_structure.sql

```

---

## 💡 按引擎示例

### 🐬 MySQL / MariaDB

```bash
# 带数据的完全同步
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > script_incremental.sql

# 仅特定表
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=users,products > script_withproducts.sql

# 排除日志表
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logs,audit > script_withnolog.sql

# MariaDB 10.2 兼容模式：使用 --nodelete 并保留指定视图
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom

```

### 🐘 PostgreSQL

```bash
# 格式：服务器:端口\数据库\架构
DBComparerPostGre.exe localhost:5432\sales\public postgres\pass localhost:5432\sales\test postgres\pass > script.sql

# 自定义架构
DBComparerPostGre.exe prod-server:5432\erp\accounting admin\pass dev-server:5432\erp\accounting admin\pass > script_schem.sql

```

### 🔴 Oracle Database

```bash
# 直接连接：主机:端口/SID 用户/密码@Owner
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > script.sql

# 使用 TNS Names
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > script_tns.sql

# 指定 Owner
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > script_owner.sql

```

### 🟦 Microsoft SQL Server

```bash
# 防止删除目标中多余的表、列、索引和记录
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --nodelete > script_safe.sql

# 带触发器和数据
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --with-triggers --with-data-diff > script_withdata.sql

```

### 🔥 InterBase / Firebird

```bash
# 远程服务器
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\scripts\update_script.sql

# 本地文件（嵌入式）
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\script.sql

```

---

## ⚙️ 命令行选项

| 选项 | 描述 |
| --- | --- |
| `--nodelete` | 🛡️ **安全模式**：不删除目标中的表、列、索引或记录 |
| `--with-triggers` | 🔫 包括触发器的比较和重建 |
| `--with-data` | 📥 批量数据复制（仅 `INSERT`） |
| `--with-data-diff` | 🔄 按主键智能同步 (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **白名单**：仅处理指定的表 |
| `--exclude-tables=t1,t2` | ❌ **黑名单**：排除指定的表 |
| `--mariadb10` | 启用兼容 MariaDB 10.2 的 SQL 生成模式（需显式启用，不会自动检测服务器版本；仅适用于 `DBComparer.exe`） |
| `--preserve-views=v1,v2` | 不删除或重新创建指定视图（名称不区分大小写）；若目标中不存在，也不会创建 |
| `--output=output.sql` | 将生成的脚本保存到指定 SQL 文件 |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | 设置 `--output` 文件的编码；默认为 `utf8bom`，仅与 `--output` 一起生效 |

`--preserve-views`、`--output` 和 `--encoding` 可用于所有 DBComparer 可执行文件。

有关技术影响和限制，请参阅英文版的 [MariaDB 10.2 compatibility](#mariadb-102-compatibility)。

### 有用的组合

```bash
# 防止删除目标中多余的表、列、索引和记录
--nodelete --with-data-diff

# 带触发器的完全同步
--with-triggers --with-data-diff

# 仅迁移主表数据
--include-tables=customers,products,categories --with-data

# MariaDB 10.2 使用 --nodelete，并保留外部管理的视图
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

```

---

## 🎯 常见用例

### 1. **部署 Dev ➡️ Production**

```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > script.sql

```

### 2. **克隆结构但不含数据**

```bash
DBComparer.exe source:3306\db user\pass target:3306\db user\pass > script.sql

```

### 3. **复制主表**

```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=countries,provinces,categories --with-data > script.sql

```

### 4. **持续同步 (CI/CD)**

```bash
# 在 Jenkins/GitLab CI 脚本中
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > script.sql

```

### 5. **不同引擎之间的迁移**

```bash
# MySQL ➡️ PostgreSQL (需要手动导出/导入)
# DBComparer 生成兼容的 DDL 脚本

```

---

## 📄 许可证

本项目采用 **MIT 许可证**。

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction...

```

有关详细信息，请参阅 [LICENSE](https://www.google.com/search?q=LICENSE) 文件。

---

## ⚠️ 免责声明

> **本软件按“原样”提供，不提供任何形式的明示或暗示保证**，包括但不限于适销性、特定用途适用性和不侵权的保证。
> **⚠️ 关键建议：**
> 在 **生产环境** 中运行任何同步脚本之前，请对目标数据库执行 **完整备份**。

</details>

<details>
<summary><strong>🇰🇷 한국어</strong></summary>
# 🔄 DBComparer

<div align="center">

**전문 데이터베이스 동기화 도구**

*단일 명령으로 동일한 엔진의 두 데이터베이스 간 스키마 및 데이터를 비교, 동기화 및 마이그레이션합니다.*

[기능](https://www.google.com/search?q=%23-%EA%B8%B0%EB%8A%A5) • [설치](https://www.google.com/search?q=%23-%EC%BB%B4%ED%8C%8C%EC%9D%BC) • [사용법](https://www.google.com/search?q=%23-%EB%B9%A0%EB%A5%B8-%EC%82%AC%EC%9A%A9) • [예제](https://www.google.com/search?q=%23-%EC%97%94%EC%A7%84%EB%B3%84-%EC%98%88%EC%A0%9C) • [라이선스](https://www.google.com/search?q=%23-%EB%9D%BC%EC%9D%B4%EC%84%A0%EC%8A%A4)

</div>

---

## 📋 목차

* [설명](https://www.google.com/search?q=%23-%EC%84%A4%EB%AA%85)
* [기능](https://www.google.com/search?q=%23-%EA%B8%B0%EB%8A%A5)
* [지원되는 데이터베이스](https://www.google.com/search?q=%23-%EC%A7%80%EC%9B%90%EB%90%98%EB%8A%94-%EB%8D%B0%EC%9D%B4%ED%84%B0%EB%B2%A0%EC%9D%B4%EC%8A%A4)
* [요구 사항](https://www.google.com/search?q=%23-%EC%A4%91%EC%9A%94-%EC%9A%94%EA%B5%AC-%EC%82%AC%ED%95%AD)
* [컴파일](https://www.google.com/search?q=%23-%EC%BB%B4%ED%8C%8C%EC%9D%BC)
* [빠른 사용](https://www.google.com/search?q=%23-%EB%B9%A0%EB%A5%B8-%EC%82%AC%EC%9A%A9)
* [엔진별 예제](https://www.google.com/search?q=%23-%EC%97%94%EC%A7%84%EB%B3%84-%EC%98%88%EC%A0%9C)
* [고급 옵션](https://www.google.com/search?q=%23-%EB%AA%85%EB%A0%B9%EC%A4%84-%EC%98%B5%EC%85%98)
* [사용 사례](https://www.google.com/search?q=%23-%EC%9D%BC%EB%B0%98%EC%A0%81%EC%9D%B8-%EC%82%AC%EC%9A%A9-%EC%82%AC%EB%A1%80)
* [라이선스](https://www.google.com/search?q=%23-%EB%9D%BC%EC%9D%B4%EC%84%A0%EC%8A%A4)
* [면책 조항](https://www.google.com/search?q=%23-%EB%A9%B4%EC%B1%85-%EC%A1%B0%ED%95%AD)

---

## 🎯 설명

**DBComparer**는 **Delphi**로 개발된 명령줄 도구 모음으로, DBA 및 개발자가 다음을 수행할 수 있도록 합니다:

* ✅ 이기종 데이터베이스 간 스키마 비교
* ✅ 자동 동기화 DDL 스크립트 생성
* ✅ 데이터 지능형 동기화 (INSERT/UPDATE/DELETE)
* ✅ 환경 (Dev ➡️ QA ➡️ Prod) 최신 상태 유지
* ✅ 안전하고 제어 가능한 배포 자동화

---

## ✨ 기능

### 🏗️ **구조 동기화 (Schema Diff)**

| 요소 | 기능 |
| --- | --- |
| **테이블** | 생성, 새 열, 유형 수정 및 Null 허용 여부 |
| **인덱스** | 기본 키 (PK), 고유 키 (Unique), 보조 인덱스 |
| **뷰** | 비교 및 자동 재생성 |
| **프로시저** | 저장 프로시저 및 함수 |
| **트리거** | `--with-triggers` 옵션으로 동기화 선택 가능 |
| **시퀀스** | 생성기 및 시퀀스 ("없으면 생성" 전략) |

### 📊 **데이터 동기화 (Data Diff)**

| 모드 | 설명 | 옵션 |
| --- | --- | --- |
| **단순 복사** | 대량 데이터 덤프 (`INSERT`) | `--with-data` |
| **스마트 동기화** | PK 기반 비교: `INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **보안 및 제어**

* 🛡️ **안전 모드** (`--nodelete`): 우발적 삭제 방지
* 🎯 **화이트리스트** (`--include-tables`): 특정 테이블만 동기화
* 🚫 **블랙리스트** (`--exclude-tables`): 프로세스에서 테이블 제외
* 📝 **SQL 스크립트**: 실행 전 검토를 위한 `.sql` 파일 생성

---

## 🗄️ 지원되는 데이터베이스

<div align="center">

| 엔진 | 버전 | 실행 파일 | 특수 기능 |
| --- | --- | --- | --- |
| MySQL / MariaDB | 5.7+ / MariaDB 10+ | `DBComparer.exe` | `--mariadb10`을 사용하는 MariaDB 10.2 수동 모드 |
| PostgreSQL | 9.6+ | `DBComparerPostGre.exe` | 스키마 지원 |
| Oracle | 11g+ | `DBComparerOracle.exe` | TNS Names, Owners |
| SQL Server | 2012+ | `DBComparerSQLServer.exe` | 식별 열 (Identity) |
| Firebird / InterBase | 2.5+ / InterBase | `DBComparerInterbase.exe` | 생성기, 방언 |

</div>

---

## ⚠️ 중요 요구 사항

### 📦 의존성

이 프로젝트는 범용 연결을 위해 **[Devart UniDAC](https://www.devart.com/unidac/)**를 사용합니다.

> **⚖️ 라이선스 참고:**
> * **DBComparer**의 소스 코드는 **MIT** 라이선스(무료 사용)로 배포됩니다.
> * 프로젝트를 **컴파일**하려면 **유효한 Devart UniDAC 상업용 라이선스**가 필요합니다.
> * UniDAC 소스 파일 (`.dcu`, `.pas`)은 이 저장소에 포함되어 **있지 않습니다**.
> 
> 

### 🖥️ 시스템 요구 사항

* **Delphi**: 10.4 Sydney 이상 (11 Alexandria, 12 Athens)
* **Devart UniDAC**: Delphi IDE에 설치됨
* **Windows**: 7/8/10/11 (32비트 또는 64비트)

---

## 🛠️ 컴파일

1. **저장소 복제:**
```bash
git clone https://github.com/alexdelphiAthn/DBComparer.git
cd DBComparer

```


2. **Delphi에서 프로젝트 열기:**
* 원하는 엔진에 대한 `.dpr` 파일을 선택합니다:
* `DBComparer.dpr` → MySQL/MariaDB
* `DBComparerPostGre.dpr` → PostgreSQL
* `DBComparerOracle.dpr` → Oracle
* `DBComparerSQLServer.dpr` → SQL Server
* `DBComparerInterbase.dpr` → InterBase/Firebird




3. **프로젝트 컴파일:**
* 모드: **Release**
* 플랫폼: **Win32** 또는 **Win64**


4. **생성된 실행 파일:**
```
DBComparer\Win32\Release\DBComparer.exe

```



---

## 🚀 빠른 사용

### 일반 구문

```bash
# 기본적으로 표준 출력에 기록됩니다. --output을 사용하거나 >로 리디렉션하세요
Executable.exe <원본> <대상> [옵션] --output=스크립트_파일.sql
Executable.exe <원본> <대상> [옵션] > 스크립트_파일.sql

```

### 연결 형식

```
서버:포트\데이터베이스 사용자\암호 

```

### 기본 예제

```bash
# 구조만 동기화
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass > script_only_structure.sql

# 구조 + 데이터 동기화
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass --with-data-diff > script_data_structure.sql

```

---

## 💡 엔진별 예제

### 🐬 MySQL / MariaDB

```bash
# 데이터를 포함한 전체 동기화
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > script_incremental.sql

# 특정 테이블만
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=users,products > script_withproducts.sql

# 로그 테이블 제외
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logs,audit > script_withnolog.sql

# MariaDB 10.2 호환 모드: --nodelete를 사용하고 지정한 뷰 보존
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom

```

### 🐘 PostgreSQL

```bash
# 형식: 서버:포트\데이터베이스\스키마
DBComparerPostGre.exe localhost:5432\sales\public postgres\pass localhost:5432\sales\test postgres\pass > script.sql

# 사용자 정의 스키마
DBComparerPostGre.exe prod-server:5432\erp\accounting admin\pass dev-server:5432\erp\accounting admin\pass > script_schem.sql

```

### 🔴 Oracle Database

```bash
# 직접 연결: 호스트:포트/SID 사용자/암호@Owner
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > script.sql

# TNS Names 사용
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > script_tns.sql

# 특정 Owner 사용
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > script_owner.sql

```

### 🟦 Microsoft SQL Server

```bash
# 대상에만 있는 테이블, 열, 인덱스 및 레코드 삭제 방지
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --nodelete > script_safe.sql

# 트리거 및 데이터 포함
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --with-triggers --with-data-diff > script_withdata.sql

```

### 🔥 InterBase / Firebird

```bash
# 원격 서버
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\scripts\update_script.sql

# 로컬 파일 (임베디드)
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\script.sql

```

---

## ⚙️ 명령줄 옵션

| 옵션 | 설명 |
| --- | --- |
| `--nodelete` | 🛡️ **안전 모드**: 대상에서 테이블, 열, 인덱스 또는 레코드를 삭제하지 않습니다 |
| `--with-triggers` | 🔫 트리거 비교 및 재생성 포함 |
| `--with-data` | 📥 대량 데이터 복사 (`INSERT` 전용) |
| `--with-data-diff` | 🔄 PK별 스마트 동기화 (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **화이트리스트**: 지정된 테이블만 처리 |
| `--exclude-tables=t1,t2` | ❌ **블랙리스트**: 지정된 테이블 제외 |
| `--mariadb10` | MariaDB 10.2 호환 SQL 생성 모드를 명시적으로 활성화합니다(서버 버전 자동 감지 없음, `DBComparer.exe` 전용) |
| `--preserve-views=v1,v2` | 지정한 뷰를 삭제하거나 다시 생성하지 않습니다(이름 대/소문자 구분 없음). 대상에 없어도 생성하지 않습니다 |
| `--output=output.sql` | 생성된 스크립트를 지정한 SQL 파일에 저장합니다 |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | `--output` 파일의 인코딩을 설정합니다. 기본값은 `utf8bom`이며 `--output`과 함께 사용할 때만 적용됩니다 |

`--preserve-views`, `--output`, `--encoding`은 모든 DBComparer 실행 파일에서 사용할 수 있습니다.

기술적 영향과 제한 사항은 영어 [MariaDB 10.2 compatibility](#mariadb-102-compatibility)를 참조하세요.

### 유용한 조합

```bash
# 대상에만 있는 테이블, 열, 인덱스 및 레코드 삭제 방지
--nodelete --with-data-diff

# 트리거 포함 전체 동기화
--with-triggers --with-data-diff

# 마스터 테이블 데이터만 마이그레이션
--include-tables=customers,products,categories --with-data

# MariaDB 10.2에서 --nodelete를 사용하고 외부 관리 뷰 보존
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

```

---

## 🎯 일반적인 사용 사례

### 1. **배포 Dev ➡️ Production**

```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > script.sql

```

### 2. **데이터 없이 구조 복제**

```bash
DBComparer.exe source:3306\db user\pass target:3306\db user\pass > script.sql

```

### 3. **마스터 테이블 복제**

```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=countries,provinces,categories --with-data > script.sql

```

### 4. **지속적 동기화 (CI/CD)**

```bash
# Jenkins/GitLab CI 스크립트에서
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > script.sql

```

### 5. **다른 엔진 간 마이그레이션**

```bash
# MySQL ➡️ PostgreSQL (수동 내보내기/가져오기 필요)
# DBComparer는 호환 가능한 DDL 스크립트를 생성합니다

```

---

## 📄 라이선스

이 프로젝트는 **MIT 라이선스** 하에 있습니다.

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction...

```

자세한 내용은 [LICENSE](https://www.google.com/search?q=LICENSE) 파일을 참조하십시오.

---

## ⚠️ 면책 조항

> **이 소프트웨어는 상품성, 특정 목적에의 적합성 및 비침해에 대한 보증을 포함하여(단, 이에 국한되지 않음) 명시적이거나 묵시적인 어떠한 종류의 보증 없이 "있는 그대로" 제공됩니다.**
> **⚠️ 중요 권장 사항:**
> **운영 환경**에서 동기화 스크립트를 실행하기 전에 대상 데이터베이스의 **전체 백업**을 수행하십시오.
</details>

<details>
<summary><strong>🇸🇦 العربية</strong></summary>
# 🔄 DBComparer

<div align="center">

**أداة احترافية لمزامنة قواعد البيانات**

*قارن، زامن، وهاجر المخططات والبيانات بين قاعدتي بيانات من نفس المحرك بأمر واحد.*

[المميزات](https://www.google.com/search?q=%23-%D8%A7%D9%84%D9%85%D9%85%D9%8A%D8%B2%D8%A7%D8%AA) • [التثبيت](https://www.google.com/search?q=%23-%D8%A7%D9%84%D8%AA%D8%AD%D9%88%D9%8A%D9%84-%D8%A7%D9%84%D8%A8%D8%B1%D9%85%D8%AC%D9%8A-compilation) • [الاستخدام](https://www.google.com/search?q=%23-%D8%A7%D9%84%D8%A7%D8%B3%D8%AA%D8%AE%D8%AF%D8%A7%D9%85-%D8%A7%D9%84%D8%B3%D8%B1%D9%8A%D8%B9) • [أمثلة](https://www.google.com/search?q=%23-%D8%A3%D9%85%D8%AB%D9%84%D8%A9-%D8%AD%D8%B3%D8%A8-%D8%A7%D9%84%D9%85%D8%AD%D8%B1%D9%83) • [الرخصة](https://www.google.com/search?q=%23-%D8%A7%D9%84%D8%B1%D8%AE%D8%B5%D8%A9)

</div>

---

## 📋 جدول المحتويات

* [الوصف](https://www.google.com/search?q=%23-%D8%A7%D9%84%D9%88%D8%B5%D9%81)
* [المميزات](https://www.google.com/search?q=%23-%D8%A7%D9%84%D9%85%D9%85%D9%8A%D8%B2%D8%A7%D8%AA)
* [قواعد البيانات المدعومة](https://www.google.com/search?q=%23-%D9%82%D9%88%D8%A7%D8%B9%D8%AF-%D8%A7%D9%84%D8%A8%D9%8A%D8%A7%D9%86%D8%A7%D8%AA-%D8%A7%D9%84%D9%85%D8%AF%D8%B9%D9%88%D9%85%D8%A9)
* [المتطلبات](https://www.google.com/search?q=%23-%D9%85%D8%AA%D8%B7%D9%84%D8%A8%D8%A7%D8%AA-%D9%85%D9%87%D9%85%D8%A9)
* [التحويل البرمجي (Compilation)](https://www.google.com/search?q=%23-%D8%A7%D9%84%D8%AA%D8%AD%D9%88%D9%8A%D9%84-%D8%A7%D9%84%D8%A8%D8%B1%D9%85%D8%AC%D9%8A-compilation)
* [الاستخدام السريع](https://www.google.com/search?q=%23-%D8%A7%D9%84%D8%A7%D8%B3%D8%AA%D8%AE%D8%AF%D8%A7%D9%85-%D8%A7%D9%84%D8%B3%D8%B1%D9%8A%D8%B9)
* [أمثلة حسب المحرك](https://www.google.com/search?q=%23-%D8%A3%D9%85%D8%AB%D9%84%D8%A9-%D8%AD%D8%B3%D8%A8-%D8%A7%D9%84%D9%85%D8%AD%D8%B1%D9%83)
* [خيارات متقدمة](https://www.google.com/search?q=%23-%D8%AE%D9%8A%D8%A7%D8%B1%D8%A7%D8%AA-%D8%B3%D8%B7%D8%B1-%D8%A7%D9%84%D8%A3%D9%88%D8%A7%D9%85%D8%B1)
* [حالات الاستخدام](https://www.google.com/search?q=%23-%D8%AD%D8%A7%D9%84%D8%A7%D8%AA-%D8%A7%D9%84%D8%A7%D8%B3%D8%AA%D8%AE%D8%AF%D8%A7%D9%85-%D8%A7%D9%84%D8%B4%D8%A7%D8%A6%D8%B9%D8%A9)
* [الرخصة](https://www.google.com/search?q=%23-%D8%A7%D9%84%D8%B1%D8%AE%D8%B5%D8%A9)
* [إخلاء المسؤولية](https://www.google.com/search?q=%23-%D8%A5%D8%AE%D9%84%D8%A7%D8%A1-%D8%A7%D9%84%D9%85%D8%B3%D8%A4%D9%88%D9%84%D9%8A%D8%A9)

---

## 🎯 الوصف

**DBComparer** عبارة عن مجموعة أدوات سطر أوامر تم تطويرها في **Delphi** والتي تتيح لمسؤولي قواعد البيانات (DBAs) والمطورين:

* ✅ مقارنة المخططات بين قواعد بيانات غير متجانسة
* ✅ إنشاء نصوص DDL للمزامنة التلقائية
* ✅ مزامنة البيانات بذكاء (INSERT/UPDATE/DELETE)
* ✅ الحفاظ على البيئات (Dev ➡️ QA ➡️ Prod) محدثة
* ✅ أتمتة عمليات النشر (Deployments) بأمان وتحكم

---

## ✨ المميزات

### 🏗️ **مزامنة الهيكل (Schema Diff)**

| العنصر | الوظيفة |
| --- | --- |
| **الجداول** | الإنشاء، أعمدة جديدة، تعديل الأنواع وقابلية القيم الفارغة |
| **الفهارس** | المفاتيح الأساسية (PK)، الفريدة (Unique)، الفهارس الثانوية |
| **العروض (Views)** | المقارنة وإعادة الإنشاء التلقائي |
| **الإجراءات** | الإجراءات المخزنة والدوال |
| **المشغلات (Triggers)** | مزامنة اختيارية باستخدام `--with-triggers` |
| **التسلسلات** | المولدات والتسلسلات (استراتيجية "إنشاء إذا لم يكن موجوداً") |

### 📊 **مزامنة البيانات (Data Diff)**

| الوضع | الوصف | الخيار |
| --- | --- | --- |
| **نسخ بسيط** | تفريغ جماعي للبيانات (`INSERT`) | `--with-data` |
| **مزامنة ذكية** | مقارنة تعتمد على المفتاح الأساسي: `INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **الأمان والتحكم**

* 🛡️ **الوضع الآمن** (`--nodelete`): يمنع الحذف العرضي
* 🎯 **القائمة البيضاء** (`--include-tables`): مزامنة جداول محددة فقط
* 🚫 **القائمة السوداء** (`--exclude-tables`): استبعاد جداول من العملية
* 📝 **نصوص SQL**: ينشئ ملفات `.sql` للمراجعة قبل التنفيذ

---

## 🗄️ قواعد البيانات المدعومة

<div align="center">

| المحرك | الإصدار | الملف التنفيذي | ميزات خاصة |
| --- | --- | --- | --- |
| MySQL / MariaDB | 5.7+ / MariaDB 10+ | `DBComparer.exe` | وضع MariaDB 10.2 اليدوي باستخدام `--mariadb10` |
| PostgreSQL | 9.6+ | `DBComparerPostGre.exe` | دعم المخططات |
| Oracle | 11g+ | `DBComparerOracle.exe` | TNS Names, Owners |
| SQL Server | 2012+ | `DBComparerSQLServer.exe` | أعمدة الهوية (Identity) |
| Firebird / InterBase | 2.5+ / InterBase | `DBComparerInterbase.exe` | المولدات، اللهجات |

</div>

---

## ⚠️ متطلبات مهمة

### 📦 التبعيات

يستخدم هذا المشروع **[Devart UniDAC](https://www.devart.com/unidac/)** للاتصال الشامل.

> **⚖️ ملاحظة حول الترخيص:**
> * يتم توزيع الكود المصدري لـ **DBComparer** بموجب ترخيص **MIT** (استخدام مجاني).
> * لكي تقوم بـ **تحويل (Compile)** المشروع، تحتاج إلى **ترخيص تجاري صالح لـ Devart UniDAC**.
> * ملفات مصدر UniDAC (`.dcu`, `.pas`) **غير** مدرجة في هذا المستودع.
> 
> 

### 🖥️ متطلبات النظام

* **Delphi**: 10.4 Sydney أو أعلى (11 Alexandria, 12 Athens)
* **Devart UniDAC**: مثبت في بيئة Delphi IDE
* **Windows**: 7/8/10/11 (32 بت أو 64 بت)

---

## 🛠️ التحويل البرمجي (Compilation)

1. **استنساخ المستودع:**
```bash
git clone https://github.com/alexdelphiAthn/DBComparer.git
cd DBComparer

```


2. **فتح المشروع في Delphi:**
* اختر ملف `.dpr` للمحرك المطلوب:
* `DBComparer.dpr` ← MySQL/MariaDB
* `DBComparerPostGre.dpr` ← PostgreSQL
* `DBComparerOracle.dpr` ← Oracle
* `DBComparerSQLServer.dpr` ← SQL Server
* `DBComparerInterbase.dpr` ← InterBase/Firebird




3. **تحويل المشروع:**
* الوضع: **Release**
* المنصة: **Win32** أو **Win64**


4. **الملف التنفيذي الناتج:**
```
DBComparer\Win32\Release\DBComparer.exe

```



---

## 🚀 الاستخدام السريع

### الصيغة العامة

```bash
# يُكتب افتراضيًا إلى الإخراج القياسي؛ استخدم --output أو أعد التوجيه بواسطة >
Executable.exe <المصدر> <الهدف> [خيارات] --output=script.sql
Executable.exe <المصدر> <الهدف> [خيارات] > script.sql

```

### تنسيق الاتصال

```
الخادم:المنفذ\قاعدة_البيانات المستخدم\كلمة_المرور 

```

### مثال أساسي

```bash
# مزامنة الهيكل فقط
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass > script_only_structure.sql

# مزامنة الهيكل + البيانات
DBComparer.exe localhost:3306\prod root\pass localhost:3306\dev root\pass --with-data-diff > script_data_structure.sql

```

---

## 💡 أمثلة حسب المحرك

### 🐬 MySQL / MariaDB

```bash
# مزامنة كاملة مع البيانات
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > script_incremental.sql

# جداول محددة فقط
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=users,products > script_withproducts.sql

# استبعاد جداول السجلات
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logs,audit > script_withnolog.sql

# وضع التوافق مع MariaDB 10.2 باستخدام --nodelete، مع الحفاظ على طرق العرض المحددة
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom

```

### 🐘 PostgreSQL

```bash
# التنسيق: الخادم:المنفذ\قاعدة_البيانات\المخطط
DBComparerPostGre.exe localhost:5432\sales\public postgres\pass localhost:5432\sales\test postgres\pass > script.sql

# مخطط مخصص
DBComparerPostGre.exe prod-server:5432\erp\accounting admin\pass dev-server:5432\erp\accounting admin\pass > script_schem.sql

```

### 🔴 Oracle Database

```bash
# اتصال مباشر: المضيف:المنفذ/SID المستخدم/كلمة_المرور@المالك
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > script.sql

# استخدام أسماء TNS
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > script_tns.sql

# مع مالك (Owner) محدد
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > script_owner.sql

```

### 🟦 Microsoft SQL Server

```bash
# يحمي الجداول والأعمدة والفهارس والسجلات الزائدة في الهدف من الحذف
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --nodelete > script_safe.sql

# مع المشغلات والبيانات
DBComparerSQLServer.exe sqlserver:1433\Production sa\pass sqlserver:1433\Development sa\pass --with-triggers --with-data-diff > script_withdata.sql

```

### 🔥 InterBase / Firebird

```bash
# خادم بعيد
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\scripts\update_script.sql

# ملف محلي (مضمن)
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\script.sql

```

---

## ⚙️ خيارات سطر الأوامر

| الخيار | الوصف |
| --- | --- |
| `--nodelete` | 🛡️ **الوضع الآمن**: لا يحذف الجداول أو الأعمدة أو الفهارس أو السجلات في الهدف |
| `--with-triggers` | 🔫 يشمل مقارنة وإعادة إنشاء المشغلات (Triggers) |
| `--with-data` | 📥 نسخ جماعي للبيانات (`INSERT` فقط) |
| `--with-data-diff` | 🔄 مزامنة ذكية بواسطة PK (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **القائمة البيضاء**: معالجة الجداول المحددة فقط |
| `--exclude-tables=t1,t2` | ❌ **القائمة السوداء**: استبعاد الجداول المحددة |
| `--mariadb10` | وضع يُفعَّل صراحةً لتوليد SQL متوافق مع MariaDB 10.2؛ لا يوجد اكتشاف تلقائي للإصدار، ومتاح فقط في `DBComparer.exe` |
| `--preserve-views=v1,v2` | لا يحذف طرق العرض المحددة ولا يعيد إنشاءها؛ الأسماء غير حساسة لحالة الأحرف، وطرق العرض غير الموجودة لا يتم إنشاؤها |
| `--output=output.sql` | يحفظ الناتج مباشرةً في ملف |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | ترميز ملف الإخراج؛ يُستخدم مع `--output`، والقيمة الافتراضية هي `utf8bom` |

تتوفر الخيارات `--preserve-views` و`--output` و`--encoding` في جميع ملفات DBComparer التنفيذية.

للتأثيرات التقنية والقيود، راجع قسم [MariaDB 10.2 compatibility](#mariadb-102-compatibility) باللغة الإنجليزية.

### مجموعات مفيدة

```bash
# يحمي الجداول والأعمدة والفهارس والسجلات الزائدة في الهدف من الحذف
--nodelete --with-data-diff

# مزامنة كاملة مع المشغلات
--with-triggers --with-data-diff

# ترحيل بيانات الجداول الرئيسية فقط
--include-tables=customers,products,categories --with-data

# MariaDB 10.2 باستخدام --nodelete مع الحفاظ على طرق العرض المُدارة خارجياً
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

```

---

## 🎯 حالات الاستخدام الشائعة

### 1. **النشر Dev ⬅️ Production**

```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > script.sql

```

### 2. **استنساخ الهيكل بدون بيانات**

```bash
DBComparer.exe source:3306\db user\pass target:3306\db user\pass > script.sql

```

### 3. **تكرار الجداول الرئيسية**

```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=countries,provinces,categories --with-data > script.sql

```

### 4. **المزامنة المستمرة (CI/CD)**

```bash
# في نص Jenkins/GitLab CI
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > script.sql

```

### 5. **الترحيل بين محركات مختلفة**

```bash
# MySQL ⬅️ PostgreSQL (يتطلب تصدير/استيراد يدوي)
# DBComparer ينشئ نصوص DDL متوافقة

```

---

## 📄 الرخصة

هذا المشروع مرخص بموجب **ترخيص MIT**.

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction...

```

راجع ملف [LICENSE](https://www.google.com/search?q=LICENSE) لمزيد من التفاصيل.

---

## ⚠️ إخلاء المسؤولية

> **يتم توفير هذا البرنامج "كما هو"، دون أي ضمان من أي نوع**، صريحاً كان أم ضمنياً، بما في ذلك، على سبيل المثال لا الحصر، ضمانات القابلية للتسويق والملاءمة لغرض معين وعدم الانتهاك.
> **⚠️ توصية بالغة الأهمية:**
> قم بإجراء **نسخ احتياطي كامل** لقاعدة البيانات المستهدفة قبل تشغيل أي نصوص مزامنة في **بيئات الإنتاج**.

</details>

<details>
<summary><strong>🇭🇷 Hrvatski</strong></summary>
# 🔄 DBComparer

<div align="center">

**Profesionalni alat za sinkronizaciju baza podataka**

*Usporedite, sinkronizirajte i migrirajte sheme i podatke između dviju baza podataka istog pogona jednom naredbom.*

[Značajke](https://www.google.com/search?q=%23-zna%C4%8Dajke) • [Instalacija](https://www.google.com/search?q=%23-kompilacija) • [Upotreba](https://www.google.com/search?q=%23-brza-upotreba) • [Primjeri](https://www.google.com/search?q=%23-primjeri-po-pogonu) • [Licenca](https://www.google.com/search?q=%23-licenca)

</div>

---

## 📋 Sadržaj

* [Opis](https://www.google.com/search?q=%23-opis)
* [Značajke](https://www.google.com/search?q=%23-zna%C4%8Dajke)
* [Podržane baze podataka](https://www.google.com/search?q=%23-podr%C5%BEane-baze-podataka)
* [Zahtjevi](https://www.google.com/search?q=%23-va%C5%BEni-zahtjevi)
* [Kompilacija](https://www.google.com/search?q=%23-kompilacija)
* [Brza upotreba](https://www.google.com/search?q=%23-brza-upotreba)
* [Primjeri po pogonu](https://www.google.com/search?q=%23-primjeri-po-pogonu)
* [Napredne opcije](https://www.google.com/search?q=%23-opcije-naredbenog-retka)
* [Slučajevi upotrebe](https://www.google.com/search?q=%23-uobi%C4%8Dajeni-slu%C4%8Dajevi-upotrebe)
* [Licenca](https://www.google.com/search?q=%23-licenca)
* [Odricanje od odgovornosti](https://www.google.com/search?q=%23-odricanje-od-odgovornosti)

---

## 🎯 Opis

**DBComparer** je paket alata naredbenog retka razvijen u **Delphi**-ju koji omogućuje administratorima baza podataka (DBA) i programerima:

* ✅ Usporedbu shema između heterogenih baza podataka
* ✅ Generiranje DDL skripti za automatsku sinkronizaciju
* ✅ Inteligentnu sinkronizaciju podataka (INSERT/UPDATE/DELETE)
* ✅ Održavanje okruženja (Dev ➡️ QA ➡️ Prod) ažurnima
* ✅ Automatizaciju implementacije (deployment) uz sigurnost i kontrolu

---

## ✨ Značajke

### 🏗️ **Sinkronizacija strukture (Schema Diff)**

| Element | Funkcionalnost |
| --- | --- |
| **Tablice** | Kreiranje, novi stupci, izmjena tipova i nulabilnosti |
| **Indeksi** | Primarni ključevi (PK), jedinstveni (Unique), sekundarni indeksi |
| **Pogledi** | Usporedba i automatsko ponovno kreiranje |
| **Procedure** | Pohranjene procedure i funkcije |
| **Okidači (Triggers)** | Opcionalna sinkronizacija s `--with-triggers` |
| **Sekvence** | Generatori i sekvence (strategija "kreiraj ako ne postoji") |

### 📊 **Sinkronizacija podataka (Data Diff)**

| Način | Opis | Opcija |
| --- | --- | --- |
| **Jednostavno kopiranje** | Masovni ispis podataka (`INSERT`) | `--with-data` |
| **Pametna sinkronizacija** | Usporedba po PK: `INSERT` + `UPDATE` + `DELETE` | `--with-data-diff` |

### 🔒 **Sigurnost i kontrola**

* 🛡️ **Siguran način** (`--nodelete`): Sprječava slučajna brisanja
* 🎯 **Bijela lista** (`--include-tables`): Sinkronizira samo određene tablice
* 🚫 **Crna lista** (`--exclude-tables`): Isključuje tablice iz procesa
* 📝 **SQL Skripte**: Generira `.sql` datoteke za pregled prije izvršavanja

---

## 🗄️ Podržane baze podataka

<div align="center">

| Pogon | Verzija | Izvršna datoteka | Posebne značajke |
| --- | --- | --- | --- |
| MySQL / MariaDB | 5.7+ / MariaDB 10+ | `DBComparer.exe` | Ručni način MariaDB 10.2 uz `--mariadb10` |
| PostgreSQL | 9.6+ | `DBComparerPostGre.exe` | Podrška za sheme |
| Oracle | 11g+ | `DBComparerOracle.exe` | TNS imena, Vlasnici |
| SQL Server | 2012+ | `DBComparerSQLServer.exe` | Identity stupci |
| Firebird / InterBase | 2.5+ / InterBase | `DBComparerInterbase.exe` | Generatori, Dijalekti |

</div>

---

## ⚠️ Važni zahtjevi

### 📦 Zavisnosti

Ovaj projekt koristi **[Devart UniDAC](https://www.devart.com/unidac/)** za univerzalno povezivanje.

> **⚖️ Napomena o licenci:**
> * Izvorni kod **DBComparer** distribuira se pod **MIT** licencom (besplatna upotreba).
> * Za **kompilaciju** projekta potrebna vam je **važeća komercijalna licenca za Devart UniDAC**.
> * UniDAC izvorne datoteke (`.dcu`, `.pas`) **NISU** uključene u ovaj repozitorij.
> 
> 

### 🖥️ Zahtjevi sustava

* **Delphi**: 10.4 Sydney ili noviji (11 Alexandria, 12 Athens)
* **Devart UniDAC**: Instaliran u Delphi IDE
* **Windows**: 7/8/10/11 (32-bit ili 64-bit)

---

## 🛠️ Kompilacija

1. **Klonirajte repozitorij:**
```bash
git clone https://github.com/alexdelphiAthn/DBComparer.git
cd DBComparer

```


2. **Otvorite projekt u Delphiju:**
* Odaberite `.dpr` datoteku za željeni pogon:
* `DBComparer.dpr` → MySQL/MariaDB
* `DBComparerPostGre.dpr` → PostgreSQL
* `DBComparerOracle.dpr` → Oracle
* `DBComparerSQLServer.dpr` → SQL Server
* `DBComparerInterbase.dpr` → InterBase/Firebird




3. **Kompilirajte projekt:**
* Način: **Release**
* Platforma: **Win32** ili **Win64**


4. **Generirana izvršna datoteka:**
```
DBComparer\Win32\Release\DBComparer.exe

```



---

## 🚀 Brza upotreba

### Opća sintaksa

```bash
# Skripta se zadano ispisuje na stdout; upotrijebite --output ili preusmjerite s >
IzvrsnaDatoteka.exe <Izvor> <Odredište> [Opcije] --output=datoteka_skripte.sql
IzvrsnaDatoteka.exe <Izvor> <Odredište> [Opcije] > datoteka_skripte.sql

```

### Format povezivanja

```
poslužitelj:port\baza_podataka korisnik\lozinka 

```

### Osnovni primjer

```bash
# Sinkroniziraj samo strukturu
DBComparer.exe localhost:3306\produkcija root\pass localhost:3306\razvoj root\pass > skripta_samo_struktura.sql

# Sinkroniziraj strukturu + podatke
DBComparer.exe localhost:3306\produkcija root\pass localhost:3306\razvoj root\pass --with-data-diff > skripta_podaci_struktura.sql

```

---

## 💡 Primjeri po pogonu

### 🐬 MySQL / MariaDB

```bash
# Potpuna sinkronizacija s podacima
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --with-data-diff > skripta_inkrementalna.sql

# Samo određene tablice
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --include-tables=korisnici,proizvodi > skripta_s_proizvodima.sql

# Isključi tablice logova
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --exclude-tables=logovi,revizija > skripta_bez_logova.sql

# Kompatibilnost s MariaDB 10.2 uz --nodelete i očuvanje navedenih pogleda
DBComparer.exe localhost:3306\db_prod root\pass localhost:3306\db_dev root\pass --mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting --output=mariadb10_sync.sql --encoding=utf8nobom

```

### 🐘 PostgreSQL

```bash
# Format: poslužitelj:port\baza\shema
DBComparerPostGre.exe localhost:5432\prodaja\public postgres\pass localhost:5432\prodaja\test postgres\pass > skripta.sql

# Prilagođena shema
DBComparerPostGre.exe prod-server:5432\erp\racunovodstvo admin\pass dev-server:5432\erp\racunovodstvo admin\pass > skripta_shema.sql

```

### 🔴 Oracle Database

```bash
# Izravna veza: host:port/SID user/pass@Owner
DBComparerOracle.exe 192.168.1.10:1521/ORCL system/pass@HR 192.168.1.20:1521/ORCL system/pass@HR_TEST > skripta.sql

# Korištenje TNS imena
DBComparerOracle.exe //PROD_DB system/pass //TEST_DB system/pass > skripta_tns.sql

# S određenim vlasnikom (Owner)
DBComparerOracle.exe //PROD_DB system/pass@APP_OWNER //TEST_DB system/pass@APP_OWNER > skripta_vlasnik.sql

```

### 🟦 Microsoft SQL Server

```bash
# Štiti suvišne tablice, stupce, indekse i zapise u odredištu od brisanja
DBComparerSQLServer.exe sqlserver:1433\Produkcija sa\pass sqlserver:1433\Razvoj sa\pass --nodelete > skripta_sigurna.sql

# S okidačima i podacima
DBComparerSQLServer.exe sqlserver:1433\Produkcija sa\pass sqlserver:1433\Razvoj sa\pass --with-triggers --with-data-diff > skripta_s_podacima.sql

```

### 🔥 InterBase / Firebird

```bash
# Udaljeni poslužitelj
DBComparerInterbase.exe 192.168.1.50:3050\C:\Data\prod.gdb sysdba\masterkey 192.168.1.50:3050\C:\Data\test.gdb sysdba\masterkey > \\skripte\update_skripta.sql

# Lokalna datoteka (ugrađena)
DBComparerInterbase.exe localhost\C:\Data\prod.gdb sysdba\masterkey localhost\C:\Data\test.gdb sysdba\masterkey > c:\skripta.sql

```

---

## ⚙️ Opcije naredbenog retka

| Opcija | Opis |
| --- | --- |
| `--nodelete` | 🛡️ **Siguran način**: Ne briše tablice, stupce, indekse niti zapise u odredištu |
| `--with-triggers` | 🔫 Uključuje usporedbu i ponovno kreiranje okidača (Triggers) |
| `--with-data` | 📥 Masovno kopiranje podataka (samo `INSERT`) |
| `--with-data-diff` | 🔄 Pametna sinkronizacija po PK (`INSERT`/`UPDATE`/`DELETE`) |
| `--include-tables=t1,t2` | ✅ **Bijela lista**: Obrađuje samo navedene tablice |
| `--exclude-tables=t1,t2` | ❌ **Crna lista**: Isključuje navedene tablice |
| `--mariadb10` | Izričito uključuje generiranje SQL-a kompatibilnog s MariaDB 10.2; nema automatskog otkrivanja verzije. Samo za `DBComparer.exe` |
| `--preserve-views=v1,v2` | Ne briše niti ponovno stvara navedene poglede; nazivi se uspoređuju bez razlikovanja velikih i malih slova, a nepostojeći pogledi se ne stvaraju |
| `--output=datoteka.sql` | Sprema izlaz izravno u datoteku |
| `--encoding=utf8bom\|utf8nobom\|ansi\|unicode` | Kodiranje izlazne datoteke; koristi se uz `--output`, zadano je `utf8bom` |

`--preserve-views`, `--output` i `--encoding` dostupni su u svim DBComparer izvršnim datotekama.

Za tehničke učinke i ograničenja pogledajte [MariaDB 10.2 compatibility](#mariadb-102-compatibility) (na engleskom).

### Korisne kombinacije

```bash
# Štiti suvišne tablice, stupce, indekse i zapise u odredištu od brisanja
--nodelete --with-data-diff

# Potpuna sinkronizacija s okidačima
--with-triggers --with-data-diff

# Migracija samo podataka matičnih tablica
--include-tables=kupci,proizvodi,kategorije --with-data

# MariaDB 10.2 uz --nodelete i očuvanje pogleda kojima se upravlja izvana
--mariadb10 --nodelete --preserve-views=vw_legacy,vw_reporting

```

---

## 🎯 Uobičajeni slučajevi upotrebe

### 1. **Implementacija Dev ➡️ Prod**

```bash
DBComparer.exe dev-server:3306\myapp root\pass prod-server:3306\myapp root\pass --nodelete > skripta.sql

```

### 2. **Kloniranje strukture bez podataka**

```bash
DBComparer.exe izvor:3306\db user\pass odrediste:3306\db user\pass > skripta.sql

```

### 3. **Replikacija matičnih tablica**

```bash
DBComparer.exe prod:3306\erp user\pass dev:3306\erp user\pass --include-tables=drzave,pokrajine,kategorije --with-data > skripta.sql

```

### 4. **Kontinuirana sinkronizacija (CI/CD)**

```bash
# U Jenkins/GitLab CI skripti
DBComparerPostGre.exe prod-db:5432\app\public admin\pass stage-db:5432\app\public admin\pass --with-data-diff --nodelete > skripta.sql

```

### 5. **Migracija između različitih pogona**

```bash
# MySQL ➡️ PostgreSQL (zahtijeva ručni izvoz/uvoz)
# DBComparer generira kompatibilne DDL skripte

```

---

## 📄 Licenca

Ovaj projekt licenciran je pod **MIT licencom**.

```
Copyright (c) 2025 Alejandro Laorden Hidalgo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction...

```

Pogledajte datoteku [LICENSE](https://www.google.com/search?q=LICENSE) za više detalja.

---

## ⚠️ Odricanje od odgovornosti

> **Ovaj softver se daje "KAKAV JEST", bez jamstva bilo koje vrste**, izričitog ili impliciranog, uključujući, ali ne ograničavajući se na jamstva prodajnosti, prikladnosti za određenu svrhu i nekršenja prava.
> **⚠️ KRITIČNA PREPORUKA:**
> Izvršite **potpune sigurnosne kopije (backup)** vaše odredišne baze podataka prije pokretanja bilo kakvih skripti za sinkronizaciju u **produkcijskim okruženjima**.
</details>
