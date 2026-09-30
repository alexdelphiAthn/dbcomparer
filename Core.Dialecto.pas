{******************************************************************************}
{                                                                              }
{  Módulo:       Core.Dialecto                                                 }
{    Tipo:       Librería                                                      }
{ Versión:       1.0.0                                                         }
{   Fecha:       24/09/2026                                                    }
{   Autor:       Alejandro Laorden Hidalgo                                     }
{                                                                              }
{  Copyright (c) Alejandro Laorden Hidalgo.                                    }
{  SPDX-License-Identifier: MPL-2.0                                            }
{  Descripción:                                                                }
{    Criterio único para el SQL que emite DBComparer. El destino (MariaDB 12,  }
{    MariaDB 10 o MySQL 8.0.41) decide, en un solo sitio, cómo se protege      }
{    cada cambio para poder relanzarlo, qué intercalaciones hay que rebajar    }
{    y si vistas y rutinas pasan por la conversión de MySQL 8. Comparar dos    }
{    bases y convertir un volcado siguen las mismas reglas.                    }
{******************************************************************************}
unit Core.Dialecto;

interface

type
  TDialectoDestino = (
    // Se decide al conectar, con SELECT VERSION() del destino.
    ddAuto,
    ddMariaDB12,
    ddMariaDB10,
    ddMySQL841);

  TCriteriosDialecto = record
    Dialecto: TDialectoDestino;
    // ADD COLUMN / ADD INDEX / DROP ... IF [NOT] EXISTS del propio motor
    // (MariaDB). Sin ello, la guarda consulta INFORMATION_SCHEMA y ejecuta
    // con PREPARE (MySQL 8 no tiene esas cláusulas).
    SiExisteNativo: Boolean;
    // uca1400 (MariaDB 11+), CURRENT_TIMESTAMP() y utf8mb3 no existen en
    // MariaDB 10 ni en MySQL 8: se rebajan a lo que ambos entienden.
    RebajarIntercalaciones: Boolean;
    // CREATE OR REPLACE TABLE a secas no existe en MySQL 8; en MariaDB 10
    // se deja en CREATE TABLE. Con la conversión de MySQL 8 se sustituye
    // por DROP + CREATE, así que no hay que tocarlo antes.
    QuitarOrReplaceTabla: Boolean;
    // Vistas y rutinas pasan por Conversion.MySQL841: SQL SECURITY
    // INVOKER, sin DEFINER, sin @@in_transaction, COLLATE con CHARACTER
    // SET y la caja declarada de tablas y vistas.
    ConvertirMySQL841: Boolean;
    class function Para(
      ADialecto: TDialectoDestino): TCriteriosDialecto; static;
  end;

// mariadb12, mariadb10, mysql841 o auto. Lanza EArgumentException con los
// valores válidos si no reconoce el nombre.
function DialectoDesdeNombre(const ANombre: string): TDialectoDestino;
function NombreDialecto(ADialecto: TDialectoDestino): string;
function DescripcionDialecto(ADialecto: TDialectoDestino): string;
// A partir de lo que devuelve SELECT VERSION(): «12.3.1-MariaDB»,
// «10.11.8-MariaDB-log», «8.0.41»... Una MariaDB 11 o posterior cuenta como
// la 12 (misma sintaxis); MySQL, de cualquier versión, como la 8.0.41.
function DialectoDesdeVersion(const AVersion: string): TDialectoDestino;
// Quita de un sql_mode de MariaDB los modos que MySQL 8 no reconoce: con
// uno solo de ellos, SET SQL_MODE falla y el script se detiene.
function FiltrarSqlModeMySQL841(const ASqlMode: string): string;
// Ejecuta AComando solo si ACuentaExistentes devuelve 0 (o, con
// ASiExiste, solo si devuelve algo). Es la guarda de los destinos sin
// IF [NOT] EXISTS: consulta INFORMATION_SCHEMA y ejecuta con PREPARE.
// Cada sentencia acaba en ATerminador y va en su línea (ASalto).
function GuardarComandoSegunCatalogo(const ACuentaExistentes,
  AComando: string; ASiExiste: Boolean; const ATerminador: string = ';';
  const ASalto: string = sLineBreak): string;
// Consultas de recuento para la guarda anterior.
function CuentaColumnaCatalogo(const ATabla, AColumna: string): string;
function CuentaIndiceCatalogo(const ATabla, AIndice: string): string;

implementation

uses
  System.StrUtils,
  System.SysUtils;

const
  cNombresDialecto: array[TDialectoDestino] of string = (
    'auto',
    'mariadb12',
    'mariadb10',
    'mysql841');
  cDescripcionesDialecto: array[TDialectoDestino] of string = (
    'según el servidor de destino',
    'MariaDB 11 o posterior',
    'MariaDB 10.2 a 10.11',
    'MySQL 8.0.41');
  // Modos de MariaDB (o retirados en MySQL 8.0) que MySQL 8 rechaza.
  cModosSinMySQL841: array[0..14] of string = (
    'NO_AUTO_CREATE_USER',
    'DB2',
    'MAXDB',
    'MSSQL',
    'MYSQL323',
    'MYSQL40',
    'ORACLE',
    'POSTGRESQL',
    'NO_FIELD_OPTIONS',
    'NO_KEY_OPTIONS',
    'NO_TABLE_OPTIONS',
    'EMPTY_STRING_IS_NULL',
    'SIMULTANEOUS_ASSIGNMENT',
    'TIME_ROUND_FRACTIONAL',
    'NO_ZERO_IN_DATE_OLD');

{ TCriteriosDialecto }

class function TCriteriosDialecto.Para(
  ADialecto: TDialectoDestino): TCriteriosDialecto;
begin
  Result := Default(TCriteriosDialecto);
  Result.Dialecto := ADialecto;
  case ADialecto of
    ddMariaDB10:
      begin
        Result.SiExisteNativo := True;
        Result.RebajarIntercalaciones := True;
        Result.QuitarOrReplaceTabla := True;
      end;
    ddMySQL841:
      begin
        Result.RebajarIntercalaciones := True;
        Result.ConvertirMySQL841 := True;
      end;
  else
    // MariaDB 12 (y auto mientras no se resuelva): la sintaxis del origen.
    Result.SiExisteNativo := True;
  end;
end;

function DialectoDesdeNombre(const ANombre: string): TDialectoDestino;
var
  Dialecto: TDialectoDestino;
  bEncontrado: Boolean;
begin
  Result := ddAuto;
  bEncontrado := False;
  for Dialecto := Low(TDialectoDestino) to High(TDialectoDestino) do
  begin
    if not bEncontrado and
       SameText(Trim(ANombre), cNombresDialecto[Dialecto]) then
    begin
      Result := Dialecto;
      bEncontrado := True;
    end;
  end;
  if not bEncontrado then
    raise EArgumentException.CreateFmt(
      'Destino no válido: "%s". Use mariadb12, mariadb10, mysql841 o auto.',
      [ANombre]);
end;

function NombreDialecto(ADialecto: TDialectoDestino): string;
begin
  Result := cNombresDialecto[ADialecto];
end;

function DescripcionDialecto(ADialecto: TDialectoDestino): string;
begin
  Result := cDescripcionesDialecto[ADialecto];
end;

function VersionPrincipal(const AVersion: string): Integer;
var
  iFin: Integer;
begin
  iFin := 1;
  while (iFin <= Length(AVersion)) and
        CharInSet(AVersion[iFin], ['0'..'9']) do
    Inc(iFin);
  Result := StrToIntDef(Copy(AVersion, 1, iFin - 1), 0);
end;

function DialectoDesdeVersion(const AVersion: string): TDialectoDestino;
var
  sVersion: string;
begin
  sVersion := Trim(AVersion);
  // Hay MariaDB que se anuncian con 5.5.5- delante por compatibilidad.
  if StartsText('5.5.5-', sVersion) then
    sVersion := Copy(sVersion, Length('5.5.5-') + 1, MaxInt);
  if not ContainsText(sVersion, 'mariadb') then
    Result := ddMySQL841
  else if VersionPrincipal(sVersion) >= 11 then
    Result := ddMariaDB12
  else
    Result := ddMariaDB10;
end;

function FiltrarSqlModeMySQL841(const ASqlMode: string): string;
var
  aModos: TArray<string>;
  sModo: string;
begin
  Result := '';
  aModos := ASqlMode.Split([',']);
  for sModo in aModos do
  begin
    if (Trim(sModo) <> '') and
       (IndexText(Trim(sModo), cModosSinMySQL841) < 0) then
    begin
      if Result <> '' then
        Result := Result + ',';
      Result := Result + Trim(sModo);
    end;
  end;
end;

function QuoteDynamicSQL(const SQL: string): string;
begin
  Result := QuotedStr(StringReplace(SQL, '\', '\\', [rfReplaceAll]));
end;

function GuardarComandoSegunCatalogo(const ACuentaExistentes,
  AComando: string; ASiExiste: Boolean; const ATerminador: string;
  const ASalto: string): string;
var
  Condicion: string;
begin
  if ASiExiste then
    Condicion := '@fza_existe > 0'
  else
    Condicion := '@fza_existe = 0';
  // DO 0 no devuelve filas: quien ejecuta el script no recibe un
  // resultado vacío por cada guarda que no hace nada.
  Result :=
    'SET @fza_existe := (' + ACuentaExistentes + ')' + ATerminador + ASalto +
    'SET @fza_sql := IF(' + Condicion + ', ' + QuoteDynamicSQL(AComando) +
    ', ''DO 0'')' + ATerminador + ASalto +
    'PREPARE fza_stmt FROM @fza_sql' + ATerminador + ASalto +
    'EXECUTE fza_stmt' + ATerminador + ASalto +
    'DEALLOCATE PREPARE fza_stmt' + ATerminador;
end;

function CuentaColumnaCatalogo(const ATabla, AColumna: string): string;
begin
  Result := 'SELECT COUNT(*) FROM information_schema.COLUMNS ' +
    'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ' +
    QuotedStr(ATabla) + ' AND COLUMN_NAME = ' + QuotedStr(AColumna);
end;

function CuentaIndiceCatalogo(const ATabla, AIndice: string): string;
begin
  Result := 'SELECT COUNT(*) FROM information_schema.STATISTICS ' +
    'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ' +
    QuotedStr(ATabla) + ' AND INDEX_NAME = ' + QuotedStr(AIndice);
end;

end.
