{******************************************************************************}
{                                                                              }
{  Módulo:       Conversion.MySQL841                                           }
{    Tipo:       Librería                                                      }
{ Versión:       1.0.0                                                         }
{   Fecha:       11/09/2026                                                    }
{   Autor:       Alejandro Laorden Hidalgo                                     }
{                                                                              }
{  Copyright (c) Alejandro Laorden Hidalgo.                                    }
{  SPDX-License-Identifier: MPL-2.0                                            }
{  Descripción:                                                                }
{    Convierte un volcado de Factuzam (MariaDB, FZAM_COPIA_SEGURIDAD_SQL) en   }
{    un script cargable en MySQL 8.0.41 sobre Linux: intercalación de la       }
{    base, tipos de MySQL 8, vistas y procedimientos con SQL SECURITY INVOKER  }
{    y sin DEFINER, CREATE OR REPLACE, COLLATE sin CHARACTER SET, sustitución  }
{    de @@in_transaction y nombres de tablas y vistas con la caja declarada.   }
{    Omite los SET de variables de MariaDB, rebaja uca1400, convierte los      }
{    IF [NOT] EXISTS de índices y columnas en guardas y, si una sentencia usa  }
{    dos veces una tabla temporal (MySQL no lo admite), la copia antes. Fija   }
{    la intercalación de la conexión (utf8mb4_spanish_ci) y pasa los SET       }
{    DEFAULT CURRENT_TIMESTAMP de ALTER COLUMN a MODIFY COLUMN. Las tablas     }
{    seq_A_to_B (motor SEQUENCE de MariaDB) pasan a una tabla derivada.        }
{******************************************************************************}
unit Conversion.MySQL841;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections,
  System.RegularExpressions, Conversion.Sentencias, Core.Dialecto;

type
  TInformeConversionMySQL841 = class
  private
    FAvisos: TStringList;
    FProcedimientosSonda: TStringList;
  public
    TablasAdaptadas: Integer;
    VistasInvoker: Integer;
    ProcedimientosInvoker: Integer;
    ProcedimientosSinOrReplace: Integer;
    TablasOrReplaceCorregidas: Integer;
    DeclaracionesCollate: Integer;
    NombresCorregidos: Integer;
    LiteralesDatosCorregidos: Integer;
    SentenciasSesionOmitidas: Integer;
    IntercalacionesRebajadas: Integer;
    GuardasCatalogo: Integer;
    SentenciasTemporalCopiada: Integer;
    DefectosTemporalesConvertidos: Integer;
    SecuenciasConvertidas: Integer;
    constructor Create;
    destructor Destroy; override;
    function Resumen: string;
    property Avisos: TStringList read FAvisos;
    property ProcedimientosSonda: TStringList read FProcedimientosSonda;
  end;

  TConversorMySQL841 = class
  private
    FNombres: TDictionary<string, string>;
    // Tablas temporales que crea el volcado (en minúsculas).
    FTemporales: TDictionary<string, string>;
    FInforme: TInformeConversionMySQL841;
    FSalida: TStringBuilder;
    FSaltoLinea: string;
    FDelimitador: string;
    FNecesitaSonda: Boolean;
    FSondaEmitida: Boolean;
    FNotaEmitida: Boolean;
    FIntercalacionEmitida: Boolean;
    // El script no fija la conexión: se pone SET NAMES delante.
    FNamesPendiente: Boolean;
    // Estado de los evaluadores de TRegEx.Replace (punteros a método).
    FSaltoCuerpo: string;
    FCuantosTemporal: Integer;
    FCuantosNombres: Integer;
    FCuantosIntercalacion: Integer;
    function EvaluarSinAncho(const AMatch: TMatch): string;
    function EvaluarDropMasCreate(const AMatch: TMatch): string;
    function EvaluarNombreDeclarado(const AMatch: TMatch): string;
    function EvaluarUca1400(const AMatch: TMatch): string;
    function EvaluarSecuencia(const AMatch: TMatch): string;
    procedure Analizar(const AElementos: TArray<TElementoVolcado>);
    procedure RegistrarNombres(const ACodigo: string);
    procedure Emitir(const AElemento: TElementoVolcado);
    procedure EmitirComentario(const AElemento: TElementoVolcado);
    procedure EmitirSentencia(const AElemento: TElementoVolcado);
    procedure EmitirOmitida(const AElemento: TElementoVolcado);
    procedure EmitirSonda;
    procedure EmitirIntercalacion(const ATerminador: string);
    function ConvertirCodigo(var AMascara: TTextoEnmascarado;
      out APrevio: string): string;
    function ConvertirCreateTable(const ACodigo: string): string;
    function ConvertirVista(const ACodigo: string): string;
    function ConvertirProcedimiento(const ACodigo: string;
      out ADrop: string): string;
    function ConvertirCuerpoProcedimiento(
      const ANombre, ACuerpo: string): string;
    function AsegurarSecurityInvoker(const AResto, ASalto: string): string;
    function EvaluarCollate(const AMatch: TMatch): string;
    function CorregirCollate(const ATexto: string): string;
    function SustituirNombres(const ATexto: string;
      var ACuantos: Integer): string;
    procedure NormalizarLiteralesDatos(var AMascara: TTextoEnmascarado);
    procedure DetectarNoConvertible(const AObjeto, ACodigo: string);
    function RebajarUca1400(const AObjeto, ACodigo: string): string;
    function ProtegerSiExiste(const AMascara: TTextoEnmascarado;
      const ACodigo: string): string;
    function CopiarTemporalesReabiertas(const AObjeto,
      ACodigo: string): string;
    function ConvertirDefectoTemporal(const AMascara: TTextoEnmascarado;
      const ACodigo: string): string;
    procedure CopiarEnFragmento(var ATexto: string; AInicio, AFin: Integer;
      const ASeparador, AObjeto: string);
  public
    constructor Create;
    destructor Destroy; override;
    function Convertir(const AVolcado: string): string;
    property Informe: TInformeConversionMySQL841 read FInforme;
  end;

  TLineaTexto = record
    Texto: string;
    Salto: string;
  end;

  // Sustituye @@in_transaction (solo MariaDB) por una variable local que
  // PRC_FZA_EN_TRANSACCION rellena justo antes de cada IF que la consulta.
  TAdaptadorEnTransaccion = class
  private
    FNombre: string;
    FLineas: TArray<TLineaTexto>;
    FInsertarAntes: TList<Integer>;
    FAvisos: TStrings;
    FIndiceBegin: Integer;
    function PrimeraPalabra(AIndice: Integer): string;
    function Sangria(AIndice: Integer): string;
    function SaltoLinea(AIndice: Integer): string;
    function SangriaDeclaracion: string;
    function BuscarIfGobernante(AIndice: Integer): Integer;
    function BuscarInicioCondicion(AIndice: Integer): Integer;
    function BuscarBegin: Integer;
    procedure Marcar(AIndice: Integer);
    procedure Avisar(AIndice: Integer; const AMotivo: string);
    procedure Clasificar(AIndice: Integer);
    function Reconstruir: string;
  public
    constructor Create(const ANombre, ACuerpo: string; AAvisos: TStrings);
    destructor Destroy; override;
    function Adaptar: string;
    class function UsaEnTransaccion(const ATexto: string): Boolean; static;
  end;

function PartirLineas(const ATexto: string): TArray<TLineaTexto>;
function UnirLineas(const ALineas: TArray<TLineaTexto>): string;
function SaltoDeLineaDe(const ATexto: string): string;

const
  PROCEDIMIENTO_SONDA = 'PRC_FZA_EN_TRANSACCION';
  VARIABLE_SONDA = 'v_fza_en_transaccion';
  NOTA_CONVERSION = '-- Convertido para MySQL 8.0.41 (Linux, ' +
    'lower_case_table_names=0) por DBComparer --mysql841';
  // MySQL 8 toma utf8mb4_0900_ai_ci con SET NAMES utf8mb4 a secas: las
  // rutinas guardan esa intercalación y sus variables chocan con las
  // columnas de Factuzam ("Illegal mix of collations").
  INTERCALACION_CONEXION = 'utf8mb4_spanish_ci';
  SENTENCIA_NAMES = 'SET NAMES utf8mb4 COLLATE ' + INTERCALACION_CONEXION;

implementation

uses
  System.StrUtils;

const
  PATRON_CABECERA_PROCEDIMIENTO = '^(\s*)CREATE\s+(OR\s+REPLACE\s+)?' +
    '(DEFINER\s*=\s*\S+\s+)?PROCEDURE\s+(`?)([A-Za-z0-9_$]+)\4\s*\(';
  PATRON_VISTA = '^(\s*CREATE\s+(?:OR\s+REPLACE\s+)?)' +
    '(ALGORITHM\s*=\s*\w+\s+)?(?:DEFINER\s*=\s*\S+\s+)?' +
    '(?:SQL\s+SECURITY\s+\w+\s+)?VIEW\b';
  PATRON_NOMBRE_VISTA = '^\s*CREATE\s+(?:OR\s+REPLACE\s+)?' +
    '(?:ALGORITHM\s*=\s*\w+\s+)?(?:DEFINER\s*=\s*\S+\s+)?' +
    '(?:SQL\s+SECURITY\s+\w+\s+)?VIEW\s+`?([A-Za-z0-9_]+)`?';
  PATRON_NOMBRE_TABLA = '^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?' +
    '`?([A-Za-z0-9_]+)`?';
  PATRON_INSERT = '^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+`?([A-Za-z0-9_]+)`?';
  PATRON_BEGIN = '^\s*(?:\w+\s*:\s*)?BEGIN\b';
  PATRON_IDENTIFICADOR = '\b[A-Za-z_][A-Za-z0-9_]*\b';
  TABLAS_CON_SQL_EN_DATOS = 'fza_usuarios_perfiles,fza_informes_guias';
  // SET de variables que solo tiene MariaDB (mysqldump 10.6.16+ y las
  // copias de Factuzam): en MySQL fallan con 1193.
  PATRON_SET_SOLO_MARIADB = '^\s*SET\b[^;]*\bNOTE_VERBOSITY\b';
  // ALTER COLUMN ... SET DEFAULT solo admite en MySQL un literal o una
  // expresión entre paréntesis, que queda como now() y no como
  // CURRENT_TIMESTAMP en INFORMATION_SCHEMA.
  PATRON_DEFECTO_TEMPORAL = '^\s*ALTER\s+TABLE\s+' + '`?([A-Za-z0-9_]+)`?' +
    '\s+ALTER\s+(?:COLUMN\s+)?`?([A-Za-z0-9_]+)`?\s+SET\s+DEFAULT\s+' +
    '(?:CURRENT_TIMESTAMP(?:\s*\(\s*\))?|NOW\s*\(\s*\))\s*$';
  // FROM/JOIN seq_A_to_B [alias]: tablas virtuales del motor SEQUENCE de
  // MariaDB (una columna seq). Grupo 5: el alias, si lo hay.
  PATRON_SECUENCIA = '(\bFROM|\bJOIN|,)(\s*)`?seq_(\d{1,15})_to_(\d{1,15})`?' +
    '(?![A-Za-z0-9_])(\s+(?:AS\s+)?(?!(?:WHERE|JOIN|LEFT|RIGHT|INNER|CROSS|' +
    'STRAIGHT_JOIN|NATURAL|ON|USING|GROUP|ORDER|LIMIT|UNION|HAVING|WINDOW|' +
    'FOR|LOCK|INTO)\b)[A-Za-z_][A-Za-z0-9_]*)?';
  PATRON_CREAR_TEMPORAL = '\bCREATE\s+(?:OR\s+REPLACE\s+)?TEMPORARY\s+' +
    'TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([A-Za-z0-9_]+)`?';
  // Un script suelto puede usar una temporal que crea otro, pero la
  // borra con DROP TEMPORARY TABLE: también cuenta.
  PATRON_BORRAR_TEMPORAL = '\bDROP\s+TEMPORARY\s+TABLE\s+(?:IF\s+EXISTS\s+)?' +
    '((?:`?[A-Za-z0-9_]+`?\s*,\s*)*`?[A-Za-z0-9_]+`?)';
  // Tabla que usa una sentencia: tras FROM, JOIN, UPDATE, INTO o una coma
  // de la lista de tablas (no un calificador tabla.columna).
  PATRON_USO_TABLA = '(\bFROM|\bJOIN|\bUPDATE|\bINTO|,)\s*`?' +
    '([A-Za-z0-9_]+)\b`?(?!\s*`?\.)';
  IDENTIFICADOR = '`?([A-Za-z0-9_]+)`?';
  PATRON_CREATE_INDEX_SI_NO_EXISTE = '^\s*CREATE\s+(?:UNIQUE\s+|' +
    'FULLTEXT\s+|SPATIAL\s+)?INDEX\s+(IF\s+NOT\s+EXISTS\s+)' +
    IDENTIFICADOR + '\s+ON\s+' + IDENTIFICADOR;
  PATRON_DROP_INDEX_SI_EXISTE = '^\s*DROP\s+INDEX\s+(IF\s+EXISTS\s+)' +
    IDENTIFICADOR + '\s+ON\s+' + IDENTIFICADOR + '\s*$';
  // ALTER TABLE con una sola cláusula IF [NOT] EXISTS: grupo 1 la tabla,
  // grupo 2 la cláusula que se quita y grupo 3 el índice o la columna.
  PATRON_ALTER_TABLE = '^\s*ALTER\s+TABLE\s+' + IDENTIFICADOR + '\s+';
  PATRON_ADD_INDEX_SI_NO_EXISTE = 'ADD\s+(?:CONSTRAINT\s+(?:`?\w+`?\s+)?)?' +
    '(?:UNIQUE\s+(?:INDEX\s+|KEY\s+)?|(?:FULLTEXT\s+|SPATIAL\s+)?' +
    '(?:INDEX|KEY)\s+)(IF\s+NOT\s+EXISTS\s+)' + IDENTIFICADOR;
  PATRON_ADD_COLUMNA_SI_NO_EXISTE = 'ADD\s+(?:COLUMN\s+)?' +
    '(IF\s+NOT\s+EXISTS\s+)' + IDENTIFICADOR;
  PATRON_DROP_INDEX_ALTER_SI_EXISTE = 'DROP\s+(?:INDEX|KEY)\s+' +
    '(IF\s+EXISTS\s+)' + IDENTIFICADOR + '\s*$';
  PATRON_DROP_COLUMNA_SI_EXISTE = 'DROP\s+(?:COLUMN\s+)?' +
    '(IF\s+EXISTS\s+)' + IDENTIFICADOR + '\s*$';

type
  TConstruccionNoConvertible = record
    Patron: string;
    Descripcion: string;
  end;

const
  CONSTRUCCIONES_NO_CONVERTIBLES: array[0..7] of TConstruccionNoConvertible = (
    (Patron: '\bEXECUTE\s+IMMEDIATE\b';
     Descripcion: 'EXECUTE IMMEDIATE (usar PREPARE/EXECUTE/DEALLOCATE)'),
    (Patron: '\bBEGIN\s+NOT\s+ATOMIC\b';
     Descripcion: 'bloque BEGIN NOT ATOMIC (solo MariaDB)'),
    (Patron: '\bADD\s+(?:COLUMN\s+)?IF\s+NOT\s+EXISTS\b';
     Descripcion: 'ADD COLUMN IF NOT EXISTS (consultar INFORMATION_SCHEMA)'),
    (Patron: '\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\b';
     Descripcion: 'CREATE INDEX IF NOT EXISTS (consultar INFORMATION_SCHEMA)'),
    (Patron: '\bDROP\s+INDEX\s+IF\s+EXISTS\b';
     Descripcion: 'DROP INDEX IF EXISTS (consultar INFORMATION_SCHEMA)'),
    (Patron: '^\s*FOR\s+\w+\s+IN\b';
     Descripcion: 'bucle FOR ... IN ... DO (solo MariaDB)'),
    (Patron: '\bRETURNING\b';
     Descripcion: 'cláusula RETURNING (solo MariaDB)'),
    (Patron: '\bCREATE\s+OR\s+REPLACE\s+(?:INDEX|TRIGGER|FUNCTION|EVENT)\b';
     Descripcion: 'CREATE OR REPLACE de índice, trigger, función o evento')
  );

// ============================================================================
//   Utilidades de líneas
// ============================================================================

function SaltoDeLineaDe(const ATexto: string): string;
begin
  if Pos(#13#10, ATexto) > 0 then
    Result := #13#10
  else
    Result := #10;
end;

function PartirLineas(const ATexto: string): TArray<TLineaTexto>;
var
  oLineas: TList<TLineaTexto>;
  oLinea: TLineaTexto;
  iLongitud, iInicio, i: Integer;
begin
  oLineas := TList<TLineaTexto>.Create;
  try
    iLongitud := Length(ATexto);
    iInicio := 1;
    i := 1;
    while i <= iLongitud do
    begin
      if CharInSet(ATexto[i], [#13, #10]) then
      begin
        oLinea.Texto := Copy(ATexto, iInicio, i - iInicio);
        if (ATexto[i] = #13) and (i < iLongitud) and (ATexto[i + 1] = #10) then
          oLinea.Salto := #13#10
        else
          oLinea.Salto := ATexto[i];
        oLineas.Add(oLinea);
        Inc(i, Length(oLinea.Salto));
        iInicio := i;
      end
      else
        Inc(i);
    end;
    if iInicio <= iLongitud then
    begin
      oLinea.Texto := Copy(ATexto, iInicio, iLongitud - iInicio + 1);
      oLinea.Salto := '';
      oLineas.Add(oLinea);
    end;
    Result := oLineas.ToArray;
  finally
    FreeAndNil(oLineas);
  end;
end;

function UnirLineas(const ALineas: TArray<TLineaTexto>): string;
var
  oSalida: TStringBuilder;
  oLinea: TLineaTexto;
begin
  oSalida := TStringBuilder.Create;
  try
    for oLinea in ALineas do
      oSalida.Append(oLinea.Texto).Append(oLinea.Salto);
    Result := oSalida.ToString;
  finally
    FreeAndNil(oSalida);
  end;
end;

// ============================================================================
//   TInformeConversionMySQL841
// ============================================================================

constructor TInformeConversionMySQL841.Create;
begin
  inherited Create;
  FAvisos := TStringList.Create;
  FProcedimientosSonda := TStringList.Create;
end;

destructor TInformeConversionMySQL841.Destroy;
begin
  FreeAndNil(FProcedimientosSonda);
  FreeAndNil(FAvisos);
  inherited;
end;

function TInformeConversionMySQL841.Resumen: string;
var
  oTexto: TStringBuilder;
  sAviso: string;
begin
  oTexto := TStringBuilder.Create;
  try
    oTexto.AppendLine('Conversión a MySQL 8.0.41:');
    oTexto.AppendLine(Format('  Tablas adaptadas (CURRENT_TIMESTAMP, anchos ' +
      'de int, DEFAULT en text): %d', [TablasAdaptadas]));
    oTexto.AppendLine(Format('  Vistas con SQL SECURITY INVOKER: %d',
      [VistasInvoker]));
    oTexto.AppendLine(Format('  Procedimientos con SQL SECURITY INVOKER: %d',
      [ProcedimientosInvoker]));
    oTexto.AppendLine(Format('  CREATE OR REPLACE PROCEDURE sustituidos por ' +
      'DROP + CREATE: %d', [ProcedimientosSinOrReplace]));
    oTexto.AppendLine(Format('  CREATE OR REPLACE TABLE dentro de ' +
      'procedimientos: %d', [TablasOrReplaceCorregidas]));
    oTexto.AppendLine(Format('  Declaraciones con COLLATE sin CHARACTER SET ' +
      'corregidas: %d', [DeclaracionesCollate]));
    oTexto.AppendLine(Format('  Procedimientos que usaban @@in_transaction ' +
      '(sonda %s): %d', [PROCEDIMIENTO_SONDA, ProcedimientosSonda.Count]));
    if ProcedimientosSonda.Count > 0 then
      oTexto.AppendLine('    ' + ProcedimientosSonda.CommaText);
    oTexto.AppendLine(Format('  Referencias a tablas/vistas con otra caja ' +
      'corregidas: %d', [NombresCorregidos]));
    oTexto.AppendLine(Format('  Literales de datos con nombres de tabla ' +
      'corregidos: %d', [LiteralesDatosCorregidos]));
    oTexto.AppendLine(Format('  SET de variables de MariaDB omitidos ' +
      '(NOTE_VERBOSITY): %d', [SentenciasSesionOmitidas]));
    oTexto.AppendLine(Format('  Intercalaciones uca1400 rebajadas a ' +
      'spanish_ci: %d', [IntercalacionesRebajadas]));
    oTexto.AppendLine(Format('  IF [NOT] EXISTS de índices y columnas ' +
      'convertidos en guarda: %d', [GuardasCatalogo]));
    oTexto.AppendLine(Format('  Sentencias que usaban dos veces una tabla ' +
      'temporal (copiada antes): %d', [SentenciasTemporalCopiada]));
    oTexto.AppendLine(Format('  ALTER COLUMN ... SET DEFAULT CURRENT_TIMESTAMP ' +
      'pasados a MODIFY COLUMN: %d', [DefectosTemporalesConvertidos]));
    oTexto.AppendLine(Format('  Tablas seq_A_to_B (SEQUENCE de MariaDB) ' +
      'convertidas: %d', [SecuenciasConvertidas]));
    oTexto.AppendLine(Format('  Avisos (revisar a mano): %d', [Avisos.Count]));
    for sAviso in Avisos do
      oTexto.AppendLine('    - ' + sAviso);
    Result := oTexto.ToString;
  finally
    FreeAndNil(oTexto);
  end;
end;

// ============================================================================
//   TConversorMySQL841: recorrido del volcado
// ============================================================================

constructor TConversorMySQL841.Create;
begin
  inherited Create;
  FNombres := TDictionary<string, string>.Create;
  FTemporales := TDictionary<string, string>.Create;
  FInforme := TInformeConversionMySQL841.Create;
  FSalida := TStringBuilder.Create;
  FSaltoLinea := #13#10;
  FDelimitador := ';';
end;

destructor TConversorMySQL841.Destroy;
begin
  FreeAndNil(FSalida);
  FreeAndNil(FInforme);
  FreeAndNil(FTemporales);
  FreeAndNil(FNombres);
  inherited;
end;

function TConversorMySQL841.Convertir(const AVolcado: string): string;
var
  aElementos: TArray<TElementoVolcado>;
  oElemento: TElementoVolcado;
begin
  FSaltoLinea := SaltoDeLineaDe(AVolcado);
  FDelimitador := ';';
  FSondaEmitida := False;
  FNotaEmitida := False;
  FIntercalacionEmitida := False;
  FNamesPendiente := True;
  FNecesitaSonda := False;
  FNombres.Clear;
  FTemporales.Clear;
  FSalida.Clear;
  FSalida.Capacity := Length(AVolcado) + 4096;
  aElementos := TLectorVolcadoSql.TrocearTexto(AVolcado);
  Analizar(aElementos);
  for oElemento in aElementos do
    Emitir(oElemento);
  Result := FSalida.ToString;
end;

procedure TConversorMySQL841.Analizar(
  const AElementos: TArray<TElementoVolcado>);
var
  oElemento: TElementoVolcado;
  oMascara: TTextoEnmascarado;
  oTemporal: TMatch;
  sNombre, sLimpio: string;
begin
  for oElemento in AElementos do
  begin
    if oElemento.Tipo = tevSentencia then
    begin
      oMascara := TTextoEnmascarado.Crear(oElemento.Texto);
      RegistrarNombres(oMascara.Codigo);
      if TRegEx.IsMatch(oMascara.Codigo, '^\s*SET\s+NAMES\b',
        [roIgnoreCase]) then
        FNamesPendiente := False;
      for oTemporal in TRegEx.Matches(oMascara.Codigo, PATRON_CREAR_TEMPORAL,
        [roIgnoreCase]) do
        FTemporales.AddOrSetValue(LowerCase(oTemporal.Groups[1].Value),
          oTemporal.Groups[1].Value);
      for oTemporal in TRegEx.Matches(oMascara.Codigo, PATRON_BORRAR_TEMPORAL,
        [roIgnoreCase]) do
        for sNombre in oTemporal.Groups[1].Value.Split([',']) do
        begin
          sLimpio := Trim(sNombre).Trim(['`']);
          if not FTemporales.ContainsKey(LowerCase(sLimpio)) then
            FTemporales.Add(LowerCase(sLimpio), sLimpio);
        end;
      if TRegEx.IsMatch(oMascara.Codigo, PATRON_CABECERA_PROCEDIMIENTO,
        [roIgnoreCase])
        and TAdaptadorEnTransaccion.UsaEnTransaccion(oMascara.Codigo) then
        FNecesitaSonda := True;
    end;
  end;
end;

procedure TConversorMySQL841.RegistrarNombres(const ACodigo: string);
var
  oCoincidencia: TMatch;
  sNombre: string;
begin
  oCoincidencia := TRegEx.Match(ACodigo, PATRON_NOMBRE_TABLA, [roIgnoreCase]);
  if not oCoincidencia.Success then
    oCoincidencia := TRegEx.Match(ACodigo, PATRON_NOMBRE_VISTA,
      [roIgnoreCase]);
  if oCoincidencia.Success then
  begin
    sNombre := oCoincidencia.Groups[1].Value;
    if not FNombres.ContainsKey(LowerCase(sNombre)) then
      FNombres.Add(LowerCase(sNombre), sNombre);
  end;
end;

procedure TConversorMySQL841.Emitir(const AElemento: TElementoVolcado);
begin
  case AElemento.Tipo of
    tevBlanco:
      FSalida.Append(AElemento.Texto);
    tevComentario:
      EmitirComentario(AElemento);
    tevDelimitador:
      begin
        FDelimitador := AElemento.Delimitador;
        FSalida.Append(AElemento.Texto);
      end;
    tevSentencia:
      EmitirSentencia(AElemento);
  end;
end;

procedure TConversorMySQL841.EmitirComentario(
  const AElemento: TElementoVolcado);
begin
  // La sonda va delante del primer "-- Procedimiento:" para no separar ese
  // comentario de su DROP/CREATE.
  if FNecesitaSonda and not FSondaEmitida
    and StartsText('-- Procedimiento:', Trim(AElemento.Texto)) then
    EmitirSonda;
  FSalida.Append(AElemento.Texto);
  if not FNotaEmitida
    and SameText(Trim(AElemento.Texto), '-- FZAM_COPIA_SEGURIDAD_SQL') then
  begin
    FSalida.Append(FSaltoLinea).Append(NOTA_CONVERSION);
    FNotaEmitida := True;
  end;
end;

procedure TConversorMySQL841.EmitirSentencia(
  const AElemento: TElementoVolcado);
var
  oMascara: TTextoEnmascarado;
  sCodigo, sPrevio: string;
  bProcedimiento: Boolean;
begin
  if FNamesPendiente then
  begin
    FSalida.Append(SENTENCIA_NAMES).Append(FDelimitador).Append(FSaltoLinea);
    FNamesPendiente := False;
  end;
  oMascara := TTextoEnmascarado.Crear(AElemento.Texto);
  if TRegEx.IsMatch(oMascara.Codigo, PATRON_SET_SOLO_MARIADB,
    [roIgnoreCase]) then
    EmitirOmitida(AElemento)
  else
  begin
    bProcedimiento := TRegEx.IsMatch(oMascara.Codigo,
      '^\s*(?:DROP\s+PROCEDURE|CREATE\s+(?:OR\s+REPLACE\s+)?' +
      '(?:DEFINER\s*=\s*\S+\s+)?PROCEDURE)\b', [roIgnoreCase]);
    if bProcedimiento and FNecesitaSonda and not FSondaEmitida then
      EmitirSonda;
    sCodigo := ConvertirCodigo(oMascara, sPrevio);
    if sPrevio <> '' then
      FSalida.Append(sPrevio).Append(AElemento.Terminador)
        .Append(FSaltoLinea);
    FSalida.Append(oMascara.Restaurar(sCodigo)).Append(AElemento.Terminador);
    if not FIntercalacionEmitida
      and TRegEx.IsMatch(sCodigo, '^\s*SET\s+NAMES\b', [roIgnoreCase]) then
      EmitirIntercalacion(AElemento.Terminador);
  end;
end;

// La sentencia queda como comentario de bloque, sin terminador: no deja
// una sentencia vacía y no se come lo que venga detrás en la misma línea.
procedure TConversorMySQL841.EmitirOmitida(const AElemento: TElementoVolcado);
begin
  FSalida.Append('/* Omitido por la conversión (NOTE_VERBOSITY solo existe ')
    .Append('en MariaDB): ')
    .Append(TRegEx.Replace(AElemento.Texto, '\s+', ' ').Trim)
    .Append(' */');
  Inc(FInforme.SentenciasSesionOmitidas);
end;

procedure TConversorMySQL841.EmitirIntercalacion(const ATerminador: string);
begin
  FSalida.Append(FSaltoLinea).Append(FSaltoLinea)
    .Append('-- Intercalación de la base: los parámetros de los ')
    .Append('procedimientos la heredan al crearse')
    .Append(FSaltoLinea)
    .Append('-- (evita "Illegal mix of collations" en MySQL 8).')
    .Append(FSaltoLinea)
    .Append('ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci')
    .Append(ATerminador);
  FIntercalacionEmitida := True;
end;

procedure TConversorMySQL841.EmitirSonda;
var
  bCambiaDelimitador: Boolean;
  sTerminador: string;
begin
  bCambiaDelimitador := FDelimitador = ';';
  if bCambiaDelimitador then
    sTerminador := ';;'
  else
    sTerminador := FDelimitador;
  FSalida.Append('-- Procedimiento: ').Append(PROCEDIMIENTO_SONDA)
    .Append(' (añadido por la conversión: MySQL 8 no tiene ')
    .Append('@@in_transaction)').Append(FSaltoLinea)
    .Append('DROP PROCEDURE IF EXISTS `').Append(PROCEDIMIENTO_SONDA)
    .Append('`').Append(FDelimitador).Append(FSaltoLinea);
  if bCambiaDelimitador then
    FSalida.Append('DELIMITER ;;').Append(FSaltoLinea);
  FSalida.Append('CREATE PROCEDURE `').Append(PROCEDIMIENTO_SONDA)
    .Append('`(OUT p_EN_TRANSACCION int)').Append(FSaltoLinea)
    .Append('  SQL SECURITY INVOKER').Append(FSaltoLinea)
    .Append('  COMMENT ''Equivale a @@in_transaction de MariaDB: 1 si la ')
    .Append('sesión tiene una transacción abierta''').Append(FSaltoLinea)
    .Append('BEGIN').Append(FSaltoLinea)
    .Append('  DECLARE CONTINUE HANDLER FOR 1305 SET p_EN_TRANSACCION = 0;')
    .Append(FSaltoLinea)
    .Append('  SET p_EN_TRANSACCION = 1;').Append(FSaltoLinea)
    .Append('  SAVEPOINT fza_sonda_transaccion;').Append(FSaltoLinea)
    .Append('  RELEASE SAVEPOINT fza_sonda_transaccion;').Append(FSaltoLinea)
    .Append('END ').Append(sTerminador).Append(FSaltoLinea);
  if bCambiaDelimitador then
    FSalida.Append('DELIMITER ;').Append(FSaltoLinea);
  FSalida.Append(FSaltoLinea);
  FSondaEmitida := True;
end;

// ============================================================================
//   TConversorMySQL841: reglas por sentencia
// ============================================================================

// Nombre del objeto para los avisos: el procedimiento o el principio de
// la sentencia.
function DescribirObjeto(const ACodigo: string): string;
var
  oCabecera: TMatch;
begin
  oCabecera := TRegEx.Match(ACodigo, PATRON_CABECERA_PROCEDIMIENTO,
    [roIgnoreCase]);
  if oCabecera.Success then
    Result := 'Procedimiento ' + oCabecera.Groups[5].Value
  else
    Result := Copy(Trim(ACodigo), 1, 60);
end;

function TConversorMySQL841.ConvertirCodigo(var AMascara: TTextoEnmascarado;
  out APrevio: string): string;
var
  sCodigo, sObjeto: string;
  oInsert: TMatch;
begin
  APrevio := '';
  sCodigo := AMascara.Codigo;
  sObjeto := DescribirObjeto(sCodigo);
  oInsert := TRegEx.Match(sCodigo, PATRON_INSERT, [roIgnoreCase]);
  if oInsert.Success then
  begin
    if Pos(',' + LowerCase(oInsert.Groups[1].Value) + ',',
      ',' + TABLAS_CON_SQL_EN_DATOS + ',') > 0 then
      NormalizarLiteralesDatos(AMascara);
  end
  else
  begin
    sCodigo := RebajarUca1400(sObjeto, sCodigo);
    sCodigo := TRegEx.Replace(sCodigo, '^(\s*SET\s+NAMES\s+utf8mb4)(\s*)$',
      '$1 COLLATE ' + INTERCALACION_CONEXION + '$2', [roIgnoreCase]);
    sCodigo := ConvertirDefectoTemporal(AMascara, sCodigo);
    sCodigo := TRegEx.Replace(sCodigo, PATRON_SECUENCIA, EvaluarSecuencia,
      [roIgnoreCase]);
    if TRegEx.IsMatch(sCodigo, PATRON_NOMBRE_TABLA, [roIgnoreCase]) then
      sCodigo := ConvertirCreateTable(sCodigo)
    else if TRegEx.IsMatch(sCodigo, PATRON_VISTA, [roIgnoreCase]) then
      sCodigo := ConvertirVista(sCodigo)
    else if TRegEx.IsMatch(sCodigo, PATRON_CABECERA_PROCEDIMIENTO,
      [roIgnoreCase]) then
      sCodigo := ConvertirProcedimiento(sCodigo, APrevio);
    sCodigo := SustituirNombres(sCodigo, FInforme.NombresCorregidos);
    sCodigo := CopiarTemporalesReabiertas(sObjeto, sCodigo);
    sCodigo := ProtegerSiExiste(AMascara, sCodigo);
    DetectarNoConvertible(sObjeto, sCodigo);
  end;
  Result := sCodigo;
end;

// Quita el ancho de presentación de los enteros (int(11) -> int), que
// MySQL 8 ya no conserva, salvo tinyint(1) y las columnas zerofill.
function TConversorMySQL841.EvaluarSinAncho(const AMatch: TMatch): string;
begin
  if SameText(AMatch.Groups[1].Value, 'tinyint')
    and (AMatch.Groups[2].Value = '1') then
    Result := AMatch.Value
  else
    Result := AMatch.Groups[1].Value;
end;

function TConversorMySQL841.EvaluarDropMasCreate(
  const AMatch: TMatch): string;
begin
  Inc(FCuantosTemporal);
  Result := AMatch.Groups[1].Value + 'DROP ' + AMatch.Groups[2].Value
    + 'TABLE IF EXISTS ' + AMatch.Groups[3].Value + ';' + FSaltoCuerpo
    + AMatch.Groups[1].Value + 'CREATE ' + AMatch.Groups[2].Value
    + 'TABLE ' + AMatch.Groups[3].Value;
end;

function TConversorMySQL841.EvaluarNombreDeclarado(
  const AMatch: TMatch): string;
var
  sReal: string;
begin
  Result := AMatch.Value;
  if FNombres.TryGetValue(LowerCase(AMatch.Value), sReal)
    and (sReal <> AMatch.Value) then
  begin
    Inc(FCuantosNombres);
    Result := sReal;
  end;
end;

function TConversorMySQL841.ConvertirCreateTable(
  const ACodigo: string): string;
begin
  Result := TRegEx.Replace(ACodigo, '\bcurrent_timestamp\(\)',
    'CURRENT_TIMESTAMP', [roIgnoreCase]);
  Result := TRegEx.Replace(Result,
    '\b(tinyint|smallint|mediumint|int|integer|bigint)\((\d+)\)' +
    '(?![^,\r\n]*\bzerofill\b)', EvaluarSinAncho, [roIgnoreCase]);
  Result := TRegEx.Replace(Result,
    '\b((?:tiny|medium|long)?(?:text|blob)|json)\b([^,\r\n]*?\bDEFAULT\s+)' +
    '(''\x01\d+\x02'')', '$1$2($3)', [roIgnoreCase]);
  if Result <> ACodigo then
    Inc(FInforme.TablasAdaptadas);
end;

function TConversorMySQL841.ConvertirVista(const ACodigo: string): string;
begin
  Result := TRegEx.Replace(ACodigo, PATRON_VISTA,
    '$1$2SQL SECURITY INVOKER VIEW', [roIgnoreCase]);
  if Result <> ACodigo then
    Inc(FInforme.VistasInvoker);
end;

function CierreParentesis(const ATexto: string; AApertura: Integer): Integer;
var
  iNivel, i: Integer;
  bCerrado: Boolean;
begin
  iNivel := 0;
  i := AApertura;
  bCerrado := False;
  while (i <= Length(ATexto)) and not bCerrado do
  begin
    if ATexto[i] = '(' then
      Inc(iNivel)
    else if ATexto[i] = ')' then
    begin
      Dec(iNivel);
      bCerrado := iNivel = 0;
    end;
    if not bCerrado then
      Inc(i);
  end;
  Result := i;
end;

function TConversorMySQL841.ConvertirProcedimiento(const ACodigo: string;
  out ADrop: string): string;
var
  oCabecera: TMatch;
  sNombre, sSalto, sParametros, sResto: string;
  iApertura, iCierre: Integer;
begin
  ADrop := '';
  oCabecera := TRegEx.Match(ACodigo, PATRON_CABECERA_PROCEDIMIENTO,
    [roIgnoreCase]);
  sNombre := oCabecera.Groups[5].Value;
  sSalto := SaltoDeLineaDe(ACodigo);
  if oCabecera.Groups[2].Success and (oCabecera.Groups[2].Value <> '') then
  begin
    ADrop := 'DROP PROCEDURE IF EXISTS `' + sNombre + '`';
    Inc(FInforme.ProcedimientosSinOrReplace);
  end;
  iApertura := oCabecera.Index + oCabecera.Length - 1;
  iCierre := CierreParentesis(ACodigo, iApertura);
  sParametros := Copy(ACodigo, iApertura + 1, iCierre - iApertura - 1);
  sResto := Copy(ACodigo, iCierre + 1, MaxInt);
  sResto := AsegurarSecurityInvoker(sResto, sSalto);
  sResto := ConvertirCuerpoProcedimiento(sNombre, sResto);
  Result := CorregirCollate(oCabecera.Groups[1].Value + 'CREATE PROCEDURE `'
    + sNombre + '`(' + sParametros + ')' + sResto);
end;

function TConversorMySQL841.AsegurarSecurityInvoker(
  const AResto, ASalto: string): string;
var
  oInicio: TMatch;
  iCorte: Integer;
  sCaracteristicas, sCuerpo: string;
begin
  oInicio := TRegEx.Match(AResto, '(?:^|\s)(?:\w+\s*:\s*)?BEGIN\b',
    [roIgnoreCase]);
  if oInicio.Success then
    iCorte := oInicio.Index
  else
    iCorte := 1;
  sCaracteristicas := Copy(AResto, 1, iCorte - 1);
  sCuerpo := Copy(AResto, iCorte, MaxInt);
  if TRegEx.IsMatch(sCaracteristicas, '\bSQL\s+SECURITY\s+\w+',
    [roIgnoreCase]) then
    sCaracteristicas := TRegEx.Replace(sCaracteristicas,
      '\bSQL\s+SECURITY\s+\w+', 'SQL SECURITY INVOKER', [roIgnoreCase])
  else
    sCaracteristicas := ASalto + '  SQL SECURITY INVOKER' + sCaracteristicas;
  Inc(FInforme.ProcedimientosInvoker);
  Result := sCaracteristicas + sCuerpo;
end;

function TConversorMySQL841.ConvertirCuerpoProcedimiento(
  const ANombre, ACuerpo: string): string;
var
  oAdaptador: TAdaptadorEnTransaccion;
begin
  FSaltoCuerpo := SaltoDeLineaDe(ACuerpo);
  FCuantosTemporal := 0;
  Result := TRegEx.Replace(ACuerpo,
    '^([ \t]*)CREATE\s+OR\s+REPLACE\s+(TEMPORARY\s+)?TABLE\s+' +
    '(`?[A-Za-z0-9_]+`?)', EvaluarDropMasCreate,
    [roIgnoreCase, roMultiLine]);
  Inc(FInforme.TablasOrReplaceCorregidas, FCuantosTemporal);
  if TAdaptadorEnTransaccion.UsaEnTransaccion(Result) then
  begin
    oAdaptador := TAdaptadorEnTransaccion.Create(ANombre, Result,
      FInforme.Avisos);
    try
      Result := oAdaptador.Adaptar;
    finally
      FreeAndNil(oAdaptador);
    end;
    FInforme.ProcedimientosSonda.Add(ANombre);
  end;
end;

function TConversorMySQL841.CorregirCollate(const ATexto: string): string;
begin
  // La declaración puede venir partida en varias líneas (el tipo en una y
  // COLLATE en la siguiente), así que se busca sobre el texto completo. El
  // grupo 2 no cruza `;` ni paréntesis: no sale del DECLARE ni del parámetro.
  Result := TRegEx.Replace(ATexto,
    '\b(DECLARE|IN|OUT|INOUT)\b([^;()]*?)' +
    '\b((?:VAR)?CHAR|(?:TINY|MEDIUM|LONG)?TEXT)\b((?:\s*\(\s*\d+\s*\))?)' +
    '(\s+)COLLATE\s+(([A-Za-z0-9]+)_[A-Za-z0-9_]+)', EvaluarCollate,
    [roIgnoreCase]);
end;

// Inserta CHARACTER SET delante del COLLATE conservando el espacio o salto
// de línea original entre el tipo y COLLATE.
function TConversorMySQL841.EvaluarCollate(const AMatch: TMatch): string;
begin
  Inc(FInforme.DeclaracionesCollate);
  Result := AMatch.Groups[1].Value + AMatch.Groups[2].Value
    + AMatch.Groups[3].Value + AMatch.Groups[4].Value + ' CHARACTER SET '
    + AMatch.Groups[7].Value + AMatch.Groups[5].Value + 'COLLATE '
    + AMatch.Groups[6].Value;
end;

function TConversorMySQL841.SustituirNombres(const ATexto: string;
  var ACuantos: Integer): string;
begin
  FCuantosNombres := 0;
  Result := TRegEx.Replace(ATexto, PATRON_IDENTIFICADOR,
    EvaluarNombreDeclarado);
  Inc(ACuantos, FCuantosNombres);
end;

procedure TConversorMySQL841.NormalizarLiteralesDatos(
  var AMascara: TTextoEnmascarado);
var
  i, iCuantos: Integer;
  sLiteral, sNuevo: string;
begin
  for i := 0 to AMascara.NumeroFragmentos - 1 do
  begin
    sLiteral := AMascara.Fragmento(i);
    if ContainsText(sLiteral, 'fza_') or ContainsText(sLiteral, 'vi_') then
    begin
      iCuantos := 0;
      sNuevo := SustituirNombres(sLiteral, iCuantos);
      if iCuantos > 0 then
      begin
        Inc(FInforme.LiteralesDatosCorregidos);
        AMascara.SustituirFragmento(i, sNuevo);
      end;
    end;
  end;
end;

procedure TConversorMySQL841.DetectarNoConvertible(
  const AObjeto, ACodigo: string);
var
  oConstruccion: TConstruccionNoConvertible;
begin
  for oConstruccion in CONSTRUCCIONES_NO_CONVERTIBLES do
  begin
    if TRegEx.IsMatch(ACodigo, oConstruccion.Patron,
      [roIgnoreCase, roMultiLine]) then
      FInforme.Avisos.Add(AObjeto + ': ' + oConstruccion.Descripcion);
  end;
end;

// La intercalación de sesión que escribe mysqldump 11+ antes de vistas y
// rutinas (SET collation_connection = utf8mb4_uca1400_ai_ci) y la de
// columnas o COLLATE: MySQL 8 no tiene uca1400. Se rebaja como para
// MariaDB 10; otras variantes de uca1400 se avisan.
function TConversorMySQL841.RebajarUca1400(
  const AObjeto, ACodigo: string): string;
begin
  FCuantosIntercalacion := 0;
  Result := TRegEx.Replace(ACodigo, '\b(utf8mb4|utf8mb3|utf8)_uca1400_ai_ci\b',
    EvaluarUca1400, [roIgnoreCase]);
  Inc(FInforme.IntercalacionesRebajadas, FCuantosIntercalacion);
  if ContainsText(Result, '_uca1400_') then
    FInforme.Avisos.Add(AObjeto + ': intercalación uca1400 sin equivalente ' +
      'en MySQL 8 (solo se rebaja *_uca1400_ai_ci)');
end;

// seq_A_to_B como tabla derivada: A más un número de tantas cifras como
// tenga B - A, sacado del producto de tablas de dígitos, hasta B.
function TConversorMySQL841.EvaluarSecuencia(const AMatch: TMatch): string;
var
  iDesde, iHasta, iPeso: Int64;
  iCifras, i: Integer;
  sDigitos, sSuma, sTablas: string;
begin
  iDesde := StrToInt64(AMatch.Groups[3].Value);
  iHasta := StrToInt64(AMatch.Groups[4].Value);
  if iHasta < iDesde then
    Result := AMatch.Value
  else
  begin
    sDigitos := '(SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ' +
      'UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 ' +
      'UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 ' +
      'UNION ALL SELECT 9)';
    iCifras := Length(IntToStr(iHasta - iDesde));
    sSuma := IntToStr(iDesde);
    sTablas := '';
    iPeso := 1;
    for i := 0 to iCifras - 1 do
    begin
      sSuma := sSuma + ' + d' + IntToStr(i) + '.n * ' + IntToStr(iPeso);
      if sTablas <> '' then
        sTablas := sTablas + ' CROSS JOIN ';
      sTablas := sTablas + sDigitos + ' d' + IntToStr(i);
      iPeso := iPeso * 10;
    end;
    Result := AMatch.Groups[1].Value + AMatch.Groups[2].Value
      + '(SELECT fza_seq.seq FROM (SELECT ' + sSuma + ' AS seq FROM '
      + sTablas + ') fza_seq WHERE fza_seq.seq <= ' + IntToStr(iHasta) + ')';
    if (AMatch.Groups.Count > 5) and AMatch.Groups[5].Success
      and (AMatch.Groups[5].Value <> '') then
      Result := Result + AMatch.Groups[5].Value
    else
      Result := Result + ' AS seq_' + AMatch.Groups[3].Value + '_to_'
        + AMatch.Groups[4].Value;
    Inc(FInforme.SecuenciasConvertidas);
  end;
end;

function TConversorMySQL841.EvaluarUca1400(const AMatch: TMatch): string;
begin
  Inc(FCuantosIntercalacion);
  if SameText(AMatch.Groups[1].Value, 'utf8mb4') then
    Result := 'utf8mb4_spanish_ci'
  else
    Result := 'utf8mb3_spanish_ci';
end;

function TieneComaNivelCero(const ATexto: string): Boolean;
var
  iNivel, i: Integer;
begin
  Result := False;
  iNivel := 0;
  i := 1;
  while not Result and (i <= Length(ATexto)) do
  begin
    case ATexto[i] of
      '(': Inc(iNivel);
      ')': Dec(iNivel);
      ',': Result := iNivel = 0;
    end;
    Inc(i);
  end;
end;

function SangriaInicial(const ATexto: string): string;
var
  i: Integer;
begin
  i := 1;
  while (i <= Length(ATexto)) and CharInSet(ATexto[i], [' ', #9, #13, #10]) do
    Inc(i);
  Result := Copy(ATexto, 1, i - 1);
end;

// ALTER TABLE t MODIFY COLUMN c con el tipo, la nulidad, ON UPDATE y el
// comentario que tiene la columna en el destino, y DEFAULT
// CURRENT_TIMESTAMP con su precisión. Es una expresión: devuelve el SQL.
function ExpresionModificarDefectoTemporal(const ATabla,
  AColumna: string): string;
begin
  Result := '(SELECT CONCAT(''ALTER TABLE `' + ATabla + '` MODIFY COLUMN `'
    + AColumna + '` '', COLUMN_TYPE, '
    + 'IF(IS_NULLABLE = ''NO'', '' NOT NULL'', '' NULL''), '
    + ''' DEFAULT CURRENT_TIMESTAMP'', '
    + 'IF(IFNULL(DATETIME_PRECISION, 0) > 0, '
    + 'CONCAT(''('', DATETIME_PRECISION, '')''), ''''), '
    + 'IF(LOCATE(''on update'', EXTRA) > 0, '
    + 'CONCAT('' '', SUBSTRING(EXTRA, LOCATE(''on update'', EXTRA))), ''''), '
    + 'IF(COLUMN_COMMENT <> '''', '
    + 'CONCAT('' COMMENT '', QUOTE(COLUMN_COMMENT)), '''')) '
    + 'FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() '
    + 'AND TABLE_NAME = ''' + ATabla + ''' AND COLUMN_NAME = '''
    + AColumna + ''')';
end;

// Suelto se ejecuta con PREPARE; dentro de un literal (SQL dinámico de
// una guarda) el literal pasa a ser la expresión, salvo tras PREPARE FROM,
// que solo admite un literal o una variable.
function TConversorMySQL841.ConvertirDefectoTemporal(
  const AMascara: TTextoEnmascarado; const ACodigo: string): string;
var
  oSuelta, oDinamica: TMatch;
  oLiterales: TMatchCollection;
  i, iFragmento: Integer;
  sExpresion: string;
begin
  Result := ACodigo;
  oSuelta := TRegEx.Match(ACodigo, PATRON_DEFECTO_TEMPORAL, [roIgnoreCase]);
  if oSuelta.Success then
  begin
    Result := SangriaInicial(ACodigo) + 'SET @fza_sql := '
      + ExpresionModificarDefectoTemporal(oSuelta.Groups[1].Value,
        oSuelta.Groups[2].Value) + FDelimitador + FSaltoLinea
      + 'PREPARE fza_stmt FROM @fza_sql' + FDelimitador + FSaltoLinea
      + 'EXECUTE fza_stmt' + FDelimitador + FSaltoLinea
      + 'DEALLOCATE PREPARE fza_stmt';
    Inc(FInforme.DefectosTemporalesConvertidos);
  end
  else
  begin
    oLiterales := TRegEx.Matches(ACodigo, '''\x01(\d+)\x02''');
    for i := oLiterales.Count - 1 downto 0 do
    begin
      iFragmento := StrToInt(oLiterales[i].Groups[1].Value);
      oDinamica := TRegEx.Match(AMascara.Fragmento(iFragmento),
        PATRON_DEFECTO_TEMPORAL, [roIgnoreCase]);
      if oDinamica.Success
        and not TRegEx.IsMatch(Copy(Result, 1, oLiterales[i].Index - 1),
          '\bPREPARE\s+\w+\s+FROM\s*$', [roIgnoreCase]) then
      begin
        sExpresion := ExpresionModificarDefectoTemporal(
          oDinamica.Groups[1].Value, oDinamica.Groups[2].Value);
        Result := Copy(Result, 1, oLiterales[i].Index - 1) + sExpresion
          + Copy(Result, oLiterales[i].Index + oLiterales[i].Length, MaxInt);
        Inc(FInforme.DefectosTemporalesConvertidos);
      end;
    end;
  end;
end;

// MySQL 8 no tiene CREATE INDEX IF NOT EXISTS, DROP INDEX IF EXISTS ni los
// IF [NOT] EXISTS de ADD/DROP COLUMN e INDEX en ALTER TABLE. Una sentencia
// suelta (no dentro de una rutina) con una sola de esas cláusulas pasa a
// la guarda de Core.Dialecto: consulta INFORMATION_SCHEMA y ejecuta con
// PREPARE. Con varias cláusulas se deja como está y se avisa.
function TConversorMySQL841.ProtegerSiExiste(
  const AMascara: TTextoEnmascarado; const ACodigo: string): string;
var
  oCoincidencia, oAlter: TMatch;
  sTabla, sClausula, sCuenta, sComando, sGuarda: string;
  iInicio, iLongitud, iDesplazamiento: Integer;
  bSiExiste: Boolean;
begin
  Result := ACodigo;
  sCuenta := '';
  bSiExiste := False;
  oCoincidencia := TRegEx.Match(ACodigo, PATRON_CREATE_INDEX_SI_NO_EXISTE,
    [roIgnoreCase]);
  if oCoincidencia.Success then
    sCuenta := CuentaIndiceCatalogo(oCoincidencia.Groups[3].Value,
      oCoincidencia.Groups[2].Value)
  else
  begin
    oCoincidencia := TRegEx.Match(ACodigo, PATRON_DROP_INDEX_SI_EXISTE,
      [roIgnoreCase]);
    if oCoincidencia.Success then
    begin
      sCuenta := CuentaIndiceCatalogo(oCoincidencia.Groups[3].Value,
        oCoincidencia.Groups[2].Value);
      bSiExiste := True;
    end;
  end;
  iDesplazamiento := 0;
  if sCuenta = '' then
  begin
    oAlter := TRegEx.Match(ACodigo, PATRON_ALTER_TABLE, [roIgnoreCase]);
    if oAlter.Success then
    begin
      sTabla := oAlter.Groups[1].Value;
      iDesplazamiento := oAlter.Index + oAlter.Length - 1;
      sClausula := Copy(ACodigo, iDesplazamiento + 1, MaxInt);
      if not TieneComaNivelCero(sClausula) then
      begin
        oCoincidencia := TRegEx.Match(sClausula,
          '^' + PATRON_ADD_INDEX_SI_NO_EXISTE, [roIgnoreCase]);
        if oCoincidencia.Success then
          sCuenta := CuentaIndiceCatalogo(sTabla,
            oCoincidencia.Groups[2].Value)
        else
        begin
          oCoincidencia := TRegEx.Match(sClausula,
            '^' + PATRON_ADD_COLUMNA_SI_NO_EXISTE, [roIgnoreCase]);
          if oCoincidencia.Success then
            sCuenta := CuentaColumnaCatalogo(sTabla,
              oCoincidencia.Groups[2].Value)
          else
          begin
            bSiExiste := True;
            oCoincidencia := TRegEx.Match(sClausula,
              '^' + PATRON_DROP_INDEX_ALTER_SI_EXISTE, [roIgnoreCase]);
            if oCoincidencia.Success then
              sCuenta := CuentaIndiceCatalogo(sTabla,
                oCoincidencia.Groups[2].Value)
            else
            begin
              oCoincidencia := TRegEx.Match(sClausula,
                '^' + PATRON_DROP_COLUMNA_SI_EXISTE, [roIgnoreCase]);
              if oCoincidencia.Success then
                sCuenta := CuentaColumnaCatalogo(sTabla,
                  oCoincidencia.Groups[2].Value);
            end;
          end;
        end;
      end;
    end;
  end;
  if sCuenta <> '' then
  begin
    iInicio := iDesplazamiento + oCoincidencia.Groups[1].Index;
    iLongitud := oCoincidencia.Groups[1].Length;
    sComando := Trim(AMascara.Restaurar(Copy(ACodigo, 1, iInicio - 1) +
      Copy(ACodigo, iInicio + iLongitud, MaxInt)));
    sGuarda := GuardarComandoSegunCatalogo(sCuenta, sComando, bSiExiste,
      FDelimitador, FSaltoLinea);
    // El último terminador lo pone quien emite la sentencia.
    Result := SangriaInicial(ACodigo) +
      Copy(sGuarda, 1, Length(sGuarda) - Length(FDelimitador));
    Inc(FInforme.GuardasCatalogo);
  end;
end;

type
  TSeparadorCuerpo = record
    Indice: Integer;
    Longitud: Integer;
    Texto: string;
  end;

// Fronteras de sentencia de un cuerpo: los ; y las palabras tras las que
// empieza otra sentencia (THEN, ELSE, DO, LOOP, REPEAT, BEGIN), salvo
// dentro de paréntesis o de una expresión CASE ... END.
function SeparadoresCuerpo(const ACodigo: string): TArray<TSeparadorCuerpo>;
var
  oLista: TList<TSeparadorCuerpo>;
  // True: CASE de expresión; False: sentencia CASE ... END CASE.
  oCases: TStack<Boolean>;
  i, iFin, iSiguiente, iNivel: Integer;
  sPalabra: string;
  bInicio: Boolean;

  function LeerPalabra(ADesde: Integer; out AFin: Integer): string;
  begin
    AFin := ADesde;
    while (AFin <= Length(ACodigo))
      and CharInSet(ACodigo[AFin], ['A'..'Z', 'a'..'z', '0'..'9', '_', '$']) do
      Inc(AFin);
    Result := UpperCase(Copy(ACodigo, ADesde, AFin - ADesde));
  end;

  procedure Anadir(AIndice, ALongitud: Integer);
  var
    oSeparador: TSeparadorCuerpo;
  begin
    oSeparador.Indice := AIndice;
    oSeparador.Longitud := ALongitud;
    oSeparador.Texto := Copy(ACodigo, AIndice, ALongitud);
    oLista.Add(oSeparador);
    bInicio := True;
  end;

begin
  oLista := TList<TSeparadorCuerpo>.Create;
  oCases := TStack<Boolean>.Create;
  try
    iNivel := 0;
    bInicio := True;
    i := 1;
    while i <= Length(ACodigo) do
    begin
      if CharInSet(ACodigo[i], ['A'..'Z', 'a'..'z', '_']) then
      begin
        sPalabra := LeerPalabra(i, iFin);
        if sPalabra = 'CASE' then
          oCases.Push(not bInicio)
        else if (sPalabra = 'END') and (oCases.Count > 0) then
        begin
          if oCases.Peek then
            oCases.Pop
          else
          begin
            iSiguiente := iFin;
            while (iSiguiente <= Length(ACodigo))
              and CharInSet(ACodigo[iSiguiente], [' ', #9, #13, #10]) do
              Inc(iSiguiente);
            if LeerPalabra(iSiguiente, iSiguiente) = 'CASE' then
            begin
              oCases.Pop;
              iFin := iSiguiente;
            end;
          end;
        end;
        if MatchText(sPalabra, ['THEN', 'ELSE', 'DO', 'LOOP', 'REPEAT',
          'BEGIN'])
          and (iNivel = 0)
          and ((oCases.Count = 0) or not oCases.Peek) then
          Anadir(i, iFin - i)
        else
          bInicio := False;
        i := iFin;
      end
      else
      begin
        case ACodigo[i] of
          '(': Inc(iNivel);
          ')': Dec(iNivel);
          ';': Anadir(i, 1);
        end;
        Inc(i);
      end;
    end;
    Result := oLista.ToArray;
  finally
    FreeAndNil(oCases);
    FreeAndNil(oLista);
  end;
end;

// MySQL no deja usar una tabla temporal más de una vez en la misma
// sentencia (ER_CANT_REOPEN_TABLE); MariaDB sí. Justo antes de la sentencia
// se copia la tabla a <tabla>_rN (una copia por cada uso de más) y esos usos
// pasan a la copia; el uso que escribe (INTO, UPDATE) se queda con la
// original. Se recorre de atrás adelante para no mover las posiciones.
function TConversorMySQL841.CopiarTemporalesReabiertas(const AObjeto,
  ACodigo: string): string;
var
  aSeparadores: TArray<TSeparadorCuerpo>;
  i, iInicio, iFin: Integer;
  sSeparador: string;
begin
  Result := ACodigo;
  if FTemporales.Count > 0 then
  begin
    aSeparadores := SeparadoresCuerpo(ACodigo);
    for i := Length(aSeparadores) downto 0 do
    begin
      if i = 0 then
      begin
        iInicio := 1;
        sSeparador := '';
      end
      else
      begin
        iInicio := aSeparadores[i - 1].Indice + aSeparadores[i - 1].Longitud;
        sSeparador := aSeparadores[i - 1].Texto;
      end;
      if i = Length(aSeparadores) then
        iFin := Length(ACodigo) + 1
      else
        iFin := aSeparadores[i].Indice;
      CopiarEnFragmento(Result, iInicio, iFin, sSeparador, AObjeto);
    end;
  end;
end;

procedure TConversorMySQL841.CopiarEnFragmento(var ATexto: string;
  AInicio, AFin: Integer; const ASeparador, AObjeto: string);
var
  oUsos: TMatchCollection;
  oUso: TMatch;
  oVeces, oConservado, oCopias: TDictionary<string, Integer>;
  oRepetidas: TStringList;
  sFragmento, sClave, sPalabra, sSangria, sSalto, sInsercion, sCopia,
    sAviso: string;
  iPalabra, iLinea, iCopia, i: Integer;
  bConvertible: Boolean;
begin
  sFragmento := Copy(ATexto, AInicio, AFin - AInicio);
  oUsos := TRegEx.Matches(sFragmento, PATRON_USO_TABLA, [roIgnoreCase]);
  oVeces := TDictionary<string, Integer>.Create;
  oConservado := TDictionary<string, Integer>.Create;
  oCopias := TDictionary<string, Integer>.Create;
  oRepetidas := TStringList.Create;
  try
    for oUso in oUsos do
    begin
      sClave := LowerCase(oUso.Groups[2].Value);
      if FTemporales.ContainsKey(sClave) then
      begin
        if not oVeces.TryGetValue(sClave, i) then
          i := 0;
        oVeces.AddOrSetValue(sClave, i + 1);
        if i = 1 then
          oRepetidas.Add(sClave);
        // Se conserva el primer uso, salvo que otro posterior escriba.
        if not oConservado.ContainsKey(sClave) then
          oConservado.Add(sClave, oUso.Groups[2].Index)
        else if MatchText(oUso.Groups[1].Value, ['INTO', 'UPDATE']) then
          oConservado[sClave] := oUso.Groups[2].Index;
      end;
    end;
    if oRepetidas.Count > 0 then
    begin
      // Donde insertar: la primera palabra, que tiene que empezar una
      // sentencia de verdad (tras ; o, tras THEN/ELSE..., en línea nueva).
      // Se saltan blancos y comentarios (marcas #1n#2 del enmascarado).
      iPalabra := 1;
      while (iPalabra <= Length(sFragmento))
        and CharInSet(sFragmento[iPalabra], [' ', #9, #13, #10, #1]) do
      begin
        if sFragmento[iPalabra] = #1 then
          while (iPalabra <= Length(sFragmento))
            and (sFragmento[iPalabra] <> #2) do
            Inc(iPalabra);
        Inc(iPalabra);
      end;
      i := iPalabra;
      while (i <= Length(sFragmento))
        and CharInSet(sFragmento[i], ['A'..'Z', 'a'..'z']) do
        Inc(i);
      sPalabra := UpperCase(Copy(sFragmento, iPalabra, i - iPalabra));
      bConvertible := (ASeparador <> '')
        and ((ASeparador = ';') or (Pos(#10, Copy(sFragmento, 1,
          iPalabra - 1)) > 0))
        and MatchText(sPalabra, ['INSERT', 'UPDATE', 'DELETE', 'SELECT',
          'SET', 'IF', 'REPLACE', 'CREATE', 'CALL']);
      for sClave in oRepetidas do
      begin
        // Un calificador tabla.columna seguiría apuntando a la original.
        if TRegEx.IsMatch(sFragmento, '(?<![A-Za-z0-9_.])`?' + sClave +
          '`?\s*\.', [roIgnoreCase])
          or (Length(sClave) + 3 > 64) then
          bConvertible := False;
      end;
      if not bConvertible then
      begin
        for sClave in oRepetidas do
        begin
          sAviso := Format('%s: la tabla temporal %s se usa dos veces en ' +
            'una misma sentencia y no se puede copiar antes; MySQL no ' +
            'puede reabrirla ("Can''t reopen table")',
            [AObjeto, FTemporales[sClave]]);
          if FInforme.Avisos.IndexOf(sAviso) < 0 then
            FInforme.Avisos.Add(sAviso);
        end;
      end
      else
      begin
        iLinea := iPalabra;
        while (iLinea > 1) and CharInSet(sFragmento[iLinea - 1], [' ', #9]) do
          Dec(iLinea);
        sSangria := Copy(sFragmento, iLinea, iPalabra - iLinea);
        sSalto := SaltoDeLineaDe(ATexto);
        sInsercion := '';
        // De atrás adelante: los usos repetidos pasan a su copia.
        for i := oUsos.Count - 1 downto 0 do
        begin
          sClave := LowerCase(oUsos[i].Groups[2].Value);
          if (oRepetidas.IndexOf(sClave) >= 0)
            and (oConservado[sClave] <> oUsos[i].Groups[2].Index) then
          begin
            if not oCopias.TryGetValue(sClave, iCopia) then
              iCopia := 0;
            Inc(iCopia);
            oCopias.AddOrSetValue(sClave, iCopia);
            sCopia := FTemporales[sClave] + '_r' + IntToStr(iCopia);
            sFragmento := Copy(sFragmento, 1, oUsos[i].Groups[2].Index - 1)
              + sCopia + Copy(sFragmento, oUsos[i].Groups[2].Index
              + oUsos[i].Groups[2].Length, MaxInt);
            sInsercion := sInsercion
              + 'DROP TEMPORARY TABLE IF EXISTS ' + sCopia + ';' + sSalto
              + sSangria + 'CREATE TEMPORARY TABLE ' + sCopia + ' LIKE '
              + FTemporales[sClave] + ';' + sSalto
              + sSangria + 'INSERT INTO ' + sCopia + ' SELECT * FROM '
              + FTemporales[sClave] + ';' + sSalto + sSangria;
          end;
        end;
        sFragmento := Copy(sFragmento, 1, iPalabra - 1) + sInsercion
          + Copy(sFragmento, iPalabra, MaxInt);
        ATexto := Copy(ATexto, 1, AInicio - 1) + sFragmento
          + Copy(ATexto, AFin, MaxInt);
        Inc(FInforme.SentenciasTemporalCopiada);
      end;
    end;
  finally
    FreeAndNil(oRepetidas);
    FreeAndNil(oCopias);
    FreeAndNil(oConservado);
    FreeAndNil(oVeces);
  end;
end;

// ============================================================================
//   TAdaptadorEnTransaccion
// ============================================================================

constructor TAdaptadorEnTransaccion.Create(const ANombre, ACuerpo: string;
  AAvisos: TStrings);
begin
  inherited Create;
  FNombre := ANombre;
  FLineas := PartirLineas(ACuerpo);
  FAvisos := AAvisos;
  FInsertarAntes := TList<Integer>.Create;
  FIndiceBegin := -1;
end;

destructor TAdaptadorEnTransaccion.Destroy;
begin
  FreeAndNil(FInsertarAntes);
  inherited;
end;

class function TAdaptadorEnTransaccion.UsaEnTransaccion(
  const ATexto: string): Boolean;
begin
  Result := ContainsText(ATexto, '@@in_transaction');
end;

function TAdaptadorEnTransaccion.Adaptar: string;
var
  i: Integer;
begin
  FIndiceBegin := BuscarBegin;
  if FIndiceBegin < 0 then
  begin
    FAvisos.Add(FNombre + ': usa @@in_transaction pero no se localiza el ' +
      'BEGIN del cuerpo; no convertido');
    Result := UnirLineas(FLineas);
  end
  else
  begin
    for i := 0 to High(FLineas) do
    begin
      if UsaEnTransaccion(FLineas[i].Texto) then
        Clasificar(i);
    end;
    Result := Reconstruir;
  end;
end;

function TAdaptadorEnTransaccion.BuscarBegin: Integer;
var
  i: Integer;
begin
  Result := -1;
  i := 0;
  while (Result < 0) and (i <= High(FLineas)) do
  begin
    if TRegEx.IsMatch(FLineas[i].Texto, PATRON_BEGIN, [roIgnoreCase]) then
      Result := i;
    Inc(i);
  end;
end;

function TAdaptadorEnTransaccion.PrimeraPalabra(AIndice: Integer): string;
var
  sLinea: string;
  i: Integer;
begin
  sLinea := Trim(FLineas[AIndice].Texto);
  i := 1;
  while (i <= Length(sLinea))
    and CharInSet(sLinea[i], ['A'..'Z', 'a'..'z', '_', '@']) do
    Inc(i);
  if (i = 1) and (sLinea <> '') then
    Result := sLinea[1]
  else
    Result := UpperCase(Copy(sLinea, 1, i - 1));
end;

function TAdaptadorEnTransaccion.Sangria(AIndice: Integer): string;
var
  sLinea: string;
  i: Integer;
begin
  sLinea := FLineas[AIndice].Texto;
  i := 1;
  while (i <= Length(sLinea)) and CharInSet(sLinea[i], [' ', #9]) do
    Inc(i);
  Result := Copy(sLinea, 1, i - 1);
end;

function TAdaptadorEnTransaccion.SaltoLinea(AIndice: Integer): string;
var
  i: Integer;
begin
  Result := FLineas[AIndice].Salto;
  i := 0;
  while (Result = '') and (i <= High(FLineas)) do
  begin
    Result := FLineas[i].Salto;
    Inc(i);
  end;
  if Result = '' then
    Result := #13#10;
end;

function TAdaptadorEnTransaccion.SangriaDeclaracion: string;
var
  i: Integer;
begin
  Result := '';
  i := FIndiceBegin + 1;
  while (Result = '') and (i <= High(FLineas)) do
  begin
    if Trim(FLineas[i].Texto) <> '' then
      Result := Sangria(i);
    Inc(i);
  end;
  if Result = '' then
    Result := Sangria(FIndiceBegin) + '  ';
end;

function TAdaptadorEnTransaccion.BuscarIfGobernante(
  AIndice: Integer): Integer;
var
  sSangria: string;
  j: Integer;
begin
  Result := -1;
  sSangria := Sangria(AIndice);
  j := AIndice - 1;
  while (Result < 0) and (j > FIndiceBegin) do
  begin
    if (Sangria(j) = sSangria) and (PrimeraPalabra(j) = 'IF') then
      Result := j;
    Dec(j);
  end;
end;

function TAdaptadorEnTransaccion.BuscarInicioCondicion(
  AIndice: Integer): Integer;
var
  j: Integer;
  sPalabra: string;
  bSeguir: Boolean;
begin
  Result := -1;
  j := AIndice - 1;
  bSeguir := True;
  while bSeguir and (j > FIndiceBegin) and (AIndice - j <= 8) do
  begin
    sPalabra := PrimeraPalabra(j);
    if sPalabra = 'IF' then
    begin
      Result := j;
      bSeguir := False;
    end
    else if sPalabra = 'ELSEIF' then
    begin
      Result := BuscarIfGobernante(j);
      bSeguir := False;
    end
    else if MatchText(sPalabra, ['AND', 'OR', 'NOT', '(']) then
      Dec(j)
    else
      bSeguir := False;
  end;
end;

procedure TAdaptadorEnTransaccion.Marcar(AIndice: Integer);
begin
  if not FInsertarAntes.Contains(AIndice) then
    FInsertarAntes.Add(AIndice);
end;

procedure TAdaptadorEnTransaccion.Avisar(AIndice: Integer;
  const AMotivo: string);
begin
  FAvisos.Add(Format('%s (línea %d del cuerpo): %s',
    [FNombre, AIndice + 1, AMotivo]));
end;

procedure TAdaptadorEnTransaccion.Clasificar(AIndice: Integer);
var
  sPalabra: string;
  iGobernante: Integer;
begin
  sPalabra := PrimeraPalabra(AIndice);
  if MatchText(sPalabra, ['IF', 'SET']) then
    Marcar(AIndice)
  else if sPalabra = 'ELSEIF' then
  begin
    iGobernante := BuscarIfGobernante(AIndice);
    if iGobernante >= 0 then
      Marcar(iGobernante)
    else
      Avisar(AIndice, 'ELSEIF con @@in_transaction sin IF localizable');
  end
  else if MatchText(sPalabra, ['AND', 'OR', 'NOT', '(']) then
  begin
    iGobernante := BuscarInicioCondicion(AIndice);
    if iGobernante >= 0 then
      Marcar(iGobernante)
    else
      Avisar(AIndice, 'condición con @@in_transaction sin IF localizable');
  end
  else
    Avisar(AIndice, 'uso de @@in_transaction en "' + sPalabra +
      '" sin conversión automática');
end;

function TAdaptadorEnTransaccion.Reconstruir: string;
var
  oSalida: TStringBuilder;
  i: Integer;
begin
  oSalida := TStringBuilder.Create;
  try
    for i := 0 to High(FLineas) do
    begin
      if FInsertarAntes.Contains(i) then
        oSalida.Append(Sangria(i)).Append('CALL ').Append(PROCEDIMIENTO_SONDA)
          .Append('(').Append(VARIABLE_SONDA).Append(');')
          .Append(SaltoLinea(i));
      oSalida.Append(TRegEx.Replace(FLineas[i].Texto, '@@in_transaction',
        VARIABLE_SONDA, [roIgnoreCase])).Append(FLineas[i].Salto);
      if i = FIndiceBegin then
        oSalida.Append(SangriaDeclaracion).Append('DECLARE ')
          .Append(VARIABLE_SONDA).Append(' INT DEFAULT 0;')
          .Append(SaltoLinea(i));
    end;
    Result := oSalida.ToString;
  finally
    FreeAndNil(oSalida);
  end;
end;

end.
