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
{******************************************************************************}
unit Conversion.MySQL841;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections,
  System.RegularExpressions, Conversion.Sentencias;

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
    constructor Create;
    destructor Destroy; override;
    function Resumen: string;
    property Avisos: TStringList read FAvisos;
    property ProcedimientosSonda: TStringList read FProcedimientosSonda;
  end;

  TConversorMySQL841 = class
  private
    FNombres: TDictionary<string, string>;
    FInforme: TInformeConversionMySQL841;
    FSalida: TStringBuilder;
    FSaltoLinea: string;
    FDelimitador: string;
    FNecesitaSonda: Boolean;
    FSondaEmitida: Boolean;
    FNotaEmitida: Boolean;
    FIntercalacionEmitida: Boolean;
    // Estado de los evaluadores de TRegEx.Replace (punteros a método).
    FSaltoCuerpo: string;
    FCuantosTemporal: Integer;
    FCuantosNombres: Integer;
    function EvaluarSinAncho(const AMatch: TMatch): string;
    function EvaluarDropMasCreate(const AMatch: TMatch): string;
    function EvaluarNombreDeclarado(const AMatch: TMatch): string;
    procedure Analizar(const AElementos: TArray<TElementoVolcado>);
    procedure RegistrarNombres(const ACodigo: string);
    procedure Emitir(const AElemento: TElementoVolcado);
    procedure EmitirComentario(const AElemento: TElementoVolcado);
    procedure EmitirSentencia(const AElemento: TElementoVolcado);
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
  FInforme := TInformeConversionMySQL841.Create;
  FSalida := TStringBuilder.Create;
  FSaltoLinea := #13#10;
  FDelimitador := ';';
end;

destructor TConversorMySQL841.Destroy;
begin
  FreeAndNil(FSalida);
  FreeAndNil(FInforme);
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
  FNecesitaSonda := False;
  FNombres.Clear;
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
begin
  for oElemento in AElementos do
  begin
    if oElemento.Tipo = tevSentencia then
    begin
      oMascara := TTextoEnmascarado.Crear(oElemento.Texto);
      RegistrarNombres(oMascara.Codigo);
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
  oMascara := TTextoEnmascarado.Crear(AElemento.Texto);
  bProcedimiento := TRegEx.IsMatch(oMascara.Codigo,
    '^\s*(?:DROP\s+PROCEDURE|CREATE\s+(?:OR\s+REPLACE\s+)?' +
    '(?:DEFINER\s*=\s*\S+\s+)?PROCEDURE)\b', [roIgnoreCase]);
  if bProcedimiento and FNecesitaSonda and not FSondaEmitida then
    EmitirSonda;
  sCodigo := ConvertirCodigo(oMascara, sPrevio);
  if sPrevio <> '' then
    FSalida.Append(sPrevio).Append(AElemento.Terminador).Append(FSaltoLinea);
  FSalida.Append(oMascara.Restaurar(sCodigo)).Append(AElemento.Terminador);
  if not FIntercalacionEmitida
    and TRegEx.IsMatch(sCodigo, '^\s*SET\s+NAMES\b', [roIgnoreCase]) then
    EmitirIntercalacion(AElemento.Terminador);
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

function TConversorMySQL841.ConvertirCodigo(var AMascara: TTextoEnmascarado;
  out APrevio: string): string;
var
  sCodigo, sObjeto: string;
  oInsert: TMatch;
begin
  APrevio := '';
  sCodigo := AMascara.Codigo;
  sObjeto := Copy(Trim(sCodigo), 1, 60);
  oInsert := TRegEx.Match(sCodigo, PATRON_INSERT, [roIgnoreCase]);
  if oInsert.Success then
  begin
    if Pos(',' + LowerCase(oInsert.Groups[1].Value) + ',',
      ',' + TABLAS_CON_SQL_EN_DATOS + ',') > 0 then
      NormalizarLiteralesDatos(AMascara);
  end
  else
  begin
    if TRegEx.IsMatch(sCodigo, PATRON_NOMBRE_TABLA, [roIgnoreCase]) then
      sCodigo := ConvertirCreateTable(sCodigo)
    else if TRegEx.IsMatch(sCodigo, PATRON_VISTA, [roIgnoreCase]) then
      sCodigo := ConvertirVista(sCodigo)
    else if TRegEx.IsMatch(sCodigo, PATRON_CABECERA_PROCEDIMIENTO,
      [roIgnoreCase]) then
      sCodigo := ConvertirProcedimiento(sCodigo, APrevio);
    sCodigo := SustituirNombres(sCodigo, FInforme.NombresCorregidos);
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
  oCabecera: TMatch;
  sObjeto: string;
begin
  sObjeto := AObjeto;
  oCabecera := TRegEx.Match(ACodigo, PATRON_CABECERA_PROCEDIMIENTO,
    [roIgnoreCase]);
  if oCabecera.Success then
    sObjeto := 'Procedimiento ' + oCabecera.Groups[5].Value;
  for oConstruccion in CONSTRUCCIONES_NO_CONVERTIBLES do
  begin
    if TRegEx.IsMatch(ACodigo, oConstruccion.Patron,
      [roIgnoreCase, roMultiLine]) then
      FInforme.Avisos.Add(sObjeto + ': ' + oConstruccion.Descripcion);
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
