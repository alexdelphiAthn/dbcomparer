{******************************************************************************}
{                                                                              }
{  Módulo:       Conversion.Sentencias                                         }
{    Tipo:       Librería                                                      }
{ Versión:       1.0.0                                                         }
{   Fecha:       11/09/2026                                                    }
{   Autor:       Alejandro Laorden Hidalgo                                     }
{                                                                              }
{  Copyright (c) Alejandro Laorden Hidalgo.                                    }
{  SPDX-License-Identifier: MPL-2.0                                            }
{  Descripción:                                                                }
{    Trocea un volcado SQL (formato FZAM_COPIA_SEGURIDAD_SQL) en elementos:    }
{    blancos, comentarios, directivas DELIMITER y sentencias, respetando       }
{    literales, identificadores con comillas invertidas y comentarios.         }
{    Enmascara los literales y comentarios de una sentencia para aplicar       }
{    expresiones regulares solo al código y restaurarlos después.              }
{******************************************************************************}
unit Conversion.Sentencias;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections;

type
  TTipoElementoVolcado = (tevBlanco, tevComentario, tevDelimitador,
    tevSentencia);

  TElementoVolcado = record
    Tipo: TTipoElementoVolcado;
    // Texto original del elemento; en las sentencias, sin el terminador.
    Texto: string;
    // Delimitador que cerraba la sentencia ('' si el fichero acaba sin él).
    Terminador: string;
    // Delimitador vigente al leer el elemento (o el nuevo, en tevDelimitador).
    Delimitador: string;
  end;

  // Primitivas de lectura compartidas por el troceador y el enmascarador.
  TCursorSql = class
  public
    class function EsInicioComentarioLinea(
      const ATexto: string; AIndice: Integer): Boolean; static;
    class function FinDeLiteral(
      const ATexto: string; AIndice: Integer; ACierre: Char): Integer; static;
    class function FinDeComentarioLinea(
      const ATexto: string; AIndice: Integer): Integer; static;
    class function FinDeComentarioBloque(
      const ATexto: string; AIndice: Integer): Integer; static;
    class function EmpiezaPor(
      const ATexto, AFragmento: string; AIndice: Integer): Boolean; static;
  end;

  TLectorVolcadoSql = class
  private
    FTexto: string;
    FPosicion: Integer;
    FDelimitador: string;
    FElementos: TList<TElementoVolcado>;
    function EsBlanco(AIndice: Integer): Boolean;
    function EsDirectivaDelimiter: Boolean;
    procedure Anadir(ATipo: TTipoElementoVolcado;
      const ATexto, ATerminador: string);
    procedure LeerBlancos;
    procedure LeerComentarioLinea;
    procedure LeerComentarioBloque;
    procedure LeerDelimitador;
    procedure LeerSentencia;
  public
    constructor Create(const ATexto: string);
    destructor Destroy; override;
    function Trocear: TArray<TElementoVolcado>;
    class function TrocearTexto(
      const ATexto: string): TArray<TElementoVolcado>; static;
  end;

  // Código de una sentencia con los literales y comentarios sustituidos por
  // marcadores #1<n>#2. Los literales conservan sus comillas en el código.
  TTextoEnmascarado = record
  private
    FCodigo: string;
    FFragmentos: TArray<string>;
  public
    class function Crear(const ATexto: string): TTextoEnmascarado; static;
    function Restaurar(const ACodigo: string): string;
    function NumeroFragmentos: Integer;
    function Fragmento(AIndice: Integer): string;
    procedure SustituirFragmento(AIndice: Integer; const AValor: string);
    property Codigo: string read FCodigo;
  end;

const
  MARCA_INICIO = #1;
  MARCA_FIN = #2;

implementation

// ============================================================================
//   TCursorSql
// ============================================================================

class function TCursorSql.EmpiezaPor(
  const ATexto, AFragmento: string; AIndice: Integer): Boolean;
var
  iLongitud: Integer;
begin
  iLongitud := Length(AFragmento);
  Result := (iLongitud > 0) and (AIndice + iLongitud - 1 <= Length(ATexto))
    and (Copy(ATexto, AIndice, iLongitud) = AFragmento);
end;

class function TCursorSql.EsInicioComentarioLinea(
  const ATexto: string; AIndice: Integer): Boolean;
var
  bDobleGuion: Boolean;
begin
  bDobleGuion := EmpiezaPor(ATexto, '--', AIndice)
    and ((AIndice + 2 > Length(ATexto))
      or CharInSet(ATexto[AIndice + 2], [' ', #9, #13, #10]));
  Result := bDobleGuion or (ATexto[AIndice] = '#');
end;

class function TCursorSql.FinDeLiteral(
  const ATexto: string; AIndice: Integer; ACierre: Char): Integer;
var
  iLongitud, j: Integer;
  bCerrado: Boolean;
begin
  iLongitud := Length(ATexto);
  j := AIndice + 1;
  bCerrado := False;
  while (j <= iLongitud) and not bCerrado do
  begin
    if (ATexto[j] = '\') and (ACierre <> '`') then
      Inc(j, 2)
    else if ATexto[j] <> ACierre then
      Inc(j)
    else if (j < iLongitud) and (ATexto[j + 1] = ACierre) then
      Inc(j, 2)
    else
    begin
      bCerrado := True;
      Inc(j);
    end;
  end;
  if j > iLongitud + 1 then
    j := iLongitud + 1;
  Result := j;
end;

class function TCursorSql.FinDeComentarioLinea(
  const ATexto: string; AIndice: Integer): Integer;
var
  iLongitud, j: Integer;
begin
  iLongitud := Length(ATexto);
  j := AIndice;
  while (j <= iLongitud) and not CharInSet(ATexto[j], [#13, #10]) do
    Inc(j);
  Result := j;
end;

class function TCursorSql.FinDeComentarioBloque(
  const ATexto: string; AIndice: Integer): Integer;
var
  iCierre: Integer;
begin
  iCierre := Pos('*/', ATexto, AIndice + 2);
  if iCierre = 0 then
    Result := Length(ATexto) + 1
  else
    Result := iCierre + 2;
end;

// ============================================================================
//   TLectorVolcadoSql
// ============================================================================

constructor TLectorVolcadoSql.Create(const ATexto: string);
begin
  inherited Create;
  FTexto := ATexto;
  FElementos := TList<TElementoVolcado>.Create;
end;

destructor TLectorVolcadoSql.Destroy;
begin
  FreeAndNil(FElementos);
  inherited;
end;

class function TLectorVolcadoSql.TrocearTexto(
  const ATexto: string): TArray<TElementoVolcado>;
var
  oLector: TLectorVolcadoSql;
begin
  oLector := TLectorVolcadoSql.Create(ATexto);
  try
    Result := oLector.Trocear;
  finally
    FreeAndNil(oLector);
  end;
end;

function TLectorVolcadoSql.Trocear: TArray<TElementoVolcado>;
begin
  FElementos.Clear;
  FPosicion := 1;
  FDelimitador := ';';
  while FPosicion <= Length(FTexto) do
  begin
    if EsBlanco(FPosicion) then
      LeerBlancos
    else if TCursorSql.EsInicioComentarioLinea(FTexto, FPosicion) then
      LeerComentarioLinea
    else if TCursorSql.EmpiezaPor(FTexto, '/*', FPosicion) then
      LeerComentarioBloque
    else if EsDirectivaDelimiter then
      LeerDelimitador
    else
      LeerSentencia;
  end;
  Result := FElementos.ToArray;
end;

function TLectorVolcadoSql.EsBlanco(AIndice: Integer): Boolean;
begin
  Result := CharInSet(FTexto[AIndice], [' ', #9, #13, #10]);
end;

function TLectorVolcadoSql.EsDirectivaDelimiter: Boolean;
const
  PALABRA = 'DELIMITER';
begin
  Result := (FPosicion + Length(PALABRA) <= Length(FTexto))
    and SameText(Copy(FTexto, FPosicion, Length(PALABRA)), PALABRA)
    and CharInSet(FTexto[FPosicion + Length(PALABRA)], [' ', #9]);
end;

procedure TLectorVolcadoSql.Anadir(ATipo: TTipoElementoVolcado;
  const ATexto, ATerminador: string);
var
  oElemento: TElementoVolcado;
begin
  oElemento.Tipo := ATipo;
  oElemento.Texto := ATexto;
  oElemento.Terminador := ATerminador;
  oElemento.Delimitador := FDelimitador;
  FElementos.Add(oElemento);
end;

procedure TLectorVolcadoSql.LeerBlancos;
var
  iInicio: Integer;
begin
  iInicio := FPosicion;
  while (FPosicion <= Length(FTexto)) and EsBlanco(FPosicion) do
    Inc(FPosicion);
  Anadir(tevBlanco, Copy(FTexto, iInicio, FPosicion - iInicio), '');
end;

procedure TLectorVolcadoSql.LeerComentarioLinea;
var
  iFin: Integer;
begin
  iFin := TCursorSql.FinDeComentarioLinea(FTexto, FPosicion);
  Anadir(tevComentario, Copy(FTexto, FPosicion, iFin - FPosicion), '');
  FPosicion := iFin;
end;

procedure TLectorVolcadoSql.LeerComentarioBloque;
var
  iFin: Integer;
begin
  iFin := TCursorSql.FinDeComentarioBloque(FTexto, FPosicion);
  Anadir(tevComentario, Copy(FTexto, FPosicion, iFin - FPosicion), '');
  FPosicion := iFin;
end;

procedure TLectorVolcadoSql.LeerDelimitador;
var
  iFin: Integer;
  sLinea: string;
begin
  iFin := TCursorSql.FinDeComentarioLinea(FTexto, FPosicion);
  sLinea := Copy(FTexto, FPosicion, iFin - FPosicion);
  FDelimitador := Trim(Copy(sLinea, Length('DELIMITER') + 1, MaxInt));
  Anadir(tevDelimitador, sLinea, '');
  FPosicion := iFin;
end;

procedure TLectorVolcadoSql.LeerSentencia;
var
  iLongitud, i: Integer;
  bTerminada: Boolean;
  cActual: Char;
begin
  iLongitud := Length(FTexto);
  i := FPosicion;
  bTerminada := False;
  while (i <= iLongitud) and not bTerminada do
  begin
    cActual := FTexto[i];
    if CharInSet(cActual, ['''', '"', '`']) then
      i := TCursorSql.FinDeLiteral(FTexto, i, cActual)
    else if TCursorSql.EsInicioComentarioLinea(FTexto, i) then
      i := TCursorSql.FinDeComentarioLinea(FTexto, i)
    else if TCursorSql.EmpiezaPor(FTexto, '/*', i) then
      i := TCursorSql.FinDeComentarioBloque(FTexto, i)
    else if TCursorSql.EmpiezaPor(FTexto, FDelimitador, i) then
      bTerminada := True
    else
      Inc(i);
  end;
  if bTerminada then
  begin
    Anadir(tevSentencia, Copy(FTexto, FPosicion, i - FPosicion), FDelimitador);
    FPosicion := i + Length(FDelimitador);
  end
  else
  begin
    Anadir(tevSentencia, Copy(FTexto, FPosicion, iLongitud - FPosicion + 1),
      '');
    FPosicion := iLongitud + 1;
  end;
end;

// ============================================================================
//   TTextoEnmascarado
// ============================================================================

class function TTextoEnmascarado.Crear(
  const ATexto: string): TTextoEnmascarado;
var
  oCodigo: TStringBuilder;
  oFragmentos: TList<string>;
  iLongitud, i, iFin: Integer;
  cActual: Char;

  procedure Guardar(const AFragmento: string);
  begin
    oCodigo.Append(MARCA_INICIO).Append(oFragmentos.Count).Append(MARCA_FIN);
    oFragmentos.Add(AFragmento);
  end;

begin
  oCodigo := TStringBuilder.Create(Length(ATexto));
  oFragmentos := TList<string>.Create;
  try
    iLongitud := Length(ATexto);
    i := 1;
    while i <= iLongitud do
    begin
      cActual := ATexto[i];
      if CharInSet(cActual, ['''', '"']) then
      begin
        iFin := TCursorSql.FinDeLiteral(ATexto, i, cActual);
        oCodigo.Append(cActual);
        Guardar(Copy(ATexto, i + 1, iFin - i - 2));
        oCodigo.Append(cActual);
        i := iFin;
      end
      else if cActual = '`' then
      begin
        iFin := TCursorSql.FinDeLiteral(ATexto, i, cActual);
        oCodigo.Append(Copy(ATexto, i, iFin - i));
        i := iFin;
      end
      else if TCursorSql.EsInicioComentarioLinea(ATexto, i) then
      begin
        iFin := TCursorSql.FinDeComentarioLinea(ATexto, i);
        Guardar(Copy(ATexto, i, iFin - i));
        i := iFin;
      end
      else if TCursorSql.EmpiezaPor(ATexto, '/*', i) then
      begin
        iFin := TCursorSql.FinDeComentarioBloque(ATexto, i);
        Guardar(Copy(ATexto, i, iFin - i));
        i := iFin;
      end
      else
      begin
        oCodigo.Append(cActual);
        Inc(i);
      end;
    end;
    Result.FCodigo := oCodigo.ToString;
    Result.FFragmentos := oFragmentos.ToArray;
  finally
    FreeAndNil(oFragmentos);
    FreeAndNil(oCodigo);
  end;
end;

function TTextoEnmascarado.Restaurar(const ACodigo: string): string;
var
  oSalida: TStringBuilder;
  iLongitud, i, iFin, iIndice: Integer;
begin
  oSalida := TStringBuilder.Create(Length(ACodigo));
  try
    iLongitud := Length(ACodigo);
    i := 1;
    while i <= iLongitud do
    begin
      if ACodigo[i] = MARCA_INICIO then
      begin
        iFin := i + 1;
        while (iFin <= iLongitud) and (ACodigo[iFin] <> MARCA_FIN) do
          Inc(iFin);
        iIndice := StrToIntDef(Copy(ACodigo, i + 1, iFin - i - 1), -1);
        if (iIndice >= 0) and (iIndice < Length(FFragmentos)) then
          oSalida.Append(FFragmentos[iIndice]);
        i := iFin + 1;
      end
      else
      begin
        oSalida.Append(ACodigo[i]);
        Inc(i);
      end;
    end;
    Result := oSalida.ToString;
  finally
    FreeAndNil(oSalida);
  end;
end;

function TTextoEnmascarado.NumeroFragmentos: Integer;
begin
  Result := Length(FFragmentos);
end;

function TTextoEnmascarado.Fragmento(AIndice: Integer): string;
begin
  Result := FFragmentos[AIndice];
end;

procedure TTextoEnmascarado.SustituirFragmento(AIndice: Integer;
  const AValor: string);
begin
  FFragmentos[AIndice] := AValor;
end;

end.
