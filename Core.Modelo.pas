{******************************************************************************}
{                                                                              }
{  Módulo:       Core.Modelo                                                   }
{    Tipo:       Servicio                                                      }
{ Versión:       1.0.0                                                         }
{   Fecha:       24/09/2026                                                    }
{   Autor:       Alejandro Laorden Hidalgo                                     }
{                                                                              }
{  Copyright (c) Alejandro Laorden Hidalgo.                                    }
{  SPDX-License-Identifier: MPL-2.0                                            }
{  Descripción:                                                                }
{    Carga el modelo de una versión (un volcado de Factuzam, ya convertido al  }
{    dialecto del destino) en un esquema temporal del mismo servidor, para     }
{    compararlo base contra base con la de trabajo. Solo la estructura: las    }
{    filas del volcado no se cargan.                                           }
{******************************************************************************}
unit Core.Modelo;

interface

uses
  Uni;

type
  TEsquemaModelo = class
  private
    FConexion: TUniConnection;
    FEsquema: string;
    FEsquemaDestino: string;
    FSentenciasCargadas: Integer;
    FFilasOmitidas: Integer;
    function IntercalacionDestino(out ACharset: string): string;
    procedure EjecutarSentencia(const ASql: string);
  public
    // AConexion es la del destino (servidor, puerto y usuario): se usa
    // para crear y borrar el esquema temporal y para cargarlo.
    constructor Create(AConexion: TUniConnection;
      const AEsquemaDestino: string);
    destructor Destroy; override;
    // Crea el esquema temporal y ejecuta en él las sentencias del volcado,
    // salvo INSERT y REPLACE. Si algo falla, borra lo creado y relanza.
    procedure Cargar(const AVolcado: string);
    // Idempotente: se llama siempre al terminar, haya ido bien o no.
    procedure Borrar;
    property Esquema: string read FEsquema;
    property SentenciasCargadas: Integer read FSentenciasCargadas;
    property FilasOmitidas: Integer read FFilasOmitidas;
  end;

// SELECT VERSION() del servidor al que apunta AConexion.
function VersionServidor(AConexion: TUniConnection): string;

implementation

uses
  System.RegularExpressions,
  System.SysUtils,
  Conversion.Sentencias;

const
  cPrefijoEsquemaModelo = 'fza_modelo_tmp_';
  // Sentencias del volcado que no hacen falta para la estructura: filas y
  // el control de la transacción de la copia de seguridad.
  cPatronOmitir = '^\s*(INSERT|REPLACE|START\s+TRANSACTION|COMMIT|' +
    'ROLLBACK|SET\s+AUTOCOMMIT|USE|CREATE\s+DATABASE|DROP\s+DATABASE)\b';
  // Marcador de un literal o comentario en el código enmascarado.
  cPatronMarca = '\x01\d+\x02';

function VersionServidor(AConexion: TUniConnection): string;
var
  oConsulta: TUniQuery;
begin
  oConsulta := TUniQuery.Create(nil);
  try
    oConsulta.Connection := AConexion;
    oConsulta.SQL.Text := 'SELECT VERSION() AS VERSION_SERVIDOR';
    oConsulta.Open;
    Result := oConsulta.Fields[0].AsString;
  finally
    oConsulta.Free;
  end;
end;

function NombreEsquemaTemporal: string;
begin
  // En minúsculas: MySQL en Linux distingue la caja en los esquemas.
  Result := cPrefijoEsquemaModelo +
    FormatDateTime('yyyymmddhhnnss', Now) + '_' +
    LowerCase(IntToHex(Random($FFFF), 4));
end;

{ TEsquemaModelo }

constructor TEsquemaModelo.Create(AConexion: TUniConnection;
  const AEsquemaDestino: string);
begin
  inherited Create;
  FEsquemaDestino := AEsquemaDestino;
  FConexion := TUniConnection.Create(nil);
  FConexion.ProviderName := AConexion.ProviderName;
  FConexion.Server := AConexion.Server;
  FConexion.Port := AConexion.Port;
  FConexion.Username := AConexion.Username;
  FConexion.Password := AConexion.Password;
  FConexion.SpecificOptions.Assign(AConexion.SpecificOptions);
  FConexion.LoginPrompt := False;
end;

destructor TEsquemaModelo.Destroy;
begin
  try
    Borrar;
  except
    // Un destructor no puede lanzar: el temporal que quede se ve por su
    // prefijo y se puede borrar a mano.
    on E: Exception do
      Writeln(ErrOutput, 'AVISO: no se ha borrado el esquema temporal: ',
        E.Message);
  end;
  FConexion.Free;
  inherited Destroy;
end;

// El temporal nace con la intercalación de la base de trabajo: así las
// columnas sin intercalación propia y las rutinas quedan igual que allí.
function TEsquemaModelo.IntercalacionDestino(out ACharset: string): string;
var
  oConsulta: TUniQuery;
begin
  Result := 'utf8mb4_spanish_ci';
  ACharset := 'utf8mb4';
  oConsulta := TUniQuery.Create(nil);
  try
    oConsulta.Connection := FConexion;
    oConsulta.SQL.Text :=
      'SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME ' +
      '  FROM information_schema.SCHEMATA ' +
      ' WHERE SCHEMA_NAME = :ESQUEMA';
    oConsulta.ParamByName('ESQUEMA').AsString := FEsquemaDestino;
    oConsulta.Open;
    if not oConsulta.Eof then
    begin
      ACharset := oConsulta.Fields[0].AsString;
      Result := oConsulta.Fields[1].AsString;
    end;
  finally
    oConsulta.Free;
  end;
end;

procedure TEsquemaModelo.EjecutarSentencia(const ASql: string);
var
  oSentencia: TUniSQL;
begin
  // Sin ParamCheck: los cuerpos de las rutinas llevan «:=» y etiquetas
  // que no son parámetros.
  oSentencia := TUniSQL.Create(nil);
  try
    oSentencia.Connection := FConexion;
    oSentencia.ParamCheck := False;
    oSentencia.SQL.Text := ASql;
    oSentencia.Execute;
  finally
    oSentencia.Free;
  end;
end;

procedure TEsquemaModelo.Cargar(const AVolcado: string);
var
  aElementos: TArray<TElementoVolcado>;
  oElemento: TElementoVolcado;
  oMascara: TTextoEnmascarado;
  sCharset: string;
  sIntercalacion: string;
begin
  if FEsquema <> '' then
    raise EInvalidOpException.Create('El modelo ya está cargado.');
  FSentenciasCargadas := 0;
  FFilasOmitidas := 0;
  FConexion.Database := '';
  FConexion.Connect;
  sIntercalacion := IntercalacionDestino(sCharset);
  FEsquema := NombreEsquemaTemporal;
  try
    EjecutarSentencia('CREATE DATABASE `' + FEsquema +
      '` CHARACTER SET ' + sCharset + ' COLLATE ' + sIntercalacion);
  except
    on E: Exception do
    begin
      FEsquema := '';
      raise Exception.CreateFmt(
        'No se puede crear el esquema temporal del modelo (hace falta ' +
        'permiso CREATE sobre el servidor para el usuario %s): %s',
        [FConexion.Username, E.Message]);
    end;
  end;
  try
    FConexion.Disconnect;
    FConexion.Database := FEsquema;
    FConexion.Connect;
    aElementos := TLectorVolcadoSql.TrocearTexto(AVolcado);
    for oElemento in aElementos do
    begin
      if oElemento.Tipo = tevSentencia then
      begin
        oMascara := TTextoEnmascarado.Crear(oElemento.Texto);
        // Un tramo con solo comentarios («Query was empty»).
        if Trim(TRegEx.Replace(oMascara.Codigo, cPatronMarca, '')) = '' then
          Inc(FFilasOmitidas)
        else if TRegEx.IsMatch(oMascara.Codigo, cPatronOmitir,
             [roIgnoreCase]) then
          Inc(FFilasOmitidas)
        else
        begin
          try
            EjecutarSentencia(oElemento.Texto);
          except
            on E: Exception do
              raise Exception.CreateFmt(
                'Error al cargar el modelo en %s: %s' + sLineBreak + '%s',
                [FEsquema, E.Message, Copy(Trim(oElemento.Texto), 1, 300)]);
          end;
          Inc(FSentenciasCargadas);
        end;
      end;
    end;
  except
    Borrar;
    raise;
  end;
end;

procedure TEsquemaModelo.Borrar;
begin
  if FEsquema <> '' then
  begin
    try
      FConexion.Disconnect;
      FConexion.Database := '';
      FConexion.Connect;
      EjecutarSentencia('DROP DATABASE IF EXISTS `' + FEsquema + '`');
    finally
      FEsquema := '';
      FConexion.Disconnect;
    end;
  end;
end;

end.
