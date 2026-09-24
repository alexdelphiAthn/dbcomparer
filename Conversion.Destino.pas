{******************************************************************************}
{                                                                              }
{  Módulo:       Conversion.Destino                                            }
{    Tipo:       Librería                                                      }
{ Versión:       1.0.0                                                         }
{   Fecha:       24/09/2026                                                    }
{   Autor:       Alejandro Laorden Hidalgo                                     }
{                                                                              }
{  Copyright (c) Alejandro Laorden Hidalgo.                                    }
{  SPDX-License-Identifier: MPL-2.0                                            }
{  Descripción:                                                                }
{    Último paso de todo lo que emite DBComparer, venga de comparar dos bases  }
{    o de un volcado: deja el SQL en el dialecto del destino según los         }
{    criterios de Core.Dialecto.                                               }
{******************************************************************************}
unit Conversion.Destino;

interface

uses
  System.Classes,
  Core.Dialecto;

type
  TResultadoConversionDestino = record
    Texto: string;
    // Construcciones sin equivalente en el destino: hay que revisarlas.
    Avisos: TArray<string>;
    Resumen: string;
  end;

// Script generado por la comparación: el SQL de tablas, índices y
// restricciones ya sale con la sintaxis del destino; aquí solo queda, para
// MySQL 8, pasar vistas y rutinas por la conversión.
function ConvertirScriptComparacion(const AScript: string;
  const ACriterios: TCriteriosDialecto): TResultadoConversionDestino;
// Volcado completo de Factuzam (MariaDB 12) al dialecto del destino.
function ConvertirVolcado(const AVolcado: string;
  const ACriterios: TCriteriosDialecto): TResultadoConversionDestino;

implementation

uses
  System.SysUtils,
  Conversion.MySQL841,
  Conversion.Sentencias,
  Providers.MySQL.Helpers;

function ConvertirMySQL841(const ATexto: string): TResultadoConversionDestino;
var
  oConversor: TConversorMySQL841;
begin
  oConversor := TConversorMySQL841.Create;
  try
    Result.Texto := oConversor.Convertir(ATexto);
    Result.Avisos := oConversor.Informe.Avisos.ToStringArray;
    Result.Resumen := oConversor.Informe.Resumen;
  finally
    FreeAndNil(oConversor);
  end;
end;

// Las rebajas de MariaDB 10 solo tocan el código: los literales (datos,
// comentarios de columna) quedan como están.
function RebajarVolcado(const AVolcado: string;
  const ACriterios: TCriteriosDialecto): string;
var
  aElementos: TArray<TElementoVolcado>;
  oElemento: TElementoVolcado;
  oMascara: TTextoEnmascarado;
  oSalida: TStringBuilder;
begin
  oSalida := TStringBuilder.Create(Length(AVolcado) + 1024);
  try
    aElementos := TLectorVolcadoSql.TrocearTexto(AVolcado);
    for oElemento in aElementos do
    begin
      if oElemento.Tipo = tevSentencia then
      begin
        oMascara := TTextoEnmascarado.Crear(oElemento.Texto);
        oSalida.Append(oMascara.Restaurar(
          NormalizarSqlParaDestino(oMascara.Codigo, ACriterios)));
        oSalida.Append(oElemento.Terminador);
      end
      else
        oSalida.Append(oElemento.Texto);
    end;
    Result := oSalida.ToString;
  finally
    FreeAndNil(oSalida);
  end;
end;

function ConvertirScriptComparacion(const AScript: string;
  const ACriterios: TCriteriosDialecto): TResultadoConversionDestino;
begin
  if ACriterios.ConvertirMySQL841 then
    Result := ConvertirMySQL841(AScript)
  else
  begin
    Result := Default(TResultadoConversionDestino);
    Result.Texto := AScript;
  end;
end;

function ConvertirVolcado(const AVolcado: string;
  const ACriterios: TCriteriosDialecto): TResultadoConversionDestino;
begin
  if ACriterios.ConvertirMySQL841 then
    Result := ConvertirMySQL841(AVolcado)
  else
  begin
    Result := Default(TResultadoConversionDestino);
    if ACriterios.RebajarIntercalaciones or ACriterios.QuitarOrReplaceTabla then
      Result.Texto := RebajarVolcado(AVolcado, ACriterios)
    else
      Result.Texto := AVolcado;
    Result.Resumen := 'Conversión a ' +
      DescripcionDialecto(ACriterios.Dialecto) + ': sin avisos.';
  end;
end;

end.
