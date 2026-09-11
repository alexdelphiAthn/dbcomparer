unit Core.Output;

interface

uses
  System.SysUtils,
  Core.Interfaces,
  Core.Types;

procedure WriteScriptOutput(const Writer: IScriptWriter;
  const Options: TComparerOptions);

// Codificación de salida a partir de su nombre (utf8bom, utf8nobom, ansi,
// unicode). AOwns indica si el llamador debe liberar el objeto devuelto.
function EncodingFromName(const AName: string;
  out AOwns: Boolean): TEncoding;

implementation

uses
  System.IOUtils,
  Core.Resources;

function EncodingFromName(const AName: string;
  out AOwns: Boolean): TEncoding;
begin
  AOwns := False;
  if AName = 'ansi' then
    Result := TEncoding.ANSI
  else if AName = 'unicode' then
    Result := TEncoding.Unicode
  else if AName = 'utf8nobom' then
  begin
    Result := TUTF8Encoding.Create(False);
    AOwns := True;
  end
  else if AName = 'utf8bom' then
    Result := TEncoding.UTF8
  else
    raise Exception.CreateFmt('Unsupported output encoding: %s', [AName]);
end;

procedure WriteScriptOutput(const Writer: IScriptWriter;
  const Options: TComparerOptions);
var
  OutputEncoding: TEncoding;
  OwnsEncoding: Boolean;
begin
  if Options.OutputFile = '' then
  begin
    Writeln(Writer.GetScript);
    Exit;
  end;

  OutputEncoding := EncodingFromName(Options.OutputEncoding, OwnsEncoding);
  try
    TFile.WriteAllText(Options.OutputFile, Writer.GetScript, OutputEncoding);
    Writeln(Format(TRes.MsgOutputSaved, [Options.OutputFile]));
  finally
    if OwnsEncoding then
      OutputEncoding.Free;
  end;
end;

end.
