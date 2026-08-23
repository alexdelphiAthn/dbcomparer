unit Core.Output;

interface

uses
  Core.Interfaces,
  Core.Types;

procedure WriteScriptOutput(const Writer: IScriptWriter;
  const Options: TComparerOptions);

implementation

uses
  System.IOUtils,
  System.SysUtils,
  Core.Resources;

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

  OwnsEncoding := False;
  if Options.OutputEncoding = 'ansi' then
    OutputEncoding := TEncoding.ANSI
  else if Options.OutputEncoding = 'unicode' then
    OutputEncoding := TEncoding.Unicode
  else if Options.OutputEncoding = 'utf8nobom' then
  begin
    OutputEncoding := TUTF8Encoding.Create(False);
    OwnsEncoding := True;
  end
  else if Options.OutputEncoding = 'utf8bom' then
    OutputEncoding := TEncoding.UTF8
  else
    raise Exception.CreateFmt('Unsupported output encoding: %s',
      [Options.OutputEncoding]);

  try
    TFile.WriteAllText(Options.OutputFile, Writer.GetScript, OutputEncoding);
    Writeln(Format(TRes.MsgOutputSaved, [Options.OutputFile]));
  finally
    if OwnsEncoding then
      OutputEncoding.Free;
  end;
end;

end.
