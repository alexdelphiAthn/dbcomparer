program DBComparerInterBase;
{$APPTYPE CONSOLE}
uses
  Uni,
  Core.Helpers in 'Core.Helpers.pas',
  Core.Engine in 'Core.Engine.pas',
  Core.Interfaces in 'Core.Interfaces.pas',
  Core.Types in 'Core.Types.pas',
  Providers.InterBase in 'Providers.InterBase.pas',
  ScriptWriters in 'ScriptWriters.pas',
  Core.Output in 'Core.Output.pas',
  Providers.InterBase.Helpers in 'Providers.InterBase.Helpers.pas',
  System.SysUtils,
  Core.Resources in 'Core.Resources.pas';

procedure ShowUsage;
begin
 Writeln(TRes.UsageHeader); // "Uso:"
  Writeln(Format(TRes.UsageIBCmd, ['DBComparerInterBase']));
  Writeln('');
  // Nota del puerto (Reutilizamos la variable genérica pasando 3050)
  Writeln(Format(TRes.UsageNotePort, [3050]));
  // Notas específicas InterBase
  Writeln(TRes.MsgIBLocal);
  Writeln(TRes.MsgIBEmbedded);
  Writeln('');
  // Opciones Comunes
  Writeln(TRes.OptionsHeader); // "Opciones:"
  Writeln('  --nodelete           ' + TRes.OptNoDelete);
  Writeln('  --with-triggers      ' + TRes.OptTriggers);
  Writeln('  --with-data          ' + TRes.OptWithData);
  Writeln('  --with-data-diff     ' + TRes.OptDataDiff);
  Writeln('                       (INSERT/UPDATE/DELETE)');
  Writeln('  --exclude-tables=T1,T2... ' + TRes.OptExclude);
  Writeln('                             ' + TRes.OptExcludeDesc);
  Writeln('  --include-tables=T1,T2... ' + TRes.OptInclude);
  Writeln('                             ' + TRes.OptIncludeDesc);
  Writeln('  --preserve-views=V1,V2... ' + TRes.OptPreserveViews);
  Writeln('  --output=file.sql         ' + TRes.OptOutput);
  Writeln('  --encoding=utf8bom|utf8nobom|ansi|unicode ' + TRes.OptEncoding);
  Writeln('');
  // Ejemplos
  Writeln(TRes.ExamplesHeader); // "Ejemplos:"
  // Ejemplo 1: Localhost completo
  Writeln(Format(TRes.ExIBFull, ['DBComparerInterBase']));
  Writeln('');
  // Ejemplo 2: Servidor remoto
  Writeln(Format(TRes.ExIBServer, ['DBComparerInterBase']));
  Writeln('');
  // Ejemplo 3: Embebido
  Writeln(Format(TRes.ExIBEmbedded, ['DBComparerInterBase']));
  Writeln('');
  // Ejemplo 4: Filtros
  Writeln(Format(TRes.ExIBFilter, ['DBComparerInterBase']));
  Writeln('');
  Writeln(TRes.FooterFile);
  Writeln('  DBComparerInterBase ... --output=script.sql --encoding=utf8bom');
  Writeln('  DBComparerInterBase ... > script.sql');
  Writeln('');
  Halt(1);
end;

var
  SourceProvider, TargetProvider: IDBMetadataProvider;
  SourceConn, TargetConn: TUniConnection;
  SourceConfig, TargetConfig: TConnectionConfig;
  Writer: IScriptWriter;
  Engine: TDBComparerEngine;
  Options: TComparerOptions;
  SourceHelpers: IDBHelpers;
begin
  SourceConn := nil;
  TargetConn := nil;
  Options := nil;
  try
    if (ParamCount < 4) then
    begin
      ShowUsage;
      Exit;
    end;
    Options := TComparerOptions.ParseFromCLI;
    
    // Parsear configuraciones
    SourceConfig := TConnectionConfig.Parse(ParamStr(1), ParamStr(2));
    TargetConfig := TConnectionConfig.Parse(ParamStr(3), ParamStr(4));
    
    try
      // ---------------------------------------------------------
      // CONEXIÓN INTERBASE
      // ---------------------------------------------------------
      SourceConn := TUniConnection.Create(nil);
      SourceConn.ProviderName := 'InterBase';
      
      // Si no hay servidor, es modo embebido
      if SourceConfig.Server <> '' then
      begin
        SourceConn.Server := SourceConfig.Server;
        if SourceConfig.Port > 0 then
          SourceConn.Port := SourceConfig.Port
        else
          SourceConn.Port := 3050; // Puerto por defecto InterBase
      end;
      
      SourceConn.Database := SourceConfig.Database;
      SourceConn.Username := SourceConfig.Username;
      SourceConn.Password := SourceConfig.Password;
      
      // InterBase específicos
      SourceConn.SpecificOptions.Values['Charset'] := 'UTF8';
      
      SourceProvider := TInterBaseMetadataProvider.Create(SourceConn,
                                                          SourceConfig.Database);
      
      TargetConn := TUniConnection.Create(nil);
      TargetConn.ProviderName := 'InterBase';
      
      if TargetConfig.Server <> '' then
      begin
        TargetConn.Server := TargetConfig.Server;
        if TargetConfig.Port > 0 then
          TargetConn.Port := TargetConfig.Port
        else
          TargetConn.Port := 3050;
      end;
      
      TargetConn.Database := TargetConfig.Database;
      TargetConn.Username := TargetConfig.Username;
      TargetConn.Password := TargetConfig.Password;
      TargetConn.SpecificOptions.Values['Charset'] := 'UTF8';
      
      TargetProvider := TInterBaseMetadataProvider.Create(TargetConn,
                                                          TargetConfig.Database);
      
      // Crear escritor y helpers
      Writer := TStringListScriptWriter.Create;
      SourceHelpers := TInterBaseHelpers.Create;
      
      // Crear e iniciar el motor
      Engine := TDBComparerEngine.Create(SourceProvider,
                                         TargetProvider,
                                         Writer,
                                         SourceHelpers,
                                         Options);
      try
        Engine.GenerateScript;
        WriteScriptOutput(Writer, Options);
      finally
        Engine.Free;
      end;
    finally
      Options.Free;
      SourceConn.Free;
      TargetConn.Free;
    end;
  except
    on E: Exception do
    begin
      Writeln(ErrOutput, 'ERROR: ', E.Message);
      ExitCode := 1;
    end;
  end;
end.
