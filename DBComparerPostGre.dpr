program DBComparerPostgreSQL;

{$APPTYPE CONSOLE}

uses
  Uni,
  Core.Helpers in 'Core.Helpers.pas',
  Core.Engine in 'Core.Engine.pas',
  Core.Interfaces in 'Core.Interfaces.pas',
  Core.Types in 'Core.Types.pas',
  Providers.PostgreSQL in 'Providers.PostgreSQL.pas',
  ScriptWriters in 'ScriptWriters.pas',
  Providers.PostgreSQL.Helpers in 'Providers.PostgreSQL.Helpers.pas',
  System.SysUtils,
  Core.Resources in 'Core.Resources.pas';

procedure ShowUsage;
begin
 Writeln(TRes.UsageHeader); // "Uso:"
  Writeln(Format(TRes.UsagePGCmd, ['DBComparerPostgreSQL']));
  Writeln('');

  // Nota del puerto (Reutilizamos la variable genérica pasando 5432)
  Writeln(Format(TRes.UsageNotePort, [5432]));

  // Nota específica de Schema
  Writeln(TRes.MsgPGSchema);
  Writeln('');

  // Opciones Comunes
  Writeln(TRes.OptionsHeader); // "Opciones:"
  Writeln('  --nodelete           ' + TRes.OptNoDelete);
  Writeln('  --with-triggers      ' + TRes.OptTriggers);
  Writeln('  --with-data          ' + TRes.OptWithData);
  Writeln('  --with-data-diff     ' + TRes.OptDataDiff);
  Writeln('                       (INSERT/UPDATE/DELETE)');
  Writeln('  --exclude-tables=... ' + TRes.OptExclude);
  Writeln('                             ' + TRes.OptExcludeDesc);
  Writeln('  --include-tables=... ' + TRes.OptInclude);
  Writeln('                             ' + TRes.OptIncludeDesc);
  Writeln('');

  // Ejemplos
  Writeln(TRes.ExamplesHeader); // "Ejemplos:"

  // Ejemplo 1: Localhost completo
  Writeln(Format(TRes.ExPGFull, ['DBComparerPostgreSQL']));
  Writeln('');

  // Ejemplo 2: Uso de Schemas explícitos
  Writeln(Format(TRes.ExPGSchema, ['DBComparerPostgreSQL']));
  Writeln('');

  // Ejemplo 3: Simple
  Writeln(Format(TRes.ExPGSimple, ['DBComparerPostgreSQL']));
  Writeln('');

  // Ejemplo 4: Filtros
  Writeln(Format(TRes.ExPGFilter, ['DBComparerPostgreSQL']));

  Writeln('');
  Writeln(TRes.FooterFile);
  Writeln('  DBComparerPostgreSQL ... > script.sql');
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
  SourceSchema, TargetSchema: string;
begin
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
    
    // Extraer schema si viene en formato database\schema
    SourceSchema := 'public';
    if Pos('\', SourceConfig.Database) > 0 then
    begin
      SourceSchema := Copy(SourceConfig.Database, 
                          Pos('\', SourceConfig.Database) + 1, 
                          Length(SourceConfig.Database));
      SourceConfig.Database := Copy(SourceConfig.Database, 1, 
                                   Pos('\', SourceConfig.Database) - 1);
    end;
    
    TargetSchema := 'public';
    if Pos('\', TargetConfig.Database) > 0 then
    begin
      TargetSchema := Copy(TargetConfig.Database, 
                          Pos('\', TargetConfig.Database) + 1, 
                          Length(TargetConfig.Database));
      TargetConfig.Database := Copy(TargetConfig.Database, 1, 
                                   Pos('\', TargetConfig.Database) - 1);
    end;
    
    try
      // ---------------------------------------------------------
      // CONEXIÓN POSTGRESQL
      // ---------------------------------------------------------
      SourceConn := TUniConnection.Create(nil);
      SourceConn.ProviderName := 'PostgreSQL';
      SourceConn.Server := SourceConfig.Server;
      if SourceConfig.Port > 0 then
        SourceConn.Port := SourceConfig.Port
      else
        SourceConn.Port := 5432; // Puerto por defecto PostgreSQL
      SourceConn.Username := SourceConfig.Username;
      SourceConn.Password := SourceConfig.Password;
      SourceConn.Database := SourceConfig.Database;
      
      SourceProvider := TPostgreSQLMetadataProvider.Create(SourceConn,
                                                          SourceConfig.Database,
                                                          SourceSchema);
      
      TargetConn := TUniConnection.Create(nil);
      TargetConn.ProviderName := 'PostgreSQL';
      TargetConn.Server := TargetConfig.Server;
      if TargetConfig.Port > 0 then
        TargetConn.Port := TargetConfig.Port
      else
        TargetConn.Port := 5432;
      TargetConn.Username := TargetConfig.Username;
      TargetConn.Password := TargetConfig.Password;
      TargetConn.Database := TargetConfig.Database;
      
      TargetProvider := TPostgreSQLMetadataProvider.Create(TargetConn,
                                                          TargetConfig.Database,
                                                          TargetSchema);
      
      // Crear escritor y helpers
      Writer := TStringListScriptWriter.Create;
      SourceHelpers := TPostgreSQLHelpers.Create;
      
      // Crear e iniciar el motor
      Engine := TDBComparerEngine.Create(SourceProvider,
                                         TargetProvider,
                                         Writer,
                                         SourceHelpers,
                                         Options);
      try
        Engine.GenerateScript;
        Writeln(Writer.GetScript);
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
      Writeln(ErrOutput, 'ERROR: ', E.Message);
  end;
end.