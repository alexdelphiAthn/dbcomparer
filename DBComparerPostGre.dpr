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
  System.SysUtils;

procedure ShowUsage;
begin
  Writeln('Uso:');
  Writeln('  DBComparerPostgreSQL servidor1:puerto1\database1 usuario1\password1 '+
          'servidor2:puerto2\database2 usuario2\password2 [opciones]');
  Writeln('');
  Writeln('Nota: Puerto por defecto es 5432 si se omite');
  Writeln('      Para especificar schema: database\schema (por defecto: public)');
  Writeln('');
  Writeln('Opciones:');
  Writeln('  --nodelete           No elimina tablas, columnas ni índices en destino');
  Writeln('  --with-triggers      Incluye comparación de triggers');
  Writeln('  --with-data          Copia todos los datos de origen a destino (INSERT)');
  Writeln('  --with-data-diff     Sincroniza datos comparando por clave primaria');
  Writeln('                       (INSERT nuevos, UPDATE modificados, DELETE si no --nodelete)');
  Writeln('  --exclude-tables=T1,T2...  Excluye tablas específicas de la sincronización de datos');
  Writeln('                             (Lista Negra: Sincroniza todo MENOS esto)');
  Writeln('  --include-tables=T1,T2...  Solo sincroniza datos de estas tablas');
  Writeln('                             (Lista Blanca: Solo sincroniza ESTO, ignora el resto)');
  Writeln('');
  Writeln('Ejemplos:');
  Writeln('  DBComparerPostgreSQL localhost:5432\midb_prod postgres\pass123 '+
          'localhost:5432\midb_dev postgres\pass456 --nodelete --with-triggers');
  Writeln('');
  Writeln('  DBComparerPostgreSQL servidor1:5432\midb\public usuario\pass '+
          'servidor2:5432\midb\test_schema usuario\pass --with-data-diff --nodelete');
  Writeln('');
  Writeln('  DBComparerPostgreSQL localhost\midb postgres\pass '+
          'localhost\midb_test postgres\pass --with-data-diff');
  Writeln('');
  Writeln('  DBComparerPostgreSQL ... --with-data-diff --include-tables=clientes,productos');
  Writeln('');
  Writeln('El resultado se imprime por la salida estándar. '+
          'Para guardarlo en archivo:');
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