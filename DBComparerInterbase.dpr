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
  Providers.InterBase.Helpers in 'Providers.InterBase.Helpers.pas',
  System.SysUtils;

procedure ShowUsage;
begin
  Writeln('Uso:');
  Writeln('  DBComparerInterBase servidor1:puerto1\database1.gdb usuario1\password1 '+
          'servidor2:puerto2\database2.gdb usuario2\password2 [opciones]');
  Writeln('');
  Writeln('Nota: Puerto por defecto es 3050 si se omite');
  Writeln('      Para base de datos local: localhost\C:\ruta\database.gdb');
  Writeln('      Para base de datos embebida: \C:\ruta\database.gdb (sin servidor)');
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
  Writeln('  DBComparerInterBase localhost:3050\C:\DB\prod.gdb SYSDBA\masterkey '+
          'localhost:3050\C:\DB\dev.gdb SYSDBA\masterkey --nodelete --with-triggers');
  Writeln('');
  Writeln('  DBComparerInterBase servidor1\C:\DB\midb.gdb usuario\pass '+
          'servidor2\C:\DB\midb.gdb usuario\pass --with-data-diff --nodelete');
  Writeln('');
  Writeln('  DBComparerInterBase \C:\DB\local.gdb SYSDBA\masterkey '+
          '\C:\DB\local_test.gdb SYSDBA\masterkey --with-data-diff');
  Writeln('');
  Writeln('  DBComparerInterBase ... --with-data-diff --include-tables=CLIENTES,PRODUCTOS');
  Writeln('');
  Writeln('El resultado se imprime por la salida estándar. '+
          'Para guardarlo en archivo:');
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