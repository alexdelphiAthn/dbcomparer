program DBComparerConsole;

uses
  ScriptWriters,
  Core.Interfaces,
  Core.Types,
  Core.Engine,
  Providers.MySQL;

var
  Conn1, Conn2: TUniConnection;
  SourceProvider, TargetProvider: IDBMetadataProvider;
  Writer: IScriptWriter;
  Engine: TDBComparerEngine;
begin
  try
    // 1. Configurar conexiones (físico)
    Conn1 := TUniConnection.Create(nil); 
    // ... configurar Conn1 ...
    
    Conn2 := TUniConnection.Create(nil);
    // ... configurar Conn2 ...

    // 2. Crear los proveedores (puente entre físico y lógico)
    SourceProvider := TMySQLMetadataProvider.Create(Conn1, 'BaseOrigen');
    TargetProvider := TMySQLMetadataProvider.Create(Conn2, 'BaseDestino');
    
    // 3. Crear escritor
    Writer := TStringListScriptWriter.Create; // Una clase simple que envuelve TStringList

    // 4. Crear e iniciar el motor
    Engine := TDBComparerEngine.Create(SourceProvider, TargetProvider, Writer, Options);
    try
      Engine.GenerateScript;
      Writeln(Writer.GetScript);
    finally
      Engine.Free;
    end;

  finally
    Conn1.Free;
    Conn2.Free;
  end;
end.