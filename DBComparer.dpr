program DBComparerConsole;
{$APPTYPE CONSOLE}
uses
  Uni,
  Core.Helpers in 'Core.Helpers.pas',
  Core.Engine in 'Core.Engine.pas',
  Core.Interfaces in 'Core.Interfaces.pas',
  Core.Types in 'Core.Types.pas',
  Providers.MySQL in 'Providers.MySQL.pas',
  ScriptWriters in 'ScriptWriters.pas',
  Core.Output in 'Core.Output.pas',
  Providers.MySQL.Helpers in 'Providers.MySQL.Helpers.pas',
  System.SysUtils,
  Core.Resources in 'Core.Resources.pas';

procedure ShowUsage;
begin
  Writeln(TRes.UsageHeader);
  Writeln(Format(TRes.UsageExampleCmd, ['DBComparer']));
  Writeln('');
  Writeln(TRes.OptionsHeader);
  Writeln('  --nodelete           ' + TRes.OptNoDelete);
  Writeln('  --with-triggers      ' + TRes.OptTriggers);
  Writeln('  --mariadb10          SQL idempotente compatible con MariaDB 10.2');
  Writeln('  --with-data          ' + TRes.OptWithData);
  Writeln('  --with-data-diff     ' + TRes.OptDataDiff);
  Writeln('  --exclude-tables=T1,T2... ' + TRes.OptExclude);
  Writeln('                            ' + TRes.OptExcludeDesc);
  Writeln('  --include-tables=T1,T2...  '+ TRes.OptInclude);
  Writeln('                             '+ TRes.OptIncludeDesc);
  Writeln('  --preserve-views=V1,V2... ' + TRes.OptPreserveViews);
  Writeln('  --output=file.sql         ' + TRes.OptOutput);
  Writeln('  --encoding=utf8bom|utf8nobom|ansi|unicode ' + TRes.OptEncoding);
  Writeln('');
  Writeln(TRes.ExamplesHeader);
  Writeln('  DBComparer localhost:3306\origin_dev root\pass123 '+
          'localhost:3306\destiny_prod root\pass456 --nodelete --with-triggers');
  Writeln('');
  Writeln('  DBComparer localhost:3306\dev root\pass '+
          'localhost:3306\prod root\pass --with-data-diff --nodelete');
  Writeln('');
  Writeln('  DBComparer ... --with-data-diff --include-tables=fza_paises,fza_monedas');
  Writeln('');
  Writeln(TRes.FooterFile);
  Writeln('  DBComparer ... --output=script.sql --encoding=utf8bom');
  Writeln('  DBComparer ... > script.sql');
  Writeln('');
  Halt(1);
end;

var
  SourceProvider, TargetProvider: IDBMetadataProvider;
  SourceConn, TargetConn:TUniConnection;
  SourceConfig, TargetConfig: TConnectionConfig;
  Writer: IScriptWriter;
  Engine: TDBComparerEngine;
  Options:TComparerOptions;
  SourceHelpers: IDBHelpers;
begin
  try
    SourceConn := nil;
    TargetConn := nil;
    FormatSettings := TFormatSettings.Create('en-US');
    Set8087CW($133F);
    if (ParamCount < 4) then
    begin
      ShowUsage;
      Exit;
    end;
    Options := TComparerOptions.ParseFromCLI;
    // 2. Crear los proveedores (puente entre físico y lógico)
    SourceConfig := TConnectionConfig.Parse(ParamStr(1), ParamStr(2));
    TargetConfig := TConnectionConfig.Parse(ParamStr(3), ParamStr(4));
    try
      // ---------------------------------------------------------
      // 2. CONEXIÓN (Usando los Configs parseados)
      // ---------------------------------------------------------
      SourceConn := TUniConnection.Create(nil);
      SourceConn.ProviderName := 'MySQL';
      SourceConn.Server := SourceConfig.Server;
      SourceConn.Port := SourceConfig.Port;
      SourceConn.Username := SourceConfig.Username;
      SourceConn.Password := SourceConfig.Password;
      SourceProvider := TMySQLMetadataProvider.Create(SourceConn,
                                                      SourceConfig.Database,
                                                      Options.MariaDB10Compat);
      TargetConn := TUniConnection.Create(nil);
      TargetConn.ProviderName := 'MySQL';
      TargetConn.Server := TargetConfig.Server;
      TargetConn.Port := TargetConfig.Port;
      TargetConn.Username := TargetConfig.Username;
      TargetConn.Password := TargetConfig.Password;
      TargetProvider := TMySQLMetadataProvider.Create(TargetConn,
                                                      TargetConfig.Database,
                                                      Options.MariaDB10Compat);
      // 3. Crear escritor
      Writer := TStringListScriptWriter.Create;
      SourceHelpers := TMySQLHelpers.Create(Options.MariaDB10Compat);
      // 4. Crear e iniciar el motor
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
