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
  Providers.MySQL.Helpers in 'Providers.MySQL.Helpers.pas',
  System.SysUtils,
  System.IOUtils,
  Core.Resources in 'Core.Resources.pas';

procedure ShowUsage;
begin
  Writeln(TRes.UsageHeader);
  Writeln(Format(TRes.UsageExampleCmd, ['DBComparer']));
  Writeln('');
  Writeln(TRes.OptionsHeader);
  Writeln('  --nodelete           ' + TRes.OptNoDelete);
  Writeln('  --with-triggers      ' + TRes.OptTriggers);
  Writeln('  --with-data          ' + TRes.OptWithData);
  Writeln('  --with-data-diff     ' + TRes.OptDataDiff);
  Writeln('  --exclude-tables=T1,T2... ' + TRes.OptExclude);
  Writeln('                            ' + TRes.OptExcludeDesc);
  Writeln('  --include-tables=T1,T2...  '+ TRes.OptInclude);
  Writeln('                             '+ TRes.OptIncludeDesc);
  Writeln('  --output=archivo.sql       ' + TRes.OptOutput);
  Writeln('  --encoding=formato         ' + TRes.OptEncoding);
  Writeln('');
  Writeln(TRes.ExamplesHeader);
  Writeln('  DBComparer localhost:3306\origin_prod root\pass123 '+
          'localhost:3306\destiny_dev root\pass456 --nodelete --with-triggers');
  Writeln('');
  Writeln('  DBComparer localhost:3306\dev root\pass '+
          'localhost:3306\prod root\pass --with-data-diff --nodelete');
  Writeln('');
  Writeln('  DBComparer ... --with-data-diff --include-tables=fza_paises,fza_monedas');
  Writeln('');
  Writeln(TRes.FooterFile);
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
                                                      SourceConfig.Database);
      TargetConn := TUniConnection.Create(nil);
      TargetConn.ProviderName := 'MySQL';
      TargetConn.Server := TargetConfig.Server;
      TargetConn.Port := TargetConfig.Port;
      TargetConn.Username := TargetConfig.Username;
      TargetConn.Password := TargetConfig.Password;
      TargetProvider := TMySQLMetadataProvider.Create(TargetConn,
                                                      TargetConfig.Database);
      // 3. Crear escritor
      Writer := TStringListScriptWriter.Create;
      SourceHelpers := TMySQLHelpers.Create;
      // 4. Crear e iniciar el motor
      Engine := TDBComparerEngine.Create(SourceProvider,
                                         TargetProvider,
                                         Writer,
                                         SourceHelpers,
                                         Options);
      try
        Engine.GenerateScript;
        if Options.OutputFile <> '' then
        begin
          var MyEncoding: TEncoding;
          var FreeEncoding: Boolean := False;
          if Options.OutputEncoding = 'ansi' then
            MyEncoding := TEncoding.ANSI
          else if Options.OutputEncoding = 'unicode' then
            MyEncoding := TEncoding.Unicode
          else if Options.OutputEncoding = 'utf8nobom' then
          begin
            MyEncoding := TUTF8Encoding.Create(False); // UTF8 sin BOM
            FreeEncoding := True;
          end
          else
            MyEncoding := TEncoding.UTF8; // Por defecto es UTF-8 con BOM
          try
            TFile.WriteAllText(Options.OutputFile, Writer.GetScript,
                               MyEncoding);
            Writeln('Script generado exitosamente y guardado en: ',
                     Options.OutputFile);
          finally
            if FreeEncoding then
              MyEncoding.Free;
          end;
        end
        else
        begin
          // Si no se pasó --output, se imprime en consola
          Writeln(Writer.GetScript);
        end;

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
