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
  System.StrUtils,
  System.IOUtils,
  Core.Resources in 'Core.Resources.pas',
  Conversion.Sentencias in 'Conversion.Sentencias.pas',
  Conversion.MySQL841 in 'Conversion.MySQL841.pas',
  Core.Dialecto in 'Core.Dialecto.pas',
  Conversion.Destino in 'Conversion.Destino.pas',
  Core.Modelo in 'Core.Modelo.pas';

const
  // Hay construcciones sin equivalente en el destino: revisar a mano.
  cSalidaConAvisos = 2;

procedure ShowUsage;
begin
  Writeln(TRes.UsageHeader);
  Writeln(Format(TRes.UsageExampleCmd, ['DBComparer']));
  Writeln('  DBComparer --modelo=modelo.sql servidor:puerto\base ' +
          'usuario\clave [opciones]');
  Writeln('  DBComparer --convertir origen.sql destino.sql ' +
          '--destino=mariadb12|mariadb10|mysql841');
  Writeln('');
  Writeln(TRes.OptionsHeader);
  Writeln('  --destino=auto|mariadb12|mariadb10|mysql841');
  Writeln('                       Dialecto del SQL emitido. Por defecto ' +
          'auto: se decide con');
  Writeln('                       SELECT VERSION() del destino (MariaDB 11+ ' +
          '= mariadb12,');
  Writeln('                       MariaDB 10 = mariadb10, MySQL = mysql841). ' +
          'Todos los');
  Writeln('                       cambios se pueden relanzar: IF NOT EXISTS ' +
          'en MariaDB,');
  Writeln('                       consulta a INFORMATION_SCHEMA en MySQL 8.');
  Writeln('  --mariadb10          Igual que --destino=mariadb10');
  Writeln('  --mysql841           Igual que --destino=mysql841');
  Writeln('  --nodelete           ' + TRes.OptNoDelete);
  Writeln('  --with-triggers      ' + TRes.OptTriggers);
  Writeln('  --with-data          ' + TRes.OptWithData);
  Writeln('  --with-data-diff     ' + TRes.OptDataDiff);
  Writeln('  --exclude-tables=T1,T2... ' + TRes.OptExclude);
  Writeln('                            ' + TRes.OptExcludeDesc);
  Writeln('  --include-tables=T1,T2...  '+ TRes.OptInclude);
  Writeln('                             '+ TRes.OptIncludeDesc);
  Writeln('  --preserve-views=V1,V2... ' + TRes.OptPreserveViews);
  Writeln('  --output=file.sql         ' + TRes.OptOutput);
  Writeln('  --encoding=utf8bom|utf8nobom|ansi|unicode ' + TRes.OptEncoding);
  Writeln('  --ssl                Conexión cifrada (MySQL.Protocol=mpSSL)');
  Writeln('');
  Writeln('  --modelo=modelo.sql  Compara un volcado de Factuzam (el modelo ' +
          'de una versión)');
  Writeln('                       con la base indicada: lo convierte al ' +
          'dialecto del');
  Writeln('                       servidor, lo carga sin filas en un esquema ' +
          'temporal,');
  Writeln('                       compara y borra el temporal. Nunca borra ' +
          'nada del');
  Writeln('                       destino (--nodelete implícito). El usuario ' +
          'necesita');
  Writeln('                       permiso CREATE y DROP sobre el servidor.');
  Writeln('  --convertir          Convierte un volcado al dialecto de ' +
          '--destino.');
  Writeln('  usuario\*            La contraseña se toma de la variable de ' +
          'entorno');
  Writeln('                       DBCOMPARER_PASSWORD.');
  Writeln('');
  Writeln('  Código de salida: 0 bien, 1 error, 2 con avisos (construcciones ' +
          'sin');
  Writeln('  equivalente en el destino; se listan en la salida de error).');
  Writeln('');
  Writeln(TRes.ExamplesHeader);
  Writeln('  DBComparer localhost:3306\origin_dev root\pass123 '+
          'localhost:3306\destiny_prod root\pass456 --nodelete --with-triggers');
  Writeln('');
  Writeln('  DBComparer --modelo=modelo_1.0.16.sql 127.0.0.1:3306\factuzam ' +
          'root\* --output=cambios.sql');
  Writeln('');
  Writeln('  DBComparer --convertir factuzam_original.sql ' +
          'factuzam_mysql.sql --destino=mysql841');
  Writeln('');
  Writeln(TRes.FooterFile);
  Writeln('  DBComparer ... --output=script.sql --encoding=utf8bom');
  Writeln('  DBComparer ... > script.sql');
  Writeln('');
  Halt(1);
end;

function HayOpcion(const AOpcion: string): Boolean;
var
  i: Integer;
begin
  Result := False;
  for i := 1 to ParamCount do
    Result := Result or SameText(ParamStr(i), AOpcion);
end;

// Las mismas opciones que usa Factuzam con MariaDB y MySQL: texto Unicode
// en utf8mb4 y, con --ssl, conexión cifrada.
function CrearConexion(const AConfig: TConnectionConfig): TUniConnection;
begin
  Result := TUniConnection.Create(nil);
  Result.ProviderName := 'MySQL';
  Result.Server := AConfig.Server;
  Result.Port := AConfig.Port;
  Result.Username := AConfig.Username;
  Result.Password := AConfig.Password;
  Result.LoginPrompt := False;
  Result.SpecificOptions.Values['MySQL.UseUnicode'] := 'True';
  Result.SpecificOptions.Values['MySQL.Charset'] := 'utf8mb4';
  if HayOpcion('--ssl') then
    Result.SpecificOptions.Values['MySQL.Protocol'] := 'mpSSL';
end;

procedure InformarAvisos(const AAvisos: TArray<string>);
var
  sAviso: string;
begin
  for sAviso in AAvisos do
    Writeln(ErrOutput, 'AVISO: ', sAviso);
end;

// El SQL de SHOW CREATE que lee del esquema temporal no puede nombrarlo: en
// el destino ese esquema ya no existirá.
function QuitarEsquemaModelo(const AScript, AEsquema: string;
  var AAvisos: TArray<string>): string;
begin
  Result := AScript;
  if AEsquema <> '' then
  begin
    Result := StringReplace(Result, '`' + AEsquema + '`.', '',
      [rfReplaceAll, rfIgnoreCase]);
    if ContainsText(Result, AEsquema) then
      AAvisos := AAvisos + ['El script todavía nombra el esquema temporal ' +
        AEsquema + ': revisar antes de aplicarlo'];
  end;
end;

// Compara, deja el script en el dialecto del destino y lo escribe.
function CompararYEmitir(ASourceConn: TUniConnection;
  const ASourceDB: string;
  ATargetConn: TUniConnection;
  const ATargetDB: string;
  const AEsquemaModelo: string;
  AOptions: TComparerOptions;
  const AAvisosPrevios: TArray<string>): Integer;
var
  aAvisos: TArray<string>;
  Conversion: TResultadoConversionDestino;
  Engine: TDBComparerEngine;
  SourceProvider, TargetProvider: IDBMetadataProvider;
  Writer: IScriptWriter;
  sScript: string;
begin
  SourceProvider := TMySQLMetadataProvider.Create(ASourceConn, ASourceDB,
    AOptions.Criterios);
  TargetProvider := TMySQLMetadataProvider.Create(ATargetConn, ATargetDB,
    AOptions.Criterios);
  Writer := TStringListScriptWriter.Create;
  Engine := TDBComparerEngine.Create(SourceProvider, TargetProvider, Writer,
    TMySQLHelpers.Create(AOptions.Criterios), AOptions);
  try
    Engine.GenerateScript;
  finally
    Engine.Free;
  end;
  Conversion := ConvertirScriptComparacion(Writer.GetScript,
    AOptions.Criterios);
  aAvisos := AAvisosPrevios + Conversion.Avisos;
  sScript := QuitarEsquemaModelo(Conversion.Texto, AEsquemaModelo, aAvisos);
  WriteScriptText(sScript, AOptions);
  InformarAvisos(aAvisos);
  if Length(aAvisos) > 0 then
    Result := cSalidaConAvisos
  else
    Result := 0;
end;

procedure AnunciarDestino(AOptions: TComparerOptions;
  const AVersion: string);
begin
  // Por la salida de error: sin --output, el script va por la estándar.
  Writeln(ErrOutput, 'DESTINO=', NombreDialecto(AOptions.Dialecto), ' (',
    DescripcionDialecto(AOptions.Dialecto), '; servidor ', AVersion, ')');
end;

// DBComparer origen credenciales destino credenciales [opciones]
function EjecutarComparacion: Integer;
var
  Options: TComparerOptions;
  SourceConfig, TargetConfig: TConnectionConfig;
  SourceConn, TargetConn: TUniConnection;
  sVersion: string;
begin
  if ParamCount < 4 then
    ShowUsage;
  SourceConn := nil;
  TargetConn := nil;
  Options := TComparerOptions.ParseFromCLI;
  try
    SourceConfig := TConnectionConfig.Parse(ParamStr(1), ParamStr(2));
    TargetConfig := TConnectionConfig.Parse(ParamStr(3), ParamStr(4));
    SourceConn := CrearConexion(SourceConfig);
    TargetConn := CrearConexion(TargetConfig);
    TargetConn.Connect;
    sVersion := VersionServidor(TargetConn);
    Options.ResolverDialecto(sVersion);
    AnunciarDestino(Options, sVersion);
    Result := CompararYEmitir(SourceConn, SourceConfig.Database,
      TargetConn, TargetConfig.Database, '', Options, nil);
  finally
    Options.Free;
    SourceConn.Free;
    TargetConn.Free;
  end;
end;

// DBComparer --modelo=modelo.sql servidor:puerto\base usuario\clave [...]
function EjecutarModelo: Integer;
var
  Conversion: TResultadoConversionDestino;
  Modelo: TEsquemaModelo;
  Options: TComparerOptions;
  SourceConn, TargetConn: TUniConnection;
  TargetConfig: TConnectionConfig;
  sModelo, sVersion: string;
begin
  if ParamCount < 3 then
    ShowUsage;
  sModelo := Copy(ParamStr(1), Pos('=', ParamStr(1)) + 1, MaxInt);
  if not TFile.Exists(sModelo) then
    raise Exception.CreateFmt('No existe el modelo: %s', [sModelo]);
  SourceConn := nil;
  TargetConn := nil;
  Modelo := nil;
  Options := TComparerOptions.ParseFromCLI(4);
  try
    // El modelo no conoce lo que el cliente haya añadido por su cuenta.
    Options.NoDelete := True;
    TargetConfig := TConnectionConfig.Parse(ParamStr(2), ParamStr(3));
    TargetConn := CrearConexion(TargetConfig);
    TargetConn.Connect;
    sVersion := VersionServidor(TargetConn);
    Options.ResolverDialecto(sVersion);
    AnunciarDestino(Options, sVersion);
    Conversion := ConvertirVolcado(
      TFile.ReadAllText(sModelo, TEncoding.UTF8),
      Options.Criterios);
    Modelo := TEsquemaModelo.Create(TargetConn, TargetConfig.Database);
    Modelo.Cargar(Conversion.Texto);
    Writeln(ErrOutput, 'MODELO=', Modelo.Esquema, ' (',
      Modelo.SentenciasCargadas, ' sentencias; ', Modelo.FilasOmitidas,
      ' omitidas)');
    SourceConn := CrearConexion(TargetConfig);
    Result := CompararYEmitir(SourceConn, Modelo.Esquema,
      TargetConn, TargetConfig.Database, Modelo.Esquema, Options,
      Conversion.Avisos);
  finally
    // Primero se suelta la conexión que mira el temporal; después se borra.
    FreeAndNil(SourceConn);
    Modelo.Free;
    Options.Free;
    TargetConn.Free;
  end;
end;

// DBComparer --convertir origen.sql destino.sql --destino=... [--encoding=]
// DBComparer --mysql841 origen.sql destino.sql [--encoding=] (anterior)
function EjecutarConversion: Integer;
var
  Conversion: TResultadoConversionDestino;
  Options: TComparerOptions;
  oCodificacion: TEncoding;
  bPropia: Boolean;
begin
  if ParamCount < 3 then
    ShowUsage;
  Options := TComparerOptions.ParseFromCLI(4);
  try
    if MatchText(ParamStr(1), ['--mysql841', '-mysql841']) then
      Options.Dialecto := ddMySQL841;
    if Options.Dialecto = ddAuto then
      raise Exception.Create('--convertir necesita --destino=mariadb12, ' +
        'mariadb10 o mysql841: sin servidor no hay nada que detectar.');
    Options.ResolverDialecto('');
    Conversion := ConvertirVolcado(
      TFile.ReadAllText(ParamStr(2), TEncoding.UTF8),
      Options.Criterios);
    oCodificacion := EncodingFromName(Options.OutputEncoding, bPropia);
    try
      TFile.WriteAllText(ParamStr(3), Conversion.Texto, oCodificacion);
    finally
      if bPropia then
        FreeAndNil(oCodificacion);
    end;
    Writeln(Format(TRes.MsgOutputSaved, [ParamStr(3)]));
    Writeln(Conversion.Resumen);
    if Length(Conversion.Avisos) > 0 then
      Result := cSalidaConAvisos
    else
      Result := 0;
  finally
    Options.Free;
  end;
end;

begin
  try
    FormatSettings := TFormatSettings.Create('en-US');
    Set8087CW($133F);
    Randomize;
    if MatchText(ParamStr(1), ['--mysql841', '-mysql841', '--convertir']) then
      ExitCode := EjecutarConversion
    else if StartsText('--modelo=', ParamStr(1)) then
      ExitCode := EjecutarModelo
    else
      ExitCode := EjecutarComparacion;
  except
    on E: Exception do
    begin
      Writeln(ErrOutput, 'ERROR: ', E.Message);
      ExitCode := 1;
    end;
  end;
end.
