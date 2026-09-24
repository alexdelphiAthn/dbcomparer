unit Core.Types;

interface

uses System.Classes, Generics.Collections, system.SysUtils, System.StrUtils,
  Core.Dialecto;

type
  TColumnInfo = record
    ColumnName: string;
    DataType: string;
    IsNullable: string;
    ColumnKey: string;
    Extra: string;
    GenerationExpression: string;
    OrdinalPosition: Integer;
    PreviousColumnName: string;
    ColumnDefault: string;
    CharMaxLength: string;
    ColumnComment: string;
  end;

  TIndexColumn = record
    ColumnName: string;
    SeqInIndex: Integer;
  end;

  TIndexInfo = record
    IndexName: string;
    IsUnique: Boolean;
    IsPrimary: Boolean;
    Columns: TArray<TIndexColumn>;
  end;

  TCheckConstraintInfo = record
    ConstraintName: string;
    CheckClause: string;
  end;

  TTriggerInfo = record
    TriggerName: string;
    EventManipulation: string;
    ActionTiming: string;
    ActionStatement: string;
    EventObjectTable: string;
  end;

  TTableInfo = class
    TableName: string;
    Engine: string;
    TableCollation: string;
    Columns: TList<TColumnInfo>;
    constructor Create;
    destructor Destroy; override;
  end;

  TConnectionConfig = record
    Server: string;
    Port: Integer;
    Database: string;
    Username: string;
    Password: string;

    // Método estático para convertir los argumentos de consola en configuración
    class function Parse(const ConnStr, CredStr: string): TConnectionConfig; static;
  end;

  TComparerOptions = class
  public
    NoDelete: Boolean;
    WithTriggers: Boolean;
    WithData: Boolean;
    WithDataDiff: Boolean;
    ExcludeTables: TStringList;
    IncludeTables: TStringList;
    PreserveViews: TStringList;
    ExtendedInsert: Boolean;
    ExtendedInsertRows: Integer;
    // Lo pedido en la línea de órdenes (puede ser ddAuto) y los criterios
    // ya resueltos contra el servidor de destino: todo el SQL generado
    // pregunta a Criterios, nunca al nombre del dialecto.
    Dialecto: TDialectoDestino;
    Criterios: TCriteriosDialecto;

    OutputFile: string;
    OutputEncoding: string;

    // APrimero: el primer parámetro que ya no es de conexión.
    class function ParseFromCLI(
      APrimero: Integer = 5): TComparerOptions;
    // Con ddAuto, el dialecto sale de SELECT VERSION() del destino.
    procedure ResolverDialecto(const AVersionDestino: string);

    constructor Create;
    destructor Destroy; override;

  end;

implementation

{ TTableInfo }
constructor TTableInfo.Create;
begin
  Columns := TList<TColumnInfo>.Create;
end;

destructor TTableInfo.Destroy;
begin
  Columns.Free;
  inherited;
end;

{ TComparerOptions }

constructor TComparerOptions.Create;
begin
  ExcludeTables := TStringList.Create;
  ExcludeTables.CaseSensitive := False;
  IncludeTables := TStringList.Create;
  IncludeTables.CaseSensitive := False;
  PreserveViews := TStringList.Create;
  PreserveViews.CaseSensitive := False;
end;

destructor TComparerOptions.Destroy;
begin
  ExcludeTables.Free;
  IncludeTables.Free;
  PreserveViews.Free;
  inherited;
end;

class function TComparerOptions.ParseFromCLI(
  APrimero: Integer): TComparerOptions;
var
  i: Integer;
  Param, Value: string;
begin
  Result := TComparerOptions.Create;
  Result.ExtendedInsert := True;
  Result.ExtendedInsertRows := 500;
  Result.OutputEncoding := 'utf8bom';
  // Por defecto desde 5: 1..4 son las dos conexiones
  for i := APrimero to ParamCount do
  begin
    Param := LowerCase(ParamStr(i));
    if Param = '--nodelete' then
      Result.NoDelete := True
    else if Param = '--with-triggers' then
      Result.WithTriggers := True
    else if Param = '--mariadb10' then
      Result.Dialecto := ddMariaDB10
    else if Param = '--mysql841' then
      Result.Dialecto := ddMySQL841
    else if StartsText('--destino=', Param) then
      Result.Dialecto := DialectoDesdeNombre(
        Copy(Param, Length('--destino=') + 1, MaxInt))
    else if Param = '--with-data' then
      Result.WithData := True
    else if Param = '--with-data-diff' then
      Result.WithDataDiff := True
    else if StartsText('--exclude-tables=', Param) then
    begin
      Value := Copy(ParamStr(i), Length('--exclude-tables=') + 1, MaxInt);
      Result.ExcludeTables.CommaText := Value;
    end
    else if StartsText('--include-tables=', Param) then
    begin
      Value := Copy(ParamStr(i), Length('--include-tables=') + 1, MaxInt);
      Result.IncludeTables.CommaText := Value;
    end
    else if StartsText('--preserve-views=', Param) then
    begin
      Value := Copy(ParamStr(i), Length('--preserve-views=') + 1, MaxInt);
      Result.PreserveViews.CommaText := Value;
    end
    else if StartsText('--output=', Param) then
    begin
      Result.OutputFile := Copy(ParamStr(i), Length('--output=') + 1, MaxInt);
    end
    else if StartsText('--encoding=', Param) then
    begin
      Result.OutputEncoding := LowerCase(Copy(ParamStr(i),
                                         Length('--encoding=') + 1, MaxInt));
    end;
  end;
  Result.Criterios := TCriteriosDialecto.Para(Result.Dialecto);
  // Validación básica
  if Result.WithData and Result.WithDataDiff then
  begin
    Result.Free;
    raise Exception.Create('Error: No puedes usar --with-data y ' +
                           '--with-data-diff a la vez.');
  end;
  if not MatchText(Result.OutputEncoding,
    ['utf8bom', 'utf8nobom', 'ansi', 'unicode']) then
  begin
    Value := Result.OutputEncoding;
    Result.Free;
    raise Exception.CreateFmt(
      'Error: Codificación de salida no válida: "%s". ' +
      'Use utf8bom, utf8nobom, ansi o unicode.', [Value]);
  end;
end;

procedure TComparerOptions.ResolverDialecto(const AVersionDestino: string);
begin
  if Dialecto = ddAuto then
    Dialecto := DialectoDesdeVersion(AVersionDestino);
  Criterios := TCriteriosDialecto.Para(Dialecto);
end;

{ TConnectionConfig }

class function TConnectionConfig.Parse(const ConnStr, CredStr: string): TConnectionConfig;
var
  PartsConn, PartsServer: TArray<string>;
  SeparatorPos: Integer;
begin
  // 1. Parsear "Servidor:Puerto\BaseDeDatos"
  PartsConn := ConnStr.Split(['\']);
  if Length(PartsConn) <> 2 then
    raise Exception.CreateFmt('Formato de conexión incorrecto: "%s". '+
                                    'Use: servidor:puerto\database', [ConnStr]);
  Result.Database := PartsConn[1];
  // Separar Servidor y Puerto
  PartsServer := PartsConn[0].Split([':']);
  if Length(PartsServer) = 2 then
  begin
    Result.Server := PartsServer[0];
    Result.Port := StrToIntDef(PartsServer[1], 3306);
  end
  else
  begin
    Result.Server := PartsConn[0];
    Result.Port := 3306; // Puerto por defecto MySQL
  end;
  // 2. Parsear "Usuario\Password". Se corta en la primera barra: la
  // contraseña puede llevar otras. Con «*» se toma de la variable de
  // entorno DBCOMPARER_PASSWORD, para no dejarla a la vista en la lista
  // de procesos.
  SeparatorPos := Pos('\', CredStr);
  if SeparatorPos = 0 then
    raise Exception.CreateFmt('Formato de credenciales incorrecto: "%s".'+
                              ' Use: usuario\password', [CredStr]);
  Result.Username := Copy(CredStr, 1, SeparatorPos - 1);
  Result.Password := Copy(CredStr, SeparatorPos + 1, MaxInt);
  if Result.Password = '*' then
    Result.Password := GetEnvironmentVariable('DBCOMPARER_PASSWORD');
end;

end.
