unit Providers.MySQL;

interface

uses
  System.Classes, System.SysUtils, Data.DB, Core.Interfaces, Core.Types, Uni,
  MySQLUniProvider, system.StrUtils, System.Generics.Collections,
  Providers.MySQL.Helpers, Core.Helpers;

const
    SCHEMADB = 'information_schema';
type
  IMySQLRoutineMetadataProvider = interface
    ['{A5847739-67C3-427C-BE45-EE337BBF6767}']
    function GetRoutineSQLMode(const RoutineName,
      RoutineType: string): string;
  end;

  TMySQLMetadataProvider = class(TInterfacedObject, IDBMetadataProvider,
    IMySQLRoutineMetadataProvider, ICheckConstraintMetadataProvider)
  private
    FConn: TUniConnection;
    FDBName: string;
    FMariaDB10Compat: Boolean;
  public
    constructor Create(Conn: TUniConnection; const DBName: string;
      const MariaDB10Compat: Boolean = False);
    destructor Destroy; override;
    // Implementación de la interfaz
    function GetTables: TStringList;
    function GetTableStructure(const TableName: string): TTableInfo;
    function GetTableIndexes(const TableName: string): TArray<TIndexInfo>;
    function GetTableCheckConstraints(
      const TableName: string): TArray<TCheckConstraintInfo>;
    function GetTriggers: TArray<TTriggerInfo>;
    function GetTriggerDefinition(const TriggerName: string): string;
    function GetViews:TStringList;
    function GetViewDefinition(const ViewName:string):string;
    function GetProcedures:TStringList;
    function GetFunctions:TStringList;
    function GetSequences:TSTringList;
    function GetProcedureDefinition(const ProcedureName:string):string;
    function GetFunctionDefinition(const FunctionName:string):string;
    function GetRoutineSQLMode(const RoutineName,
      RoutineType: string): string;
    function GetData(const TableName: string; const Filter: string = ''): TDataSet;
  private
    function StripDefiner(const SQL: string): string;
    function NormalizeMariaDB10SQL(const SQL: string): string;
  end;

implementation

{ TMySQLMetadataProvider }

function TMySQLMetadataProvider.GetSequences: TStringList;
begin
  // MySQL no tiene secuencias independientes, devolvemos lista vacía.
  Result := TStringList.Create;
end;

function TMySQLMetadataProvider.GetData(const TableName: string;
                                        const Filter: string = ''): TDataSet;
var
  Query: TUniQuery;
  function QuoteIdentifier(const Identifier: string): string;
  begin
    Result := '`' + Identifier + '`';
  end;
begin
  Query := TUniQuery.Create(nil);
  Query.Connection := FConn;
  FConn.Database := FDBName;
  Query.SQL.Text := 'SELECT * FROM ' + QuoteIdentifier(TableName);
  if Filter <> '' then
    Query.SQL.Add('WHERE ' + Filter);
  Query.Open;
  Result := Query;
end;

function TMySQLMetadataProvider.GetFunctionDefinition(
  const FunctionName: string): string;
var
  Query: TUniQuery;
  OldDB: string;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.Connection.Database := FDBName;
    Query.SQL.Text := 'SHOW CREATE FUNCTION `' + FunctionName + '`';
    Query.Open;
    Result := Query.Fields[2].AsString;
    Result := StripDefiner(Result);
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.GetFunctions: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    FConn.Database := SCHEMADB;
    Query.SQL.Text := '   SELECT ROUTINE_NAME '+
                      '     FROM INFORMATION_SCHEMA.ROUTINES ' +
                      '    WHERE ROUTINE_SCHEMA = ' + QuotedStr(FDBName) +
                      '      AND ROUTINE_TYPE = '+ QuotedStr('FUNCTION') +
                      ' ORDER BY ROUTINE_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('ROUTINE_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

constructor TMySQLMetadataProvider.Create(Conn: TUniConnection;
  const DBName: string; const MariaDB10Compat: Boolean);
begin
//  FConn := TUniConnection.Create(nil);
  FDBName := DBName;
  FMariaDB10Compat := MariaDB10Compat;
  Fconn := Conn;
  FConn.ProviderName := 'MySQL';
  FConn.Connected := True;
end;

destructor TMySQLMetadataProvider.Destroy;
begin
  //Fconn.Free;
  inherited;
end;

function TMySQLMetadataProvider.GetProcedureDefinition(
  const ProcedureName:string): string;
var
  Query: TUniQuery;
  OldDB: string;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.Connection.Database := FDBName;
    Query.SQL.Text := 'SHOW CREATE PROCEDURE `' + ProcedureName + '`';
    Query.Open;
    Result := Query.Fields[2].AsString;
    Result := StripDefiner(Result);
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.GetProcedures: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    FConn.Database := SCHEMADB;
    Query.SQL.Text := '   SELECT ROUTINE_NAME '+
                      '     FROM INFORMATION_SCHEMA.ROUTINES ' +
                      '    WHERE ROUTINE_SCHEMA = ' + QuotedStr(FDBName) +
                      '      AND ROUTINE_TYPE = '+ QuotedStr('PROCEDURE') +
                      ' ORDER BY ROUTINE_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('ROUTINE_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.GetTableIndexes(
  const TableName: string): TArray<TIndexInfo>;
var
  Query: TUniQuery;
  IndexList: TList<TIndexInfo>;
  CurrentIndex: TIndexInfo;
  LastIndexName: string;
  ColList: TList<TIndexColumn>;
  IndexCol: TIndexColumn;
begin
  IndexList := TList<TIndexInfo>.Create;
  ColList := TList<TIndexColumn>.Create;
  try
    Query := TUniQuery.Create(nil);
    try
      Query.Connection := FConn;
      FConn.Database := SCHEMADB;
      Query.SQL.Text :=
        'SELECT INDEX_NAME, ' +
        '       NON_UNIQUE, ' +
        '       COLUMN_NAME, ' +
        '       SEQ_IN_INDEX ' +
        '  FROM INFORMATION_SCHEMA.STATISTICS ' +
        ' WHERE TABLE_SCHEMA = ' + QuotedStr(FDBName) +
        '   AND TABLE_NAME = ' + QuotedStr(TableName) + ' ' +
        'ORDER BY INDEX_NAME, SEQ_IN_INDEX';
      Query.Open;
      LastIndexName := '';
      while not Query.Eof do
      begin
        // Nuevo índice detectado
        if not SameText(Query.FieldByName('INDEX_NAME').AsString, LastIndexName) then
        begin
          // Guardar el índice anterior si existe
          if not SameText(LastIndexName, '') then
          begin
            CurrentIndex.Columns := ColList.ToArray;
            IndexList.Add(CurrentIndex);
            ColList.Clear;
          end;
          // Iniciar nuevo índice
          LastIndexName := Query.FieldByName('INDEX_NAME').AsString;
          CurrentIndex.IndexName := LastIndexName;
          CurrentIndex.IsPrimary := SameText(LastIndexName, 'PRIMARY');
          CurrentIndex.IsUnique := (Query.FieldByName('NON_UNIQUE').AsInteger = 0);
        end;
        // Agregar columna al índice actual
        IndexCol.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
        IndexCol.SeqInIndex := Query.FieldByName('SEQ_IN_INDEX').AsInteger;
        ColList.Add(IndexCol);
        Query.Next;
      end;
      // Guardar el último índice
      if not SameText(LastIndexName, '') then
      begin
        CurrentIndex.Columns := ColList.ToArray;
        IndexList.Add(CurrentIndex);
      end;
    finally
      Query.Free;
    end;
    Result := IndexList.ToArray;
  finally
    IndexList.Free;
    ColList.Free;
  end;
end;

function TMySQLMetadataProvider.GetTableCheckConstraints(
  const TableName: string): TArray<TCheckConstraintInfo>;
var
  Query: TUniQuery;
  CheckList: TList<TCheckConstraintInfo>;
  CheckConstraint: TCheckConstraintInfo;
  HasTableName: Boolean;
begin
  SetLength(Result, 0);
  CheckList := TList<TCheckConstraintInfo>.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text :=
      'SELECT COUNT(*) AS TABLE_COUNT ' +
      '  FROM INFORMATION_SCHEMA.TABLES ' +
      ' WHERE TABLE_SCHEMA = ''information_schema'' ' +
      '   AND UPPER(TABLE_NAME) = ''CHECK_CONSTRAINTS''';
    Query.Open;
    if Query.FieldByName('TABLE_COUNT').AsInteger = 0 then
      Exit;

    Query.Close;
    Query.SQL.Text :=
      'SELECT COUNT(*) AS COLUMN_COUNT ' +
      '  FROM INFORMATION_SCHEMA.COLUMNS ' +
      ' WHERE TABLE_SCHEMA = ''information_schema'' ' +
      '   AND UPPER(TABLE_NAME) = ''CHECK_CONSTRAINTS'' ' +
      '   AND UPPER(COLUMN_NAME) = ''TABLE_NAME''';
    Query.Open;
    HasTableName := Query.FieldByName('COLUMN_COUNT').AsInteger > 0;

    Query.Close;
    Query.SQL.Text :=
      'SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE ' +
      '  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ' +
      '  JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc ' +
      '    ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA ' +
      '   AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME ';
    if HasTableName then
      Query.SQL.Add('   AND cc.TABLE_NAME = tc.TABLE_NAME ');
    Query.SQL.Add(
      ' WHERE tc.TABLE_SCHEMA = :DbName ' +
      '   AND tc.TABLE_NAME = :TbName ' +
      '   AND tc.CONSTRAINT_TYPE = ''CHECK'' ' +
      ' ORDER BY tc.CONSTRAINT_NAME');
    Query.ParamByName('DbName').AsString := FDBName;
    Query.ParamByName('TbName').AsString := TableName;
    Query.Open;
    while not Query.Eof do
    begin
      CheckConstraint := Default(TCheckConstraintInfo);
      CheckConstraint.ConstraintName :=
        Query.FieldByName('CONSTRAINT_NAME').AsString;
      CheckConstraint.CheckClause :=
        Query.FieldByName('CHECK_CLAUSE').AsString;
      CheckList.Add(CheckConstraint);
      Query.Next;
    end;
    Result := CheckList.ToArray;
  finally
    Query.Free;
    CheckList.Free;
  end;
end;

function TMySQLMetadataProvider.GetTables: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ' +
                      'WHERE TABLE_SCHEMA = ' + QuotedStr(FDBName) +
                      '  AND TABLE_TYPE = ''BASE TABLE'' ' +
                      'ORDER BY TABLE_NAME';
    Query.Open;
    while (not(Query.Eof)) do
    begin
      Result.Add(Query.FieldByName('TABLE_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.GetTableStructure(const TableName: string): TTableInfo;
var
  Query: TUniQuery;
  Col: TColumnInfo;
  PreviousColumnName: string;
begin
  // 1. Inicializamos el resultado y la consulta
  Result := TTableInfo.Create;
  Result.TableName := TableName;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    // 2. Consulta a INFORMATION_SCHEMA optimizada y parametrizada
    Query.SQL.Text := 'SELECT COLUMN_NAME, ' +
                      '       COLUMN_TYPE, ' +
                      '       IS_NULLABLE, ' +
                      '       COLUMN_KEY, ' +
                      '       EXTRA, ' +
                      '       GENERATION_EXPRESSION, ' +
                      '       ORDINAL_POSITION, ' +
                      '       COLUMN_DEFAULT, ' +
                      '       CHARACTER_MAXIMUM_LENGTH, ' +
                      '       COLUMN_COMMENT ' +
                      '  FROM INFORMATION_SCHEMA.COLUMNS ' +
                      ' WHERE TABLE_SCHEMA = :DbName ' +
                      '   AND TABLE_NAME = :TbName ' +
                      ' ORDER BY ORDINAL_POSITION';
    // Asignamos los parámetros para seguridad y robustez
    Query.ParamByName('DbName').AsString := FDBName;
    Query.ParamByName('TbName').AsString := TableName;
    Query.Open;
    PreviousColumnName := '';
    // 3. Iteramos por las columnas encontradas
    while not Query.Eof do
    begin
      Col := Default(TColumnInfo);
      // IMPORTANTE: Si TColumnInfo es una CLASE, descomenta la línea de abajo.
      // Si es un RECORD, déjala comentada.
      // Col := TColumnInfo.Create;
      // Lectura de campos básicos
      Col.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
      Col.DataType   := Query.FieldByName('COLUMN_TYPE').AsString;
      Col.IsNullable := Query.FieldByName('IS_NULLABLE').AsString; // 'YES' o 'NO'
      Col.ColumnKey  := Query.FieldByName('COLUMN_KEY').AsString;  // 'PRI', 'UNI', etc.
      Col.Extra      := Query.FieldByName('EXTRA').AsString;       // 'auto_increment', etc.
      Col.GenerationExpression :=
        Query.FieldByName('GENERATION_EXPRESSION').AsString;
      Col.OrdinalPosition := Query.FieldByName('ORDINAL_POSITION').AsInteger;
      Col.PreviousColumnName := PreviousColumnName;
      // --- Lógica CRÍTICA para el Valor por Defecto (Solución Error 1067) ---
      if Query.FieldByName('COLUMN_DEFAULT').IsNull then
      begin
        // Si es nulo en la BD, usamos una marca especial interna.
        // NOTA: Tu generador de SQL debe saber que '<NULL>' significa "sin default".
        Col.ColumnDefault := '<NULL>';
      end
      else
      begin
        // Si tiene un valor real (incluso cadena vacía ''), lo tomamos tal cual.
        Col.ColumnDefault := Query.FieldByName('COLUMN_DEFAULT').AsString;
      end;
      // Manejo de longitud máxima
      if not Query.FieldByName('CHARACTER_MAXIMUM_LENGTH').IsNull then
        Col.CharMaxLength := Query.FieldByName('CHARACTER_MAXIMUM_LENGTH').AsString
      else
        Col.CharMaxLength := '0';
      // Manejo de comentarios
      if not Query.FieldByName('COLUMN_COMMENT').IsNull then
        Col.ColumnComment := Query.FieldByName('COLUMN_COMMENT').AsString
      else
        Col.ColumnComment := '';
      // Agregamos la columna a la lista
      Result.Columns.Add(Col);
      PreviousColumnName := Col.ColumnName;
      Query.Next;
    end;
    Query.Close;
    Query.SQL.Text :=
      'SELECT ENGINE, TABLE_COLLATION FROM INFORMATION_SCHEMA.TABLES ' +
      'WHERE TABLE_SCHEMA = :DbName AND TABLE_NAME = :TbName ' +
      'AND TABLE_TYPE = ''BASE TABLE''';
    Query.ParamByName('DbName').AsString := FDBName;
    Query.ParamByName('TbName').AsString := TableName;
    Query.Open;
    if not Query.Eof then
    begin
      Result.Engine := Query.FieldByName('ENGINE').AsString;
      Result.TableCollation := Query.FieldByName('TABLE_COLLATION').AsString;
    end;
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.GetTriggerDefinition(
  const TriggerName: string): string;
var
  Query: TUniQuery;
  OldDB: string;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    FConn.Database := FDBName;
    Query.SQL.Text := 'SHOW CREATE TRIGGER `' + TriggerName + '`';
    Query.Open;
    Result := Query.Fields[2].AsString;
    Result := StripDefiner(Result);
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.GetTriggers: TArray<TTriggerInfo>;
var
  Query: TUniQuery;
  TriggerList: TList<TTriggerInfo>;
  Trigger: TTriggerInfo;
begin
  TriggerList := TList<TTriggerInfo>.Create;
  try
    Query := TUniQuery.Create(nil);
    try
      FConn.Database := SCHEMADB;
      Query.Connection := FConn;
      // CORRECCIÓN: Concatenación y espacios
      Query.SQL.Text :=
        'SELECT TRIGGER_NAME, ' +
        '       EVENT_MANIPULATION, ' +
        '       ACTION_TIMING, ' +
        '       ACTION_STATEMENT, ' +
        '       EVENT_OBJECT_TABLE ' +
        '  FROM INFORMATION_SCHEMA.TRIGGERS ' +
        ' WHERE TRIGGER_SCHEMA = ' + QuotedStr(FDBName) + ' ' +
        'ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME';
      Query.Open;
      while not Query.Eof do
      begin
        Trigger.TriggerName := Query.FieldByName('TRIGGER_NAME').AsString;
        Trigger.EventManipulation :=
                               Query.FieldByName('EVENT_MANIPULATION').AsString;
        Trigger.ActionTiming := Query.FieldByName('ACTION_TIMING').AsString;
        Trigger.ActionStatement :=
                                 Query.FieldByName('ACTION_STATEMENT').AsString;
        Trigger.EventObjectTable :=
                               Query.FieldByName('EVENT_OBJECT_TABLE').AsString;
        TriggerList.Add(Trigger);
        Query.Next;
      end;
    finally
      Query.Free;
    end;
    Result := TriggerList.ToArray;
  finally
    TriggerList.Free;
  end;
end;

function TMySQLMetadataProvider.GetViewDefinition(
  const ViewName: string): string;
var
  Query: TUniQuery;
  OldDB: string;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    FConn.Database := FDBName;
    Query.SQL.Text := 'SHOW CREATE VIEW `' + ViewName + '`';
    Query.Open;
    Result := Query.Fields[1].AsString;
    Result := StripDefiner(Result) + ';';
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.GetViews: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Fconn.Database := SCHEMADB;
    Query.SQL.Text := 'SELECT TABLE_NAME' +
                      '  FROM INFORMATION_SCHEMA.VIEWS ' +
                      ' WHERE TABLE_SCHEMA = ' + QuotedStr(FDBName) + ' ' +
                      'ORDER BY TABLE_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('TABLE_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.StripDefiner(const SQL: string): string;
var
  PosDefiner, PosEnd, SQLLength: Integer;
  QuoteChar: Char;
  UpperSQL: string;
begin
  Result := SQL;
  UpperSQL := UpperCase(SQL);
  PosDefiner := Pos('DEFINER=', UpperSQL);
  if PosDefiner > 0 then
  begin
    // SHOW CREATE siempre devuelve el definidor como un único token
    // DEFINER=usuario@host. Se elimina sólo ese token: buscar palabras como
    // PROCEDURE o VIEW en toda la sentencia confunde literales o consultas
    // del cuerpo con el tipo real del objeto.
    SQLLength := Length(SQL);
    PosEnd := PosDefiner + Length('DEFINER=');
    QuoteChar := #0;
    while PosEnd <= SQLLength do
    begin
      if QuoteChar <> #0 then
      begin
        if (SQL[PosEnd] = '\') and (PosEnd < SQLLength) then
          Inc(PosEnd)
        else if SQL[PosEnd] = QuoteChar then
        begin
          // Los identificadores y literales escapan la comilla duplicándola.
          if (PosEnd < SQLLength) and (SQL[PosEnd + 1] = QuoteChar) then
            Inc(PosEnd)
          else
            QuoteChar := #0;
        end;
      end
      else if CharInSet(SQL[PosEnd], ['`', '''', '"']) then
        QuoteChar := SQL[PosEnd]
      else if CharInSet(SQL[PosEnd], [#9, #10, #13, ' ']) then
        Break;
      Inc(PosEnd);
    end;
    while (PosEnd <= SQLLength) and
          CharInSet(SQL[PosEnd], [#9, #10, #13, ' ']) do
      Inc(PosEnd);
    Result := TrimRight(Copy(SQL, 1, PosDefiner - 1)) + ' ' +
              TrimLeft(Copy(SQL, PosEnd, MaxInt));
  end;
  if FMariaDB10Compat then
    Result := NormalizeMariaDB10SQL(Result);
end;

function TMySQLMetadataProvider.GetRoutineSQLMode(const RoutineName,
  RoutineType: string): string;
var
  Query: TUniQuery;
begin
  Result := '';
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text :=
      'SELECT SQL_MODE FROM INFORMATION_SCHEMA.ROUTINES ' +
      'WHERE ROUTINE_SCHEMA = :DbName AND ROUTINE_NAME = :RoutineName ' +
      'AND ROUTINE_TYPE = :RoutineType';
    Query.ParamByName('DbName').AsString := FDBName;
    Query.ParamByName('RoutineName').AsString := RoutineName;
    Query.ParamByName('RoutineType').AsString := UpperCase(RoutineType);
    Query.Open;
    if not Query.Eof then
      Result := Query.FieldByName('SQL_MODE').AsString;
  finally
    Query.Free;
  end;
end;

function TMySQLMetadataProvider.NormalizeMariaDB10SQL(
  const SQL: string): string;
begin
  Result := NormalizeMariaDB10SQLText(SQL);
end;


end.

