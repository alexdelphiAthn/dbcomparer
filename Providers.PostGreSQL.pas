unit Providers.PostgreSQL;

interface

uses
  System.Classes, System.SysUtils, Data.DB, Core.Interfaces, Core.Types, Uni,
  PostgreSQLUniProvider, System.StrUtils, System.Generics.Collections,
  Providers.PostgreSQL.Helpers, Core.Helpers;

type
  TPostgreSQLMetadataProvider = class(TInterfacedObject, IDBMetadataProvider)
  private
    FConn: TUniConnection;
    FDBName: string;
    FSchema: string;
  public
    constructor Create(Conn: TUniConnection; const DBName: string; const Schema: string = 'public');
    destructor Destroy; override;
    // Implementación de la interfaz
    function GetTables: TStringList;
    function GetTableStructure(const TableName: string): TTableInfo;
    function GetTableIndexes(const TableName: string): TArray<TIndexInfo>;
    function GetTriggers: TArray<TTriggerInfo>;
    function GetTriggerDefinition(const TriggerName: string): string;
    function GetViews: TStringList;
    function GetViewDefinition(const ViewName: string): string;
    function GetProcedures: TStringList;
    function GetFunctions: TStringList;
    function GetProcedureDefinition(const ProcedureName: string): string;
    function GetFunctionDefinition(const FunctionName: string): string;
    function GetData(const TableName: string; const Filter: string = ''): TDataSet;
  private
    function QuoteIdentifier(const Identifier: string): string;
  end;

implementation

{ TPostgreSQLMetadataProvider }

function TPostgreSQLMetadataProvider.QuoteIdentifier(const Identifier: string): string;
begin
  Result := '"' + StringReplace(Identifier, '"', '""', [rfReplaceAll]) + '"';
end;

function TPostgreSQLMetadataProvider.GetData(const TableName: string; 
  const Filter: string = ''): TDataSet;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  Query.Connection := FConn;
  Query.SQL.Text := 'SELECT * FROM ' + QuoteIdentifier(FSchema) + '.' + 
                    QuoteIdentifier(TableName);
  if Filter <> '' then
    Query.SQL.Add('WHERE ' + Filter);
  Query.Open;
  Result := Query;
end;

function TPostgreSQLMetadataProvider.GetFunctionDefinition(
  const FunctionName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT pg_get_functiondef(p.oid) AS definition ' +
      '  FROM pg_proc p ' +
      ' INNER JOIN pg_namespace n ON p.pronamespace = n.oid ' +
      ' WHERE n.nspname = ' + QuotedStr(FSchema) +
      '   AND p.proname = ' + QuotedStr(FunctionName);
    Query.Open;
    if not Query.Eof then
      Result := Query.FieldByName('definition').AsString
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TPostgreSQLMetadataProvider.GetFunctions: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT p.proname AS function_name ' +
      '  FROM pg_proc p ' +
      ' INNER JOIN pg_namespace n ON p.pronamespace = n.oid ' +
      ' WHERE n.nspname = ' + QuotedStr(FSchema) +
      '   AND p.prokind = ''f'' ' + // 'f' = function (not procedure)
      'ORDER BY p.proname';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('function_name').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

constructor TPostgreSQLMetadataProvider.Create(Conn: TUniConnection;
  const DBName: string; const Schema: string = 'public');
begin
  FDBName := DBName;
  FSchema := Schema;
  FConn := Conn;
  FConn.ProviderName := 'PostgreSQL';
  FConn.Database := FDBName;
  FConn.Connected := True;
end;

destructor TPostgreSQLMetadataProvider.Destroy;
begin
  inherited;
end;

function TPostgreSQLMetadataProvider.GetProcedureDefinition(
  const ProcedureName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT pg_get_functiondef(p.oid) AS definition ' +
      '  FROM pg_proc p ' +
      ' INNER JOIN pg_namespace n ON p.pronamespace = n.oid ' +
      ' WHERE n.nspname = ' + QuotedStr(FSchema) +
      '   AND p.proname = ' + QuotedStr(ProcedureName) +
      '   AND p.prokind = ''p'''; // 'p' = procedure (PostgreSQL 11+)
    Query.Open;
    if not Query.Eof then
      Result := Query.FieldByName('definition').AsString
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TPostgreSQLMetadataProvider.GetProcedures: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT p.proname AS procedure_name ' +
      '  FROM pg_proc p ' +
      ' INNER JOIN pg_namespace n ON p.pronamespace = n.oid ' +
      ' WHERE n.nspname = ' + QuotedStr(FSchema) +
      '   AND p.prokind = ''p'' ' + // 'p' = procedure
      'ORDER BY p.proname';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('procedure_name').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TPostgreSQLMetadataProvider.GetTableIndexes(
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
      Query.SQL.Text :=
        'SELECT i.relname AS index_name, ' +
        '       ix.indisunique AS is_unique, ' +
        '       ix.indisprimary AS is_primary, ' +
        '       a.attname AS column_name, ' +
        '       array_position(ix.indkey, a.attnum) AS seq_in_index ' +
        '  FROM pg_index ix ' +
        ' INNER JOIN pg_class i ON i.oid = ix.indexrelid ' +
        ' INNER JOIN pg_class t ON t.oid = ix.indrelid ' +
        ' INNER JOIN pg_namespace n ON t.relnamespace = n.oid ' +
        ' INNER JOIN pg_attribute a ON a.attrelid = t.oid ' +
        '                           AND a.attnum = ANY(ix.indkey) ' +
        ' WHERE n.nspname = ' + QuotedStr(FSchema) +
        '   AND t.relname = ' + QuotedStr(TableName) +
        'ORDER BY i.relname, array_position(ix.indkey, a.attnum)';
      Query.Open;
      
      LastIndexName := '';
      while not Query.Eof do
      begin
        if not SameText(Query.FieldByName('index_name').AsString, LastIndexName) then
        begin
          if not SameText(LastIndexName, '') then
          begin
            CurrentIndex.Columns := ColList.ToArray;
            IndexList.Add(CurrentIndex);
            ColList.Clear;
          end;
          
          LastIndexName := Query.FieldByName('index_name').AsString;
          CurrentIndex.IndexName := LastIndexName;
          CurrentIndex.IsPrimary := Query.FieldByName('is_primary').AsBoolean;
          CurrentIndex.IsUnique := Query.FieldByName('is_unique').AsBoolean;
        end;
        
        IndexCol.ColumnName := Query.FieldByName('column_name').AsString;
        IndexCol.SeqInIndex := Query.FieldByName('seq_in_index').AsInteger;
        ColList.Add(IndexCol);
        Query.Next;
      end;
      
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

function TPostgreSQLMetadataProvider.GetTables: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT tablename ' +
      '  FROM pg_tables ' +
      ' WHERE schemaname = ' + QuotedStr(FSchema) +
      'ORDER BY tablename';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('tablename').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TPostgreSQLMetadataProvider.GetTableStructure(
  const TableName: string): TTableInfo;
var
  Query: TUniQuery;
  Col: TColumnInfo;
begin
  Result := TTableInfo.Create;
  Result.TableName := TableName;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text :=
      'SELECT c.column_name, ' +
      '       CASE ' +
      '         WHEN c.data_type = ''character varying'' THEN ' +
      '           ''varchar('' || c.character_maximum_length || '')'' ' +
      '         WHEN c.data_type = ''character'' THEN ' +
      '           ''char('' || c.character_maximum_length || '')'' ' +
      '         WHEN c.data_type = ''numeric'' AND c.numeric_precision IS NOT NULL THEN ' +
      '           ''numeric('' || c.numeric_precision || '','' || c.numeric_scale || '')'' ' +
      '         ELSE c.data_type ' +
      '       END AS data_type, ' +
      '       c.is_nullable, ' +
      '       CASE WHEN pk.column_name IS NOT NULL THEN ''PRI'' ELSE '''' END AS column_key, ' +
      '       CASE WHEN c.column_default LIKE ''nextval%'' THEN ''auto_increment'' ELSE '''' END AS extra, ' +
      '       c.column_default, ' +
      '       CAST(c.character_maximum_length AS VARCHAR) AS character_maximum_length, ' +
      '       '''' AS column_comment ' +
      '  FROM information_schema.columns c ' +
      '  LEFT JOIN ( ' +
      '    SELECT ku.column_name ' +
      '      FROM information_schema.table_constraints tc ' +
      '     INNER JOIN information_schema.key_column_usage ku ' +
      '             ON tc.constraint_name = ku.constraint_name ' +
      '            AND tc.table_schema = ku.table_schema ' +
      '     WHERE tc.constraint_type = ''PRIMARY KEY'' ' +
      '       AND tc.table_schema = ' + QuotedStr(FSchema) +
      '       AND tc.table_name = ' + QuotedStr(TableName) +
      '  ) pk ON c.column_name = pk.column_name ' +
      ' WHERE c.table_schema = ' + QuotedStr(FSchema) +
      '   AND c.table_name = ' + QuotedStr(TableName) +
      'ORDER BY c.ordinal_position';
    Query.Open;
    
    while not Query.Eof do
    begin
      Col.ColumnName := Query.FieldByName('column_name').AsString;
      Col.DataType := Query.FieldByName('data_type').AsString;
      Col.IsNullable := Query.FieldByName('is_nullable').AsString;
      Col.ColumnKey := Query.FieldByName('column_key').AsString;
      Col.Extra := Query.FieldByName('extra').AsString;
      
      if not Query.FieldByName('column_default').IsNull then
        Col.ColumnDefault := Query.FieldByName('column_default').AsString
      else
        Col.ColumnDefault := '';
        
      if not Query.FieldByName('character_maximum_length').IsNull then
        Col.CharMaxLength := Query.FieldByName('character_maximum_length').AsString
      else
        Col.CharMaxLength := '';
        
      Col.ColumnComment := ''; // PostgreSQL no guarda comentarios en information_schema
        
      Result.Columns.Add(Col);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TPostgreSQLMetadataProvider.GetTriggerDefinition(
  const TriggerName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT pg_get_triggerdef(t.oid) AS definition ' +
      '  FROM pg_trigger t ' +
      ' INNER JOIN pg_class c ON t.tgrelid = c.oid ' +
      ' INNER JOIN pg_namespace n ON c.relnamespace = n.oid ' +
      ' WHERE n.nspname = ' + QuotedStr(FSchema) +
      '   AND t.tgname = ' + QuotedStr(TriggerName);
    Query.Open;
    if not Query.Eof then
      Result := Query.FieldByName('definition').AsString
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TPostgreSQLMetadataProvider.GetTriggers: TArray<TTriggerInfo>;
var
  Query: TUniQuery;
  TriggerList: TList<TTriggerInfo>;
  Trigger: TTriggerInfo;
begin
  TriggerList := TList<TTriggerInfo>.Create;
  try
    Query := TUniQuery.Create(nil);
    try
      Query.Connection := FConn;
      Query.SQL.Text :=
        'SELECT t.tgname AS trigger_name, ' +
        '       CASE ' +
        '         WHEN (t.tgtype & 4) <> 0 THEN ''INSERT'' ' +
        '         WHEN (t.tgtype & 8) <> 0 THEN ''DELETE'' ' +
        '         WHEN (t.tgtype & 16) <> 0 THEN ''UPDATE'' ' +
        '       END AS event_manipulation, ' +
        '       CASE ' +
        '         WHEN (t.tgtype & 2) <> 0 THEN ''BEFORE'' ' +
        '         WHEN (t.tgtype & 64) <> 0 THEN ''INSTEAD OF'' ' +
        '         ELSE ''AFTER'' ' +
        '       END AS action_timing, ' +
        '       pg_get_triggerdef(t.oid) AS action_statement, ' +
        '       c.relname AS event_object_table ' +
        '  FROM pg_trigger t ' +
        ' INNER JOIN pg_class c ON t.tgrelid = c.oid ' +
        ' INNER JOIN pg_namespace n ON c.relnamespace = n.oid ' +
        ' WHERE n.nspname = ' + QuotedStr(FSchema) +
        '   AND NOT t.tgisinternal ' +
        'ORDER BY c.relname, t.tgname';
      Query.Open;
      
      while not Query.Eof do
      begin
        Trigger.TriggerName := Query.FieldByName('trigger_name').AsString;
        Trigger.EventManipulation := Query.FieldByName('event_manipulation').AsString;
        Trigger.ActionTiming := Query.FieldByName('action_timing').AsString;
        Trigger.ActionStatement := Query.FieldByName('action_statement').AsString;
        Trigger.EventObjectTable := Query.FieldByName('event_object_table').AsString;
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

function TPostgreSQLMetadataProvider.GetViewDefinition(
  const ViewName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT pg_get_viewdef(' + QuotedStr(FSchema + '.' + ViewName) + ') AS definition';
    Query.Open;
    if not Query.Eof then
      Result := 'CREATE VIEW ' + QuoteIdentifier(ViewName) + ' AS ' + 
                Query.FieldByName('definition').AsString
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TPostgreSQLMetadataProvider.GetViews: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT viewname ' +
      '  FROM pg_views ' +
      ' WHERE schemaname = ' + QuotedStr(FSchema) +
      'ORDER BY viewname';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('viewname').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

end.