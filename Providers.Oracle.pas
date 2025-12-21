unit Providers.Oracle;

interface

uses
  System.Classes, System.SysUtils, Data.DB, Core.Interfaces, Core.Types, Uni,
  OracleUniProvider, System.StrUtils, System.Generics.Collections,
  Providers.Oracle.Helpers, Core.Helpers;

type
  TOracleMetadataProvider = class(TInterfacedObject, IDBMetadataProvider)
  private
    FConn: TUniConnection;
    FDBName: string;
    FOwner: string;
  public
    constructor Create(Conn: TUniConnection; const DBName: string; const Owner: string = '');
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
    function GetSequences: TStringList;
    function GetProcedureDefinition(const ProcedureName: string): string;
    function GetFunctionDefinition(const FunctionName: string): string;
    function GetData(const TableName: string; const Filter: string = ''): TDataSet;
  private
    function QuoteIdentifier(const Identifier: string): string;
  end;

implementation

{ TOracleMetadataProvider }

function TOracleMetadataProvider.GetSequences: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT SEQUENCE_NAME ' +
      '  FROM ALL_SEQUENCES ' +
      ' WHERE SEQUENCE_OWNER = ' + QuotedStr(FOwner) +
      ' ORDER BY SEQUENCE_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('SEQUENCE_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TOracleMetadataProvider.QuoteIdentifier(const Identifier: string): string;
begin
  Result := '"' + Identifier + '"';
end;

function TOracleMetadataProvider.GetData(const TableName: string; 
  const Filter: string = ''): TDataSet;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  Query.Connection := FConn;
  Query.SQL.Text := 'SELECT * FROM ' + QuoteIdentifier(TableName);
  if Filter <> '' then
    Query.SQL.Add('WHERE ' + Filter);
  Query.Open;
  Result := Query;
end;

function TOracleMetadataProvider.GetFunctionDefinition(
  const FunctionName: string): string;
var
  Query: TUniQuery;
  SourceLines: TStringList;
begin
  Query := TUniQuery.Create(nil);
  SourceLines := TStringList.Create;
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TEXT ' +
      '  FROM ALL_SOURCE ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
      '   AND NAME = ' + QuotedStr(UpperCase(FunctionName)) +
      '   AND TYPE = ''FUNCTION'' ' +
      'ORDER BY LINE';
    Query.Open;
    
    while not Query.Eof do
    begin
      SourceLines.Add(Query.FieldByName('TEXT').AsString);
      Query.Next;
    end;
    
    Result := SourceLines.Text;
  finally
    SourceLines.Free;
    Query.Free;
  end;
end;

function TOracleMetadataProvider.GetFunctions: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT OBJECT_NAME ' +
      '  FROM ALL_OBJECTS ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
      '   AND OBJECT_TYPE = ''FUNCTION'' ' +
      'ORDER BY OBJECT_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('OBJECT_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

constructor TOracleMetadataProvider.Create(Conn: TUniConnection;
  const DBName: string; const Owner: string = '');
begin
  FDBName := DBName;
  FConn := Conn;
  FConn.ProviderName := 'Oracle';
  
  // Si no se especifica Owner, usar el usuario conectado
  if Owner = '' then
    FOwner := UpperCase(FConn.Username)
  else
    FOwner := UpperCase(Owner);
  
  FConn.Connected := True;
end;

destructor TOracleMetadataProvider.Destroy;
begin
  inherited;
end;

function TOracleMetadataProvider.GetProcedureDefinition(
  const ProcedureName: string): string;
var
  Query: TUniQuery;
  SourceLines: TStringList;
begin
  Query := TUniQuery.Create(nil);
  SourceLines := TStringList.Create;
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TEXT ' +
      '  FROM ALL_SOURCE ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
      '   AND NAME = ' + QuotedStr(UpperCase(ProcedureName)) +
      '   AND TYPE = ''PROCEDURE'' ' +
      'ORDER BY LINE';
    Query.Open;
    
    while not Query.Eof do
    begin
      SourceLines.Add(Query.FieldByName('TEXT').AsString);
      Query.Next;
    end;
    
    Result := SourceLines.Text;
  finally
    SourceLines.Free;
    Query.Free;
  end;
end;

function TOracleMetadataProvider.GetProcedures: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT OBJECT_NAME ' +
      '  FROM ALL_OBJECTS ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
      '   AND OBJECT_TYPE = ''PROCEDURE'' ' +
      'ORDER BY OBJECT_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('OBJECT_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TOracleMetadataProvider.GetTableIndexes(
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
        'SELECT i.INDEX_NAME, ' +
        '       i.UNIQUENESS, ' +
        '       c.CONSTRAINT_TYPE, ' +
        '       ic.COLUMN_NAME, ' +
        '       ic.COLUMN_POSITION ' +
        '  FROM ALL_INDEXES i ' +
        '  LEFT JOIN ALL_CONSTRAINTS c ' +
        '         ON i.INDEX_NAME = c.CONSTRAINT_NAME ' +
        '        AND i.OWNER = c.OWNER ' +
        ' INNER JOIN ALL_IND_COLUMNS ic ' +
        '         ON i.INDEX_NAME = ic.INDEX_NAME ' +
        '        AND i.OWNER = ic.INDEX_OWNER ' +
        ' WHERE i.TABLE_OWNER = ' + QuotedStr(FOwner) +
        '   AND i.TABLE_NAME = ' + QuotedStr(UpperCase(TableName)) +
        'ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION';
      Query.Open;
      
      LastIndexName := '';
      while not Query.Eof do
      begin
        if not SameText(Query.FieldByName('INDEX_NAME').AsString, LastIndexName) then
        begin
          if not SameText(LastIndexName, '') then
          begin
            CurrentIndex.Columns := ColList.ToArray;
            IndexList.Add(CurrentIndex);
            ColList.Clear;
          end;
          
          LastIndexName := Query.FieldByName('INDEX_NAME').AsString;
          CurrentIndex.IndexName := LastIndexName;
          CurrentIndex.IsPrimary := SameText(Query.FieldByName('CONSTRAINT_TYPE').AsString, 'P');
          CurrentIndex.IsUnique := SameText(Query.FieldByName('UNIQUENESS').AsString, 'UNIQUE');
        end;
        
        IndexCol.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
        IndexCol.SeqInIndex := Query.FieldByName('COLUMN_POSITION').AsInteger;
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

function TOracleMetadataProvider.GetTables: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TABLE_NAME ' +
      '  FROM ALL_TABLES ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
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

function TOracleMetadataProvider.GetTableStructure(
  const TableName: string): TTableInfo;
var
  Query: TUniQuery;
  Col: TColumnInfo;
  DataType: string;
begin
  Result := TTableInfo.Create;
  Result.TableName := TableName;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text :=
      'SELECT c.COLUMN_NAME, ' +
      '       c.DATA_TYPE, ' +
      '       c.DATA_LENGTH, ' +
      '       c.DATA_PRECISION, ' +
      '       c.DATA_SCALE, ' +
      '       c.CHAR_LENGTH, ' +
      '       c.NULLABLE, ' +
      '       c.DATA_DEFAULT, ' +
      '       cc.CONSTRAINT_TYPE ' +
      '  FROM ALL_TAB_COLUMNS c ' +
      '  LEFT JOIN ALL_CONS_COLUMNS ccc ' +
      '         ON c.OWNER = ccc.OWNER ' +
      '        AND c.TABLE_NAME = ccc.TABLE_NAME ' +
      '        AND c.COLUMN_NAME = ccc.COLUMN_NAME ' +
      '  LEFT JOIN ALL_CONSTRAINTS cc ' +
      '         ON ccc.OWNER = cc.OWNER ' +
      '        AND ccc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME ' +
      '        AND cc.CONSTRAINT_TYPE = ''P'' ' +
      ' WHERE c.OWNER = ' + QuotedStr(FOwner) +
      '   AND c.TABLE_NAME = ' + QuotedStr(UpperCase(TableName)) +
      'ORDER BY c.COLUMN_ID';
    Query.Open;
    
    while not Query.Eof do
    begin
      Col.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
      
      // Construir tipo de dato completo
      DataType := Query.FieldByName('DATA_TYPE').AsString;
      
      if SameText(DataType, 'NUMBER') then
      begin
        if not Query.FieldByName('DATA_PRECISION').IsNull then
        begin
          DataType := 'NUMBER(' + Query.FieldByName('DATA_PRECISION').AsString;
          if not Query.FieldByName('DATA_SCALE').IsNull and 
             (Query.FieldByName('DATA_SCALE').AsInteger > 0) then
            DataType := DataType + ',' + Query.FieldByName('DATA_SCALE').AsString;
          DataType := DataType + ')';
        end;
      end
      else if SameText(DataType, 'VARCHAR2') or SameText(DataType, 'NVARCHAR2') or
              SameText(DataType, 'CHAR') or SameText(DataType, 'NCHAR') then
      begin
        if not Query.FieldByName('CHAR_LENGTH').IsNull then
          DataType := DataType + '(' + Query.FieldByName('CHAR_LENGTH').AsString + ')';
      end
      else if SameText(DataType, 'RAW') then
      begin
        if not Query.FieldByName('DATA_LENGTH').IsNull then
          DataType := DataType + '(' + Query.FieldByName('DATA_LENGTH').AsString + ')';
      end;
      
      Col.DataType := DataType;
      
      // Nullability
      if SameText(Query.FieldByName('NULLABLE').AsString, 'N') then
        Col.IsNullable := 'NO'
      else
        Col.IsNullable := 'YES';
      
      // Primary Key
      if SameText(Query.FieldByName('CONSTRAINT_TYPE').AsString, 'P') then
        Col.ColumnKey := 'PRI'
      else
        Col.ColumnKey := '';
      
      Col.Extra := '';
      
      // Default value
      if not Query.FieldByName('DATA_DEFAULT').IsNull then
        Col.ColumnDefault := Trim(Query.FieldByName('DATA_DEFAULT').AsString)
      else
        Col.ColumnDefault := '';
      
      if not Query.FieldByName('CHAR_LENGTH').IsNull then
        Col.CharMaxLength := Query.FieldByName('CHAR_LENGTH').AsString
      else
        Col.CharMaxLength := '';
      
      Col.ColumnComment := '';
      
      Result.Columns.Add(Col);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TOracleMetadataProvider.GetTriggerDefinition(
  const TriggerName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TRIGGER_BODY ' +
      '  FROM ALL_TRIGGERS ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
      '   AND TRIGGER_NAME = ' + QuotedStr(UpperCase(TriggerName));
    Query.Open;
    
    if not Query.Eof and not Query.FieldByName('TRIGGER_BODY').IsNull then
      Result := Query.FieldByName('TRIGGER_BODY').AsString
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TOracleMetadataProvider.GetTriggers: TArray<TTriggerInfo>;
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
        'SELECT TRIGGER_NAME, ' +
        '       TRIGGERING_EVENT, ' +
        '       TRIGGER_TYPE, ' +
        '       TRIGGER_BODY, ' +
        '       TABLE_NAME ' +
        '  FROM ALL_TRIGGERS ' +
        ' WHERE OWNER = ' + QuotedStr(FOwner) +
        '   AND BASE_OBJECT_TYPE = ''TABLE'' ' +
        'ORDER BY TABLE_NAME, TRIGGER_NAME';
      Query.Open;
      
      while not Query.Eof do
      begin
        Trigger.TriggerName := Query.FieldByName('TRIGGER_NAME').AsString;
        
        // Evento: INSERT, UPDATE, DELETE, o combinaciones
        Trigger.EventManipulation := Query.FieldByName('TRIGGERING_EVENT').AsString;
        
        // Tipo: BEFORE/AFTER EACH ROW, BEFORE/AFTER STATEMENT
        if Pos('BEFORE', Query.FieldByName('TRIGGER_TYPE').AsString) > 0 then
          Trigger.ActionTiming := 'BEFORE'
        else if Pos('AFTER', Query.FieldByName('TRIGGER_TYPE').AsString) > 0 then
          Trigger.ActionTiming := 'AFTER'
        else if Pos('INSTEAD OF', Query.FieldByName('TRIGGER_TYPE').AsString) > 0 then
          Trigger.ActionTiming := 'INSTEAD OF'
        else
          Trigger.ActionTiming := 'UNKNOWN';
        
        if not Query.FieldByName('TRIGGER_BODY').IsNull then
          Trigger.ActionStatement := Query.FieldByName('TRIGGER_BODY').AsString
        else
          Trigger.ActionStatement := '';
        
        Trigger.EventObjectTable := Query.FieldByName('TABLE_NAME').AsString;
        
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

function TOracleMetadataProvider.GetViewDefinition(
  const ViewName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TEXT ' +
      '  FROM ALL_VIEWS ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
      '   AND VIEW_NAME = ' + QuotedStr(UpperCase(ViewName));
    Query.Open;
    
    if not Query.Eof and not Query.FieldByName('TEXT').IsNull then
      Result := 'CREATE VIEW ' + QuoteIdentifier(ViewName) + ' AS ' +
                Query.FieldByName('TEXT').AsString
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TOracleMetadataProvider.GetViews: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT VIEW_NAME ' +
      '  FROM ALL_VIEWS ' +
      ' WHERE OWNER = ' + QuotedStr(FOwner) +
      'ORDER BY VIEW_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('VIEW_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

end.