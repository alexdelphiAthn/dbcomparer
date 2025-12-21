unit Providers.InterBase;

interface

uses
  System.Classes, System.SysUtils, Data.DB, Core.Interfaces, Core.Types, Uni,
  InterBaseUniProvider, System.StrUtils, System.Generics.Collections,
  Providers.InterBase.Helpers, Core.Helpers;

type
  TInterBaseMetadataProvider = class(TInterfacedObject, IDBMetadataProvider)
  private
    FConn: TUniConnection;
    FDBName: string;
    function GetFieldType(FieldType, FieldSubType, FieldLength, 
                         CharLength, Precision, Scale: Integer): string;
  public
    constructor Create(Conn: TUniConnection; const DBName: string);
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

{ TInterBaseMetadataProvider }

function TInterBaseMetadataProvider.GetSequences: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    // RDB$GENERATORS contiene las secuencias
    Query.SQL.Text := 'SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS ' +
                      'WHERE RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Trim(Query.Fields[0].AsString));
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TInterBaseMetadataProvider.QuoteIdentifier(const Identifier: string): string;
begin
  Result := '"' + Identifier + '"';
end;

function TInterBaseMetadataProvider.GetFieldType(FieldType, FieldSubType, 
  FieldLength, CharLength, Precision, Scale: Integer): string;
begin
  case FieldType of
    7: // SMALLINT o NUMERIC(4,x)
      begin
        if Scale < 0 then
          Result := 'NUMERIC(' + IntToStr(Precision) + ',' + IntToStr(Abs(Scale)) + ')'
        else
          Result := 'SMALLINT';
      end;
    8: // INTEGER o NUMERIC(9,x)
      begin
        if Scale < 0 then
          Result := 'NUMERIC(' + IntToStr(Precision) + ',' + IntToStr(Abs(Scale)) + ')'
        else
          Result := 'INTEGER';
      end;
    9: // QUAD (array dimension)
      Result := 'QUAD';
    10: // FLOAT
      Result := 'FLOAT';
    12: // DATE
      Result := 'DATE';
    13: // TIME
      Result := 'TIME';
    14: // CHAR
      begin
        if CharLength > 0 then
          Result := 'CHAR(' + IntToStr(CharLength) + ')'
        else
          Result := 'CHAR(' + IntToStr(FieldLength) + ')';
      end;
    16: // BIGINT o NUMERIC(18,x)
      begin
        if Scale < 0 then
          Result := 'NUMERIC(' + IntToStr(Precision) + ',' + IntToStr(Abs(Scale)) + ')'
        else
          Result := 'BIGINT';
      end;
    27: // DOUBLE PRECISION o NUMERIC(15,x)
      begin
        if Scale < 0 then
          Result := 'NUMERIC(' + IntToStr(Precision) + ',' + IntToStr(Abs(Scale)) + ')'
        else
          Result := 'DOUBLE PRECISION';
      end;
    35: // TIMESTAMP
      Result := 'TIMESTAMP';
    37: // VARCHAR
      begin
        if CharLength > 0 then
          Result := 'VARCHAR(' + IntToStr(CharLength) + ')'
        else
          Result := 'VARCHAR(' + IntToStr(FieldLength) + ')';
      end;
    261: // BLOB
      begin
        case FieldSubType of
          0: Result := 'BLOB'; // Binary
          1: Result := 'BLOB SUB_TYPE TEXT'; // Text
          else
            Result := 'BLOB SUB_TYPE ' + IntToStr(FieldSubType);
        end;
      end;
    else
      Result := 'UNKNOWN(' + IntToStr(FieldType) + ')';
  end;
end;

function TInterBaseMetadataProvider.GetData(const TableName: string; 
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

function TInterBaseMetadataProvider.GetFunctionDefinition(
  const FunctionName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    // InterBase 2020+ soporta funciones con RDB$FUNCTION_SOURCE
    Query.SQL.Text := 
      'SELECT RDB$FUNCTION_SOURCE ' +
      '  FROM RDB$FUNCTIONS ' +
      ' WHERE TRIM(RDB$FUNCTION_NAME) = ' + QuotedStr(Trim(UpperCase(FunctionName)));
    Query.Open;
    if not Query.Eof and not Query.Fields[0].IsNull then
      Result := Trim(Query.Fields[0].AsString)
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TInterBaseMetadataProvider.GetFunctions: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    // Verificar si la tabla RDB$FUNCTIONS existe (InterBase 2020+)
    Query.SQL.Text := 
      'SELECT TRIM(RDB$FUNCTION_NAME) AS FUNCTION_NAME ' +
      '  FROM RDB$FUNCTIONS ' +
      ' WHERE RDB$SYSTEM_FLAG = 0 ' +
      '   OR RDB$SYSTEM_FLAG IS NULL ' +
      'ORDER BY RDB$FUNCTION_NAME';
    try
      Query.Open;
      while not Query.Eof do
      begin
        Result.Add(Query.FieldByName('FUNCTION_NAME').AsString);
        Query.Next;
      end;
    except
      // Si RDB$FUNCTIONS no existe, ignorar (versiones antiguas)
    end;
  finally
    Query.Free;
  end;
end;

constructor TInterBaseMetadataProvider.Create(Conn: TUniConnection;
  const DBName: string);
begin
  FDBName := DBName;
  FConn := Conn;
  FConn.ProviderName := 'InterBase';
  FConn.Connected := True;
end;

destructor TInterBaseMetadataProvider.Destroy;
begin
  inherited;
end;

function TInterBaseMetadataProvider.GetProcedureDefinition(
  const ProcedureName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT RDB$PROCEDURE_SOURCE ' +
      '  FROM RDB$PROCEDURES ' +
      ' WHERE TRIM(RDB$PROCEDURE_NAME) = ' + QuotedStr(Trim(UpperCase(ProcedureName)));
    Query.Open;
    if not Query.Eof and not Query.Fields[0].IsNull then
      Result := Trim(Query.Fields[0].AsString)
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TInterBaseMetadataProvider.GetProcedures: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TRIM(RDB$PROCEDURE_NAME) AS PROCEDURE_NAME ' +
      '  FROM RDB$PROCEDURES ' +
      ' WHERE RDB$SYSTEM_FLAG = 0 ' +
      '   OR RDB$SYSTEM_FLAG IS NULL ' +
      'ORDER BY RDB$PROCEDURE_NAME';
    Query.Open;
    while not Query.Eof do
    begin
      Result.Add(Query.FieldByName('PROCEDURE_NAME').AsString);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TInterBaseMetadataProvider.GetTableIndexes(
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
        'SELECT TRIM(i.RDB$INDEX_NAME) AS INDEX_NAME, ' +
        '       i.RDB$UNIQUE_FLAG AS IS_UNIQUE, ' +
        '       TRIM(rc.RDB$CONSTRAINT_TYPE) AS CONSTRAINT_TYPE, ' +
        '       TRIM(s.RDB$FIELD_NAME) AS COLUMN_NAME, ' +
        '       s.RDB$FIELD_POSITION AS SEQ_IN_INDEX ' +
        '  FROM RDB$INDICES i ' +
        '  LEFT JOIN RDB$RELATION_CONSTRAINTS rc ' +
        '         ON i.RDB$INDEX_NAME = rc.RDB$INDEX_NAME ' +
        ' INNER JOIN RDB$INDEX_SEGMENTS s ' +
        '         ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME ' +
        ' WHERE TRIM(i.RDB$RELATION_NAME) = ' + QuotedStr(Trim(UpperCase(TableName))) +
        'ORDER BY i.RDB$INDEX_NAME, s.RDB$FIELD_POSITION';
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
          CurrentIndex.IsPrimary := SameText(Query.FieldByName('CONSTRAINT_TYPE').AsString, 'PRIMARY KEY');
          CurrentIndex.IsUnique := (Query.FieldByName('IS_UNIQUE').AsInteger = 1);
        end;
        
        IndexCol.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
        IndexCol.SeqInIndex := Query.FieldByName('SEQ_IN_INDEX').AsInteger + 1;
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

function TInterBaseMetadataProvider.GetTables: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TRIM(RDB$RELATION_NAME) AS TABLE_NAME ' +
      '  FROM RDB$RELATIONS ' +
      ' WHERE RDB$VIEW_BLR IS NULL ' +
      '   AND (RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL) ' +
      'ORDER BY RDB$RELATION_NAME';
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

function TInterBaseMetadataProvider.GetTableStructure(
  const TableName: string): TTableInfo;
var
  Query: TUniQuery;
  Col: TColumnInfo;
  FieldType, FieldSubType, FieldLength, CharLength, Precision, Scale: Integer;
  PKQuery: TUniQuery;
begin
  Result := TTableInfo.Create;
  Result.TableName := TableName;
  
  // Primero obtener las columnas de Primary Key
  PKQuery := TUniQuery.Create(nil);
  try
    PKQuery.Connection := FConn;
    PKQuery.SQL.Text :=
      'SELECT TRIM(s.RDB$FIELD_NAME) AS COLUMN_NAME ' +
      '  FROM RDB$RELATION_CONSTRAINTS rc ' +
      ' INNER JOIN RDB$INDEX_SEGMENTS s ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME ' +
      ' WHERE TRIM(rc.RDB$RELATION_NAME) = ' + QuotedStr(Trim(UpperCase(TableName))) +
      '   AND rc.RDB$CONSTRAINT_TYPE = ''PRIMARY KEY''';
    PKQuery.Open;
  except
    PKQuery.Free;
    PKQuery := nil;
  end;
  
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text :=
      'SELECT TRIM(rf.RDB$FIELD_NAME) AS COLUMN_NAME, ' +
      '       f.RDB$FIELD_TYPE, ' +
      '       f.RDB$FIELD_SUB_TYPE, ' +
      '       f.RDB$FIELD_LENGTH, ' +
      '       f.RDB$FIELD_SCALE, ' +
      '       f.RDB$FIELD_PRECISION, ' +
      '       f.RDB$CHARACTER_LENGTH, ' +
      '       rf.RDB$NULL_FLAG, ' +
      '       rf.RDB$DEFAULT_SOURCE, ' +
      '       rf.RDB$FIELD_POSITION ' +
      '  FROM RDB$RELATION_FIELDS rf ' +
      ' INNER JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME ' +
      ' WHERE TRIM(rf.RDB$RELATION_NAME) = ' + QuotedStr(Trim(UpperCase(TableName))) +
      'ORDER BY rf.RDB$FIELD_POSITION';
    Query.Open;
    
    while not Query.Eof do
    begin
      Col.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
      
      // Obtener parámetros del tipo
      FieldType := Query.FieldByName('RDB$FIELD_TYPE').AsInteger;
      FieldSubType := 0;
      if not Query.FieldByName('RDB$FIELD_SUB_TYPE').IsNull then
        FieldSubType := Query.FieldByName('RDB$FIELD_SUB_TYPE').AsInteger;
      FieldLength := Query.FieldByName('RDB$FIELD_LENGTH').AsInteger;
      CharLength := 0;
      if not Query.FieldByName('RDB$CHARACTER_LENGTH').IsNull then
        CharLength := Query.FieldByName('RDB$CHARACTER_LENGTH').AsInteger;
      Precision := 0;
      if not Query.FieldByName('RDB$FIELD_PRECISION').IsNull then
        Precision := Query.FieldByName('RDB$FIELD_PRECISION').AsInteger;
      Scale := 0;
      if not Query.FieldByName('RDB$FIELD_SCALE').IsNull then
        Scale := Query.FieldByName('RDB$FIELD_SCALE').AsInteger;
      
      Col.DataType := GetFieldType(FieldType, FieldSubType, FieldLength, 
                                   CharLength, Precision, Scale);
      
      // Nullability
      if Query.FieldByName('RDB$NULL_FLAG').AsInteger = 1 then
        Col.IsNullable := 'NO'
      else
        Col.IsNullable := 'YES';
      
      // Primary Key
      Col.ColumnKey := '';
      if Assigned(PKQuery) then
      begin
        PKQuery.First;
        while not PKQuery.Eof do
        begin
          if SameText(PKQuery.FieldByName('COLUMN_NAME').AsString, Col.ColumnName) then
          begin
            Col.ColumnKey := 'PRI';
            Break;
          end;
          PKQuery.Next;
        end;
      end;
      
      Col.Extra := '';
      
      // Default value
      if not Query.FieldByName('RDB$DEFAULT_SOURCE').IsNull then
      begin
        Col.ColumnDefault := Trim(Query.FieldByName('RDB$DEFAULT_SOURCE').AsString);
        // Eliminar "DEFAULT " del inicio si existe
        if StartsText('DEFAULT ', UpperCase(Col.ColumnDefault)) then
          Col.ColumnDefault := Trim(Copy(Col.ColumnDefault, 9, Length(Col.ColumnDefault)));
      end
      else
        Col.ColumnDefault := '';
      
      if CharLength > 0 then
        Col.CharMaxLength := IntToStr(CharLength)
      else
        Col.CharMaxLength := '';
        
      Col.ColumnComment := '';
      
      Result.Columns.Add(Col);
      Query.Next;
    end;
  finally
    Query.Free;
    if Assigned(PKQuery) then
      PKQuery.Free;
  end;
end;

function TInterBaseMetadataProvider.GetTriggerDefinition(
  const TriggerName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT RDB$TRIGGER_SOURCE ' +
      '  FROM RDB$TRIGGERS ' +
      ' WHERE TRIM(RDB$TRIGGER_NAME) = ' + QuotedStr(Trim(UpperCase(TriggerName)));
    Query.Open;
    if not Query.Eof and not Query.Fields[0].IsNull then
      Result := Trim(Query.Fields[0].AsString)
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TInterBaseMetadataProvider.GetTriggers: TArray<TTriggerInfo>;
var
  Query: TUniQuery;
  TriggerList: TList<TTriggerInfo>;
  Trigger: TTriggerInfo;
  TriggerType: Integer;
begin
  TriggerList := TList<TTriggerInfo>.Create;
  try
    Query := TUniQuery.Create(nil);
    try
      Query.Connection := FConn;
      Query.SQL.Text :=
        'SELECT TRIM(RDB$TRIGGER_NAME) AS TRIGGER_NAME, ' +
        '       RDB$TRIGGER_TYPE, ' +
        '       RDB$TRIGGER_SOURCE, ' +
        '       TRIM(RDB$RELATION_NAME) AS TABLE_NAME ' +
        '  FROM RDB$TRIGGERS ' +
        ' WHERE (RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL) ' +
        '   AND RDB$RELATION_NAME IS NOT NULL ' +
        'ORDER BY RDB$RELATION_NAME, RDB$TRIGGER_NAME';
      Query.Open;
      
      while not Query.Eof do
      begin
        Trigger.TriggerName := Query.FieldByName('TRIGGER_NAME').AsString;
        TriggerType := Query.FieldByName('RDB$TRIGGER_TYPE').AsInteger;
        
        // Decodificar tipo de trigger InterBase/Firebird
        // Los bits indican: 1=BEFORE, 2=AFTER
        // Los eventos: +1=INSERT, +3=UPDATE, +5=DELETE
        case (TriggerType and not 1) of
          0: Trigger.EventManipulation := 'INSERT';   // Type 1 (BEFORE) o 2 (AFTER)
          2: Trigger.EventManipulation := 'UPDATE';   // Type 3 (BEFORE) o 4 (AFTER)
          4: Trigger.EventManipulation := 'DELETE';   // Type 5 (BEFORE) o 6 (AFTER)
          else
            Trigger.EventManipulation := 'UNKNOWN';
        end;
        
        if (TriggerType and 1) = 1 then
          Trigger.ActionTiming := 'BEFORE'
        else
          Trigger.ActionTiming := 'AFTER';
        
        if not Query.FieldByName('RDB$TRIGGER_SOURCE').IsNull then
          Trigger.ActionStatement := Trim(Query.FieldByName('RDB$TRIGGER_SOURCE').AsString)
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

function TInterBaseMetadataProvider.GetViewDefinition(
  const ViewName: string): string;
var
  Query: TUniQuery;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT RDB$VIEW_SOURCE ' +
      '  FROM RDB$RELATIONS ' +
      ' WHERE TRIM(RDB$RELATION_NAME) = ' + QuotedStr(Trim(UpperCase(ViewName)));
    Query.Open;
    if not Query.Eof and not Query.Fields[0].IsNull then
      Result := 'CREATE VIEW ' + QuoteIdentifier(ViewName) + ' AS ' +
                Trim(Query.Fields[0].AsString)
    else
      Result := '';
  finally
    Query.Free;
  end;
end;

function TInterBaseMetadataProvider.GetViews: TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := FConn;
    Query.SQL.Text := 
      'SELECT TRIM(RDB$RELATION_NAME) AS VIEW_NAME ' +
      '  FROM RDB$RELATIONS ' +
      ' WHERE RDB$VIEW_BLR IS NOT NULL ' +
      '   AND (RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL) ' +
      'ORDER BY RDB$RELATION_NAME';
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