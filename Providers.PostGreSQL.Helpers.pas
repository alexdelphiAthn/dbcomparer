unit Providers.PostgreSQL.Helpers;

interface
uses Core.Helpers, Core.Types, System.SysUtils, System.StrUtils,
  System.Classes, Data.DB, Uni;
type
  TPostgreSQLHelpers = class(TDBHelpers)
  public
    function QuoteIdentifier(const Identifier: string): string; override;
    function GenerateColumnDefinition(const Col: TColumnInfo): string; override;
    function GenerateIndexDefinition(const TableName: string;
                                     const Idx: TIndexInfo): string; override;
    function NormalizeType(const AType: string): string; override;
    function TriggersAreEqual(const Trg1, Trg2: TTriggerInfo): Boolean; override;
    function GenerateCreateTableSQL(const Table: TTableInfo;
                                    const Indexes: TArray<TIndexInfo>): string; override;
    function GenerateAddColumnSQL(const TableName:string;
                                  const ColumnInfo:TColumnInfo): string; override;
    function GenerateDropColumnSQL(const TableName, ColumnName:string): string; override;
    function GenerateModifyColumnSQL(const TableName:string;
                                     const ColumnInfo:TColumnInfo): string; override;
    function GenerateUpdateSQL(const TableName: string;
                                  const SetClause, WhereClause: string): string; override;
    function GenerateDropIndexSQL(const TableName,
                                        IndexName:string): string; override;
    function GenerateDropTableSQL(const TableName:String): string; override;
    function GenerateDropTrigger(const Trigger:string):string; override;
    function GenerateDropProcedure(const Proc:string):string; override;
    function GenerateDropFunction(const FuncName: string): string; override;
    function GenerateDropView(const View:string):string; override;
    function ValueToSQL(const Field: TField): string;
    function GenerateCreateProcedureSQL(const Body: string): string; override;
    function GenerateCreateFunctionSQL(const Body: string): string; override;
    function GenerateDeleteSQL(const TableName, WhereClause: string): string; override;
    function GenerateInsertSQL(const TableName: string; Fields,
                                                        Values: TStringList;
                               const HasIdentity: Boolean = False): string; override;
    function GenerateCreateSequence(const SequenceName: string): string; override;
    function GenerateDropSequence(const SequenceName: string): string; override;
  end;

implementation

function TPostgreSQLHelpers.GenerateCreateSequence(const SequenceName: string): string;
begin
  // Generación simple.
  // Nota: Si la secuencia viene de un SERIAL, Postgres la crea automáticamente
  // al crear la tabla. El Core.Engine detectará que ya existe y no ejecutará esto.
  Result := 'CREATE SEQUENCE ' + QuoteIdentifier(SequenceName) + ';';
end;

function TPostgreSQLHelpers.GenerateDropSequence(const SequenceName: string): string;
begin
  Result := 'DROP SEQUENCE IF EXISTS ' + QuoteIdentifier(SequenceName) + ' CASCADE;';
end;

function TPostgreSQLHelpers.ValueToSQL(const Field: TField): string;
  function BytesToHex(const Bytes: TBytes): string;
  var
    i: Integer;
  begin
    Result := '';
    for i := Low(Bytes) to High(Bytes) do
      Result := Result + IntToHex(Bytes[i], 2);
  end;
begin
  if Field.IsNull then
    Exit('NULL');

  case Field.DataType of
    ftString, ftWideString, ftMemo, ftWideMemo, ftFmtMemo:
      Result := QuotedStr(StringReplace(Field.AsString, '''', '''''', [rfReplaceAll]));
    ftDate:
      Result := QuotedStr(FormatDateTime('yyyy-mm-dd', Field.AsDateTime));
    ftTime:
      Result := QuotedStr(FormatDateTime('hh:nn:ss', Field.AsDateTime));
    ftDateTime, ftTimeStamp:
      Result := QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Field.AsDateTime));
    ftBoolean:
      if Field.AsBoolean then Result := 'TRUE' else Result := 'FALSE';
    ftBlob, ftGraphic, ftVarBytes, ftBytes:
      Result := '''\\x' + BytesToHex(Field.AsBytes) + '''';
    else
      Result := Field.AsString;
  end;
end;

function TPostgreSQLHelpers.GenerateInsertSQL(const TableName: string;
  Fields, Values: TStringList; const HasIdentity: Boolean = False): string;
var
  i: Integer;
  FieldList, ValueList: string;
begin
  FieldList := '';
  ValueList := '';
  for i := 0 to Fields.Count - 1 do
  begin
    if i > 0 then FieldList := FieldList + ', ';
    FieldList := FieldList + Fields[i];
  end;
  for i := 0 to Values.Count - 1 do
  begin
    if i > 0 then ValueList := ValueList + ', ';
    ValueList := ValueList + Values[i];
  end;
  Result := 'INSERT INTO ' + QuoteIdentifier(TableName) + ' (' +
            FieldList + ') VALUES (' + ValueList + ');';
end;

function TPostgreSQLHelpers.GenerateUpdateSQL(const TableName: string;
  const SetClause, WhereClause: string): string;
begin
  Result := 'UPDATE ' + QuoteIdentifier(TableName) + ' SET ' + SetClause +
            ' WHERE ' + WhereClause + ';';
end;

function TPostgreSQLHelpers.TriggersAreEqual(const Trg1,
                                                   Trg2: TTriggerInfo): Boolean;
begin
  Result := SameText(Trg1.TriggerName, Trg2.TriggerName) and
            (Trg1.EventManipulation = Trg2.EventManipulation) and
            (Trg1.ActionTiming = Trg2.ActionTiming) and
            (Trim(Trg1.ActionStatement) = Trim(Trg2.ActionStatement));
end;

function TPostgreSQLHelpers.QuoteIdentifier(const Identifier: string): string;
begin
  Result := '"' + StringReplace(Identifier, '"', '""', [rfReplaceAll]) + '"';
end;

function TPostgreSQLHelpers.NormalizeType(const AType: string): string;
var
  S: string;
begin
  S := LowerCase(Trim(AType));
  S := StringReplace(S, ' ', '', [rfReplaceAll]);
  
  // PostgreSQL aliases normalizados
  if S = 'int' then S := 'integer'
  else if S = 'int4' then S := 'integer'
  else if S = 'int2' then S := 'smallint'
  else if S = 'int8' then S := 'bigint'
  else if S = 'serial' then S := 'integer'
  else if S = 'bigserial' then S := 'bigint'
  else if S = 'bool' then S := 'boolean'
  else if StartsText('charactervarying', S) then S := StringReplace(S, 'charactervarying', 'varchar', [])
  else if StartsText('character', S) then S := StringReplace(S, 'character', 'char', []);
  
  Result := S;
end;

function TPostgreSQLHelpers.GenerateAddColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' ADD COLUMN ' + GenerateColumnDefinition(ColumnInfo) + ';';
end;

function TPostgreSQLHelpers.GenerateColumnDefinition(const Col: TColumnInfo): string;
var
  DefVal: string;
begin
  Result := QuoteIdentifier(Col.ColumnName) + ' ' + Col.DataType;
  
  // PostgreSQL usa SERIAL/BIGSERIAL para auto-increment
  // Si ya viene como SERIAL en el tipo, no agregar nada más
  if not StartsText('serial', LowerCase(Col.DataType)) and
     not StartsText('bigserial', LowerCase(Col.DataType)) then
  begin
    // Nullability
    if SameText(Col.IsNullable, 'NO') then
      Result := Result + ' NOT NULL'
    else
      Result := Result + ' NULL';
    
    // Default value
    if (not SameText(Col.ColumnDefault, '')) and
       (not SameText(Col.ColumnDefault, 'NULL')) then
    begin
      DefVal := Col.ColumnDefault;
      
      // Funciones comunes de PostgreSQL
      if (Pos('now()', LowerCase(DefVal)) > 0) or
         (Pos('current_timestamp', LowerCase(DefVal)) > 0) or
         (Pos('current_date', LowerCase(DefVal)) > 0) or
         (Pos('current_time', LowerCase(DefVal)) > 0) or
         (Pos('nextval(', LowerCase(DefVal)) > 0) or
         (Pos('gen_random_uuid()', LowerCase(DefVal)) > 0) then
      begin
        Result := Result + ' DEFAULT ' + DefVal;
      end
      else
      begin
        // Eliminar comillas si ya las tiene
        if (Length(DefVal) >= 2) then
        begin
          if (DefVal[1] = '''') and (DefVal[Length(DefVal)] = '''') then
            DefVal := Copy(DefVal, 2, Length(DefVal) - 2)
          else if (DefVal[1] = '"') and (DefVal[Length(DefVal)] = '"') then
            DefVal := Copy(DefVal, 2, Length(DefVal) - 2);
        end;
        Result := Result + ' DEFAULT ' + QuotedStr(DefVal);
      end;
    end
    else if SameText(Col.ColumnDefault, 'NULL') then
      Result := Result + ' DEFAULT NULL';
  end;
end;

function TPostgreSQLHelpers.GenerateCreateProcedureSQL(const Body: string): string;
begin
  // PostgreSQL no necesita delimitador especial, usa $$ dentro del cuerpo
  Result := Body + ';';
end;

function TPostgreSQLHelpers.GenerateCreateFunctionSQL(const Body: string): string;
begin
  // PostgreSQL no necesita delimitador especial
  Result := Body + ';';
end;

function TPostgreSQLHelpers.GenerateCreateTableSQL(const Table: TTableInfo;
  const Indexes: TArray<TIndexInfo>): string;
var
  i: Integer;
  PKList: TStringList;
  ColDef: string;
begin
  Result := 'CREATE TABLE ' + QuoteIdentifier(Table.TableName) + ' (' + sLineBreak;
  PKList := TStringList.Create;
  try
    for i := 0 to Table.Columns.Count - 1 do
    begin
      ColDef := '  ' + GenerateColumnDefinition(Table.Columns[i]);
      // Detectar Primary Key
      if SameText(Table.Columns[i].ColumnKey, 'PRI') then
        PKList.Add(QuoteIdentifier(Table.Columns[i].ColumnName));
      if (i < Table.Columns.Count - 1) or (PKList.Count > 0) then
        ColDef := ColDef + ',';
      Result := Result + ColDef + sLineBreak;
    end;
    // Agregar Primary Key constraint
    if PKList.Count > 0 then
    begin
      Result := Result + '  CONSTRAINT ' + QuoteIdentifier('pk_' + Table.TableName) + 
                ' PRIMARY KEY (' + PKList.CommaText + ')' + sLineBreak;
    end;
    Result := Result + ');';
  finally
    PKList.Free;
  end;
end;

function TPostgreSQLHelpers.GenerateDeleteSQL(const TableName,
  WhereClause: string): string;
begin
  Result := 'DELETE FROM ' + QuoteIdentifier(TableName) +
            ' WHERE ' + WhereClause + ';';
end;

function TPostgreSQLHelpers.GenerateDropColumnSQL(const TableName,
  ColumnName: string): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' DROP COLUMN ' + QuoteIdentifier(ColumnName) + ';';
end;

function TPostgreSQLHelpers.GenerateDropFunction(const FuncName: string): string;
begin
  Result := 'DROP FUNCTION IF EXISTS ' + QuoteIdentifier(FuncName) + ' CASCADE;';
end;

function TPostgreSQLHelpers.GenerateDropTableSQL(const TableName:String): string;
begin
  Result := 'DROP TABLE IF EXISTS ' + QuoteIdentifier(TableName) + ' CASCADE;';
end;

function TPostgreSQLHelpers.GenerateDropTrigger(const Trigger: string): string;
begin
  // En PostgreSQL los triggers se dropean con ON table_name
  // Asumimos que el trigger name viene con formato 'trigger ON table'
  // Si solo viene el nombre, generamos sin ON
  Result := 'DROP TRIGGER IF EXISTS ' + QuoteIdentifier(Trigger) + ';';
end;

function TPostgreSQLHelpers.GenerateDropView(const View: string): string;
begin
  Result := 'DROP VIEW IF EXISTS ' + QuoteIdentifier(View) + ' CASCADE;';
end;

function TPostgreSQLHelpers.GenerateDropIndexSQL(const TableName,
  IndexName: string): string;
begin
  // PostgreSQL no requiere especificar la tabla para DROP INDEX
  Result := 'DROP INDEX IF EXISTS ' + QuoteIdentifier(IndexName) + ';';
end;

function TPostgreSQLHelpers.GenerateDropProcedure(const Proc: string): string;
begin
  // En PostgreSQL, los procedimientos se introducen en versión 11+
  Result := 'DROP PROCEDURE IF EXISTS ' + QuoteIdentifier(Proc) + ' CASCADE;';
end;

function TPostgreSQLHelpers.GenerateIndexDefinition(const TableName: string;
                                                     const Idx: TIndexInfo): string;
var
  i: Integer;
  ColNames: string;
begin
  ColNames := '';
  for i := 0 to High(Idx.Columns) do
  begin
    if i > 0 then
      ColNames := ColNames + ', ';
    ColNames := ColNames + QuoteIdentifier(Idx.Columns[i].ColumnName);
  end;
  
  if Idx.IsPrimary then
    Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
              ' ADD CONSTRAINT ' + QuoteIdentifier('pk_' + TableName) +
              ' PRIMARY KEY (' + ColNames + ');'
  else if Idx.IsUnique then
    Result := 'CREATE UNIQUE INDEX ' + QuoteIdentifier(Idx.IndexName) +
              ' ON ' + QuoteIdentifier(TableName) +
              ' (' + ColNames + ');'
  else
    Result := 'CREATE INDEX ' + QuoteIdentifier(Idx.IndexName) +
              ' ON ' + QuoteIdentifier(TableName) +
              ' (' + ColNames + ');';
end;

function TPostgreSQLHelpers.GenerateModifyColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
var
  SQLParts: TStringList;
begin
  // PostgreSQL requiere múltiples ALTER COLUMN para cambiar tipo, nullability, default
  SQLParts := TStringList.Create;
  try
    // Cambiar tipo de dato
    SQLParts.Add('ALTER TABLE ' + QuoteIdentifier(TableName) +
                 ' ALTER COLUMN ' + QuoteIdentifier(ColumnInfo.ColumnName) +
                 ' TYPE ' + ColumnInfo.DataType + ';');
    
    // Cambiar nullability
    if SameText(ColumnInfo.IsNullable, 'NO') then
      SQLParts.Add('ALTER TABLE ' + QuoteIdentifier(TableName) +
                   ' ALTER COLUMN ' + QuoteIdentifier(ColumnInfo.ColumnName) +
                   ' SET NOT NULL;')
    else
      SQLParts.Add('ALTER TABLE ' + QuoteIdentifier(TableName) +
                   ' ALTER COLUMN ' + QuoteIdentifier(ColumnInfo.ColumnName) +
                   ' DROP NOT NULL;');
    
    // Cambiar default
    if (not SameText(ColumnInfo.ColumnDefault, '')) and
       (not SameText(ColumnInfo.ColumnDefault, 'NULL')) then
    begin
      SQLParts.Add('ALTER TABLE ' + QuoteIdentifier(TableName) +
                   ' ALTER COLUMN ' + QuoteIdentifier(ColumnInfo.ColumnName) +
                   ' SET DEFAULT ' + ColumnInfo.ColumnDefault + ';');
    end
    else
    begin
      SQLParts.Add('ALTER TABLE ' + QuoteIdentifier(TableName) +
                   ' ALTER COLUMN ' + QuoteIdentifier(ColumnInfo.ColumnName) +
                   ' DROP DEFAULT;');
    end;
    
    Result := SQLParts.Text;
  finally
    SQLParts.Free;
  end;
end;

end.