unit Providers.InterBase.Helpers;

interface
uses Core.Helpers, Core.Types, System.SysUtils, System.StrUtils,
  System.Classes, Data.DB, Uni;
type
  TInterBaseHelpers = class(TDBHelpers)
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
    function GenerateCreateSequence(const GeneratorName: string): string;
    function GenerateDropSequence(const GeneratorName: string): string;
  end;

implementation

function TInterBaseHelpers.ValueToSQL(const Field: TField): string;
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
    ftTime:
      Result := QuotedStr(FormatDateTime('hh:nn:ss', Field.AsDateTime));
    ftDate:
      Result := QuotedStr(FormatDateTime('yyyy-mm-dd', Field.AsDateTime)); // ISO puro
    ftDateTime, ftTimeStamp:
      Result := QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Field.AsDateTime));
    ftBoolean:
      if Field.AsBoolean then Result := '1' else Result := '0';
    ftBlob, ftGraphic, ftVarBytes, ftBytes:
      // InterBase usa formato hexadecimal con x'...'
      Result := 'x''' + BytesToHex(Field.AsBytes) + '''';
    else
      Result := Field.AsString;
  end;
end;

function TInterBaseHelpers.GenerateInsertSQL(const TableName: string;
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

function TInterBaseHelpers.GenerateUpdateSQL(const TableName: string;
  const SetClause, WhereClause: string): string;
begin
  Result := 'UPDATE ' + QuoteIdentifier(TableName) + ' SET ' + SetClause +
            ' WHERE ' + WhereClause + ';';
end;

function TInterBaseHelpers.TriggersAreEqual(const Trg1,
                                                   Trg2: TTriggerInfo): Boolean;
begin
  Result := SameText(Trg1.TriggerName, Trg2.TriggerName) and
            (Trg1.EventManipulation = Trg2.EventManipulation) and
            (Trg1.ActionTiming = Trg2.ActionTiming) and
            (Trim(Trg1.ActionStatement) = Trim(Trg2.ActionStatement));
end;

function TInterBaseHelpers.QuoteIdentifier(const Identifier: string): string;
begin
  // InterBase usa comillas dobles para identificadores case-sensitive
  // o sin comillas para case-insensitive (uppercase automático)
  Result := '"' + Identifier + '"';
end;

function TInterBaseHelpers.NormalizeType(const AType: string): string;
var
  S: string;
begin
  S := UpperCase(Trim(AType)); // InterBase guarda tipos en UPPERCASE
  S := StringReplace(S, ' ', '', [rfReplaceAll]);
  
  // Normalizar aliases comunes
  if S = 'INT' then S := 'INTEGER'
  else if S = 'INT64' then S := 'BIGINT'
  else if StartsText('VARCHAR', S) then S := StringReplace(S, 'VARCHAR', 'VARCHAR', []);
  
  Result := S;
end;

function TInterBaseHelpers.GenerateAddColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' ADD ' + GenerateColumnDefinition(ColumnInfo) + ';';
end;

function TInterBaseHelpers.GenerateColumnDefinition(const Col: TColumnInfo): string;
var
  DefVal: string;
begin
  Result := QuoteIdentifier(Col.ColumnName) + ' ' + Col.DataType;
  
  // Nullability (InterBase: NOT NULL debe ir antes del DEFAULT)
  if SameText(Col.IsNullable, 'NO') then
    Result := Result + ' NOT NULL';
  
  // Default value
  if (not SameText(Col.ColumnDefault, '')) and
     (not SameText(Col.ColumnDefault, 'NULL')) then
  begin
    DefVal := Col.ColumnDefault;
    
    // Funciones comunes de InterBase
    if (Pos('CURRENT_TIMESTAMP', UpperCase(DefVal)) > 0) or
       (Pos('CURRENT_DATE', UpperCase(DefVal)) > 0) or
       (Pos('CURRENT_TIME', UpperCase(DefVal)) > 0) or
       (Pos('GEN_ID(', UpperCase(DefVal)) > 0) then
    begin
      Result := Result + ' DEFAULT ' + DefVal;
    end
    else
    begin
      // Eliminar comillas si ya las tiene
      if (Length(DefVal) >= 2) and (DefVal[1] = '''') and
         (DefVal[Length(DefVal)] = '''') then
        DefVal := Copy(DefVal, 2, Length(DefVal) - 2);
      Result := Result + ' DEFAULT ' + QuotedStr(DefVal);
    end;
  end;
  
  // InterBase no tiene AUTO_INCREMENT, se usa GENERATOR + TRIGGER
  // El campo Extra podría indicar si tiene un generator asociado
end;

function TInterBaseHelpers.GenerateCreateProcedureSQL(const Body: string): string;
begin
  // InterBase usa SET TERM para cambiar el terminador de sentencias
  Result := 'SET TERM ^ ;' + sLineBreak +
            Body + '^' + sLineBreak +
            'SET TERM ; ^';
end;

function TInterBaseHelpers.GenerateCreateFunctionSQL(const Body: string): string;
begin
  // InterBase 2020+ soporta funciones UDF, sintaxis similar a procedures
  Result := 'SET TERM ^ ;' + sLineBreak +
            Body + '^' + sLineBreak +
            'SET TERM ; ^';
end;

function TInterBaseHelpers.GenerateCreateTableSQL(const Table: TTableInfo;
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
      Result := Result + '  CONSTRAINT PK_' + Table.TableName + 
                ' PRIMARY KEY (' + PKList.CommaText + ')' + sLineBreak;
    end;
    Result := Result + ');';
  finally
    PKList.Free;
  end;
end;

function TInterBaseHelpers.GenerateDeleteSQL(const TableName,
  WhereClause: string): string;
begin
  Result := 'DELETE FROM ' + QuoteIdentifier(TableName) +
            ' WHERE ' + WhereClause + ';';
end;

function TInterBaseHelpers.GenerateDropColumnSQL(const TableName,
  ColumnName: string): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' DROP ' + QuoteIdentifier(ColumnName) + ';';
end;

function TInterBaseHelpers.GenerateDropFunction(const FuncName: string): string;
begin
  // InterBase 2020+ soporta DROP FUNCTION
  Result := 'DROP FUNCTION ' + QuoteIdentifier(FuncName) + ';';
end;

function TInterBaseHelpers.GenerateDropTableSQL(const TableName:String): string;
begin
  Result := 'DROP TABLE ' + QuoteIdentifier(TableName) + ';';
end;

function TInterBaseHelpers.GenerateDropTrigger(const Trigger: string): string;
begin
  Result := 'DROP TRIGGER ' + QuoteIdentifier(Trigger) + ';';
end;

function TInterBaseHelpers.GenerateDropView(const View: string): string;
begin
  Result := 'DROP VIEW ' + QuoteIdentifier(View) + ';';
end;

function TInterBaseHelpers.GenerateDropIndexSQL(const TableName,
  IndexName: string): string;
begin
  // InterBase no necesita especificar la tabla para DROP INDEX
  // Los índices PRIMARY KEY se eliminan con ALTER TABLE DROP CONSTRAINT
  if StartsText('PK_', UpperCase(IndexName)) then
    Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
              ' DROP CONSTRAINT ' + QuoteIdentifier(IndexName) + ';'
  else
    Result := 'DROP INDEX ' + QuoteIdentifier(IndexName) + ';';
end;

function TInterBaseHelpers.GenerateDropProcedure(const Proc: string): string;
begin
  Result := 'DROP PROCEDURE ' + QuoteIdentifier(Proc) + ';';
end;

function TInterBaseHelpers.GenerateIndexDefinition(const TableName: string;
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
              ' ADD CONSTRAINT ' + QuoteIdentifier('PK_' + TableName) +
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

function TInterBaseHelpers.GenerateModifyColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
begin
  // InterBase usa ALTER TABLE ALTER COLUMN para modificar
  // Nota: InterBase tiene limitaciones, no todos los cambios son posibles
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' ALTER COLUMN ' + QuoteIdentifier(ColumnInfo.ColumnName) +
            ' TYPE ' + ColumnInfo.DataType + ';';
  
  // Para cambiar NULL/NOT NULL se requiere sentencia separada
  if SameText(ColumnInfo.IsNullable, 'NO') then
    Result := Result + sLineBreak +
              'UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = 1 ' +
              'WHERE RDB$FIELD_NAME = ''' + UpperCase(ColumnInfo.ColumnName) + ''' ' +
              'AND RDB$RELATION_NAME = ''' + UpperCase(TableName) + ''';'
  else
    Result := Result + sLineBreak +
              'UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = NULL ' +
              'WHERE RDB$FIELD_NAME = ''' + UpperCase(ColumnInfo.ColumnName) + ''' ' +
              'AND RDB$RELATION_NAME = ''' + UpperCase(TableName) + ''';';
end;

function TInterBaseHelpers.GenerateCreateSequence(const GeneratorName: string): string;
begin
  Result := 'CREATE GENERATOR ' + QuoteIdentifier(GeneratorName) + ';';
end;

function TInterBaseHelpers.GenerateDropSequence(const GeneratorName: string): string;
begin
  Result := 'DROP GENERATOR ' + QuoteIdentifier(GeneratorName) + ';';
end;

end.