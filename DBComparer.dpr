program DBComparerConsole;

{$APPTYPE CONSOLE}
uses
  System.SysUtils, System.Classes, Data.DB, Uni, MySQLUniProvider,
  System.Generics.Collections, system.StrUtils;
type
  TColumnInfo = record
    ColumnName: string;
    DataType: string;
    IsNullable: string;
    ColumnKey: string;
    Extra: string;
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
  TTableInfo = class
    TableName: string;
    Columns: TList<TColumnInfo>;
    constructor Create;
    destructor Destroy; override;
  end;
  TDBComparer = class
  private
    FConn1, FConn2: TUniConnection;
    FScript: TStringList;
    function GetTables(Conn: TUniConnection; const DBName: string): TStringList;
    function GetTableStructure(Conn: TUniConnection;
                               const DBName, TableName: string): TTableInfo;
    function GetTableIndexes(Conn: TUniConnection;
                             const DBName, TableName: string): TArray<TIndexInfo>;
    function GetViews(Conn: TUniConnection; const DBName: string): TStringList;
    function GetViewDefinition(Conn: TUniConnection;
                               const DBName, ViewName: string): string;
    function GetProcedures(Conn: TUniConnection;
                           const DBName: string): TStringList;
    function GetProcedureDefinition(Conn: TUniConnection;
                                    const DBName, ProcName: string): string;
    procedure CompareTables(const DB1, DB2: string);
    procedure CompareIndexes(Conn1, Conn2: TUniConnection;
                             const DB1, DB2, TableName: string);
    procedure CompareViews(const DB1, DB2: string);
    procedure CompareProcedures(const DB1, DB2: string);
    function ColumnsAreEqual(const Col1, Col2: TColumnInfo): Boolean;
    function IndexesAreEqual(const Idx1, Idx2: TIndexInfo): Boolean;
    function GenerateColumnDefinition(const Col: TColumnInfo): string;
    function GenerateIndexDefinition(const TableName: string;
                                     const Idx: TIndexInfo): string;
  public
    constructor Create(const Server1, User1, Pass1, Port1, DB1: string;
                       const Server2, User2, Pass2, Port2, DB2: string);
    destructor Destroy; override;
    function GenerateScript(const DB1, DB2: string): string;
  end;
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
{ TDBComparer }
constructor TDBComparer.Create(const Server1, User1, Pass1, Port1, DB1: string;
                               const Server2, User2, Pass2, Port2, DB2: string);
begin
  FScript := TStringList.Create;
  // Conexión 1
  FConn1 := TUniConnection.Create(nil);
  FConn1.ProviderName := 'MySQL';
  FConn1.Server := Server1;
  FConn1.Port := StrToIntDef(Port1, 3306);
  FConn1.Username := User1;
  FConn1.Password := Pass1;
  FConn1.Database := 'information_schema';
  FConn1.Connected := True;
  // Conexión 2
  FConn2 := TUniConnection.Create(nil);
  FConn2.ProviderName := 'MySQL';
  FConn2.Server := Server2;
  FConn2.Port := StrToIntDef(Port2, 3306);
  FConn2.Username := User2;
  FConn2.Password := Pass2;
  FConn2.Database := 'information_schema';
  FConn2.Connected := True;
end;

destructor TDBComparer.Destroy;
begin
  FConn1.Free;
  FConn2.Free;
  FScript.Free;
  inherited;
end;

function TDBComparer.GetTables(Conn: TUniConnection;
                               const DBName: string): TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := Conn;
    Query.SQL.Text := 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ' +
                      'WHERE TABLE_SCHEMA = ' + QuotedStr(DBName) +
                      '  AND TABLE_TYPE = ''BASE TABLE'' ' +
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

function TDBComparer.GetTableStructure(Conn: TUniConnection;
                                       const DBName,
                                             TableName: string): TTableInfo;
var
  Query: TUniQuery;
  Col: TColumnInfo;
begin
  Result := TTableInfo.Create;
  Result.TableName := TableName;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := Conn;
    Query.SQL.Text := 'SELECT COLUMN_NAME, ' +
                      '       COLUMN_TYPE, ' +
                      '       IS_NULLABLE, ' +
                      '       COLUMN_KEY, ' +
                      '       EXTRA, ' +
                      '       COLUMN_DEFAULT, ' +
                      '       CHARACTER_MAXIMUM_LENGTH, ' +
                      '       COLUMN_COMMENT ' +
                      '  FROM INFORMATION_SCHEMA.COLUMNS ' +
                      ' WHERE TABLE_SCHEMA = ' + QuotedStr(DBName) +
                      '   AND TABLE_NAME = ' + QuotedStr(TableName) + ' ' +
                      'ORDER BY ORDINAL_POSITION';
    Query.Open;
    while not Query.Eof do
    begin
      Col.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
      Col.DataType := Query.FieldByName('COLUMN_TYPE').AsString;
      Col.IsNullable := Query.FieldByName('IS_NULLABLE').AsString;
      Col.ColumnKey := Query.FieldByName('COLUMN_KEY').AsString;
      Col.Extra := Query.FieldByName('EXTRA').AsString;
      if not Query.FieldByName('COLUMN_DEFAULT').IsNull then
        Col.ColumnDefault := Query.FieldByName('COLUMN_DEFAULT').AsString
      else
        Col.ColumnDefault := '';
      if not Query.FieldByName('CHARACTER_MAXIMUM_LENGTH').IsNull then
        Col.CharMaxLength :=
                          Query.FieldByName('CHARACTER_MAXIMUM_LENGTH').AsString
      else
        Col.CharMaxLength := '';
      if not Query.FieldByName('COLUMN_COMMENT').IsNull then
        Col.ColumnComment := Query.FieldByName('COLUMN_COMMENT').AsString
      else
        Col.ColumnComment := '';
      Result.Columns.Add(Col);
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TDBComparer.GetTableIndexes(Conn: TUniConnection;
                                     const DBName,
                                           TableName: string):
                                                             TArray<TIndexInfo>;
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
      Query.Connection := Conn;
      Query.SQL.Text := 'SELECT INDEX_NAME, ' +
                        '       NON_UNIQUE, ' +
                        '       COLUMN_NAME, ' +
                        '       SEQ_IN_INDEX ' +
                        '  FROM INFORMATION_SCHEMA.STATISTICS ' +
                        ' WHERE TABLE_SCHEMA = ' + QuotedStr(DBName) +
                        '   AND TABLE_NAME = ' + QuotedStr(TableName) + ' ' +
                        'ORDER BY INDEX_NAME, SEQ_IN_INDEX';
      Query.Open;
      LastIndexName := '';
      while not Query.Eof do
      begin
        if Query.FieldByName('INDEX_NAME').AsString <> LastIndexName then
        begin
          // Guardar índice anterior
          if LastIndexName <> '' then
          begin
            CurrentIndex.Columns := ColList.ToArray;
            IndexList.Add(CurrentIndex);
            ColList.Clear;
          end;
          // Nuevo índice
          LastIndexName := Query.FieldByName('INDEX_NAME').AsString;
          CurrentIndex.IndexName := LastIndexName;
          CurrentIndex.IsPrimary := (LastIndexName = 'PRIMARY');
          CurrentIndex.IsUnique :=
                                (Query.FieldByName('NON_UNIQUE').AsInteger = 0);
        end;
        // Agregar columna al índice
        IndexCol.ColumnName := Query.FieldByName('COLUMN_NAME').AsString;
        IndexCol.SeqInIndex := Query.FieldByName('SEQ_IN_INDEX').AsInteger;
        ColList.Add(IndexCol);
        Query.Next;
      end;
      // Guardar último índice
      if LastIndexName <> '' then
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

function TDBComparer.GetViews(Conn: TUniConnection;
                              const DBName: string): TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := Conn;
    Query.SQL.Text := 'SELECT TABLE_NAME' +
                      '  FROM INFORMATION_SCHEMA.VIEWS ' +
                      ' WHERE TABLE_SCHEMA = ' + QuotedStr(DBName) + ' ' +
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

function TDBComparer.GetViewDefinition(Conn: TUniConnection;
                                       const DBName, ViewName: string): string;
var
  Query: TUniQuery;
  OldDB: string;
  ViewDef: string;
  PosDefiner, PosSQL: Integer;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := Conn;
    OldDB := Conn.Database;
    Conn.Database := DBName;
    Query.SQL.Text := 'SHOW CREATE VIEW `' + ViewName + '`';
    Query.Open;
    ViewDef := Query.Fields[1].AsString;

    // Eliminar la cláusula DEFINER
    PosDefiner := Pos('DEFINER=', UpperCase(ViewDef));
    if PosDefiner > 0 then
    begin
      // Buscar el siguiente espacio o la palabra SQL después del DEFINER
      PosSQL := PosEx('SQL', UpperCase(ViewDef), PosDefiner);
      if PosSQL > 0 then
      begin
        // Eliminar desde DEFINER hasta justo antes de SQL SECURITY
        ViewDef := Copy(ViewDef, 1, PosDefiner - 1) +
                   Copy(ViewDef, PosSQL, Length(ViewDef));
      end;
    end;

    Result := Trim(ViewDef);
    Conn.Database := OldDB;
  finally
    Query.Free;
  end;
end;

function TDBComparer.GetProcedures(Conn: TUniConnection;
                                   const DBName: string): TStringList;
var
  Query: TUniQuery;
begin
  Result := TStringList.Create;
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := Conn;
    Query.SQL.Text := 'SELECT ROUTINE_NAME '+
                      '  FROM INFORMATION_SCHEMA.ROUTINES ' +
                      ' WHERE ROUTINE_SCHEMA = ' + QuotedStr(DBName) + ' ' +
                      'ORDER BY ROUTINE_NAME';
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

function TDBComparer.GetProcedureDefinition(Conn: TUniConnection;
                                            const DBName,
                                            ProcName: string): string;
var
  Query: TUniQuery;
  OldDB: string;
  ProcDef: string;
  PosDefiner, PosProcedure: Integer;
begin
  Query := TUniQuery.Create(nil);
  try
    Query.Connection := Conn;
    OldDB := Conn.Database;
    Conn.Database := DBName;
    Query.SQL.Text := 'SHOW CREATE PROCEDURE `' + ProcName + '`';
    Query.Open;
    ProcDef := Query.Fields[2].AsString;

    // Eliminar la cláusula DEFINER
    PosDefiner := Pos('DEFINER=', UpperCase(ProcDef));
    if PosDefiner > 0 then
    begin
      // Buscar la palabra PROCEDURE después del DEFINER
      PosProcedure := PosEx('PROCEDURE', UpperCase(ProcDef), PosDefiner);
      if PosProcedure > 0 then
      begin
        // Eliminar desde DEFINER hasta justo antes de PROCEDURE
        ProcDef := Copy(ProcDef, 1, PosDefiner - 1) +
                   Copy(ProcDef, PosProcedure, Length(ProcDef));
      end;
    end;

    Result := Trim(ProcDef);
    Conn.Database := OldDB;
  finally
    Query.Free;
  end;
end;

function TDBComparer.ColumnsAreEqual(const Col1, Col2: TColumnInfo): Boolean;

  // FUNCIÓN AUXILIAR DE NORMALIZACIÓN CORREGIDA
  function NormalizeType(const AType: string): string;
  var
    S: string;
    PStart, PEnd: Integer;
  begin
    S := LowerCase(Trim(AType));
    // Solo normalizamos la anchura para los tipos ENTEROS
    //(donde es solo display width).
    // Mantenemos la precisión para VARCHAR, DECIMAL, etc.
    //ya que es estructural.
    if StartsText('int', S) or
       StartsText('tinyint', S) or
       StartsText('smallint', S) then
    begin
      PStart := Pos('(', S);
      PEnd := Pos(')', S);
      // Si encontramos (XX), lo eliminamos para normalizar
      if (PStart > 0) and (PEnd > PStart) then
      begin
        // Resultado es la parte antes de '(' + la parte después de ')'
        S := Copy(S, 1, PStart - 1) + Copy(S, PEnd + 1, MaxInt);
      end;
    end;
    // Eliminamos cualquier espacio sobrante
    // (ej: 'int unsigned' -> 'intunsigned')
    Result := StringReplace(S, ' ', '', [rfReplaceAll]);
  end;
  // FUNCIÓN AUXILIAR PARA NORMALIZAR EXTRA
  function NormalizeExtra(const AExtra: string): string;
  begin
    Result := LowerCase(Trim(AExtra));
  end;
var
  Typ1, Typ2: string;
  Null1, Null2, Key1, Key2, Extra1, Extra2, Def1, Def2: string;
  IsAutoInc: Boolean;
begin
  // --- 1. NORMALIZAR TODOS LOS COMPONENTES ---
  // A partir de aquí, Typ1/Typ2 SOLO habrán perdido el (X) si eran tipos INT.
  Typ1 := NormalizeType(Col1.DataType);
  Typ2 := NormalizeType(Col2.DataType);
  Null1 := LowerCase(Trim(Col1.IsNullable));
  Null2 := LowerCase(Trim(Col2.IsNullable));
  Key1 := LowerCase(Trim(Col1.ColumnKey));
  Key2 := LowerCase(Trim(Col2.ColumnKey));
  Extra1 := NormalizeExtra(Col1.Extra);
  Extra2 := NormalizeExtra(Col2.Extra);
  Def1 := Trim(Col1.ColumnDefault);
  Def2 := Trim(Col2.ColumnDefault);
  // 2. Comparación básica de atributos normalizados
  Result := (Typ1 = Typ2) and // Ahora varchar(255) <> varchar(20)
            (Null1 = Null2) and
            (Key1 = Key2) and
            (Extra1 = Extra2);
  // 3. TRATAMIENTO DE DEFAULT
  if Result then
  begin
    IsAutoInc := (Pos('auto_increment', Extra1) > 0)
                  or (Pos('auto_increment', Extra2) > 0);
    if not IsAutoInc then
    begin
      // Normalizamos: Si el default es 'NULL' (como cadena),
      //lo tratamos como vacío para la comparación
      if SameText(Def1, 'NULL') then Def1 := '';
      if SameText(Def2, 'NULL') then Def2 := '';
      if not SameText(Def1, Def2) then
        Result := False;
    end;
  end;
  // 4. Comentario
  if Result then
    Result := SameText(Col1.ColumnComment, Col2.ColumnComment);
end;

function TDBComparer.IndexesAreEqual(const Idx1, Idx2: TIndexInfo): Boolean;
var
  i: Integer;
begin
  Result := (Idx1.IndexName = Idx2.IndexName) and
            (Idx1.IsUnique = Idx2.IsUnique) and
            (Idx1.IsPrimary = Idx2.IsPrimary) and
            (Length(Idx1.Columns) = Length(Idx2.Columns));
  if Result then
  begin
    for i := 0 to High(Idx1.Columns) do
    begin
      if (Idx1.Columns[i].ColumnName <> Idx2.Columns[i].ColumnName) or
         (Idx1.Columns[i].SeqInIndex <> Idx2.Columns[i].SeqInIndex) then
      begin
        Result := False;
        Break;
      end;
    end;
  end;
end;

function TDBComparer.GenerateColumnDefinition(const Col: TColumnInfo): string;
begin
  Result := '`' + Col.ColumnName + '` ' + Col.DataType;
  if Col.IsNullable = 'NO' then
    Result := Result + ' NOT NULL'
  else
    Result := Result + ' NULL';
  if (Col.ColumnDefault <> '') and (Col.ColumnDefault <> 'NULL') then
  begin
    if (Pos('CURRENT_TIMESTAMP', UpperCase(Col.ColumnDefault)) > 0) or
       (Pos('NOW()', UpperCase(Col.ColumnDefault)) > 0) then
      Result := Result + ' DEFAULT ' + Col.ColumnDefault
    else
      Result := Result + ' DEFAULT ' + QuotedStr(Col.ColumnDefault);
  end
  else if Col.ColumnDefault = 'NULL' then
    Result := Result + ' DEFAULT NULL';
  if Pos('auto_increment', LowerCase(Col.Extra)) > 0 then
    Result := Result + ' AUTO_INCREMENT';
  if Pos('on update', LowerCase(Col.Extra)) > 0 then
    Result := Result + ' ON UPDATE CURRENT_TIMESTAMP';
  if Col.ColumnComment <> '' then
    Result := Result + ' COMMENT ' + QuotedStr(Col.ColumnComment);
end;

function TDBComparer.GenerateIndexDefinition(const TableName: string;
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
    ColNames := ColNames + '`' + Idx.Columns[i].ColumnName + '`';
  end;
  if Idx.IsPrimary then
    Result := 'ALTER TABLE `' + TableName +
              '` ADD PRIMARY KEY (' + ColNames + ')'
  else if Idx.IsUnique then
    Result := 'ALTER TABLE `' + TableName +
              '` ADD UNIQUE INDEX `' + Idx.IndexName + '` (' + ColNames + ')'
  else
    Result := 'ALTER TABLE `' + TableName +
              '` ADD INDEX `' + Idx.IndexName + '` (' + ColNames + ')';
end;

procedure TDBComparer.CompareIndexes(Conn1, Conn2: TUniConnection;
                                     const DB1, DB2, TableName: string);
var
  Indexes1, Indexes2: TArray<TIndexInfo>;
  i, j: Integer;
  Found: Boolean;
begin
  Indexes1 := GetTableIndexes(Conn1, DB1, TableName);
  Indexes2 := GetTableIndexes(Conn2, DB2, TableName);
  // Índices que existen en DB2 pero no en DB1 (eliminar)
  for i := 0 to High(Indexes2) do
  begin
    if Indexes2[i].IsPrimary then
      Continue; // No eliminar PRIMARY KEY automáticamente
    Found := False;
    for j := 0 to High(Indexes1) do
    begin
      if Indexes1[j].IndexName = Indexes2[i].IndexName then
      begin
        Found := True;
        Break;
      end;
    end;
    if not Found then
    begin
      FScript.Add('-- Eliminar índice: ' + TableName + '.'
                                                       + Indexes2[i].IndexName);
      FScript.Add('ALTER TABLE `' + TableName + '` DROP INDEX `'
                                                + Indexes2[i].IndexName + '`;');
      FScript.Add('');
    end;
  end;
  // Índices nuevos o modificados
  for i := 0 to High(Indexes1) do
  begin
    Found := False;
    for j := 0 to High(Indexes2) do
    begin
      if Indexes1[i].IndexName = Indexes2[j].IndexName then
      begin
        Found := True;
        // Comparar definición
        if not IndexesAreEqual(Indexes1[i], Indexes2[j]) then
        begin
          FScript.Add('-- Modificar índice: ' + TableName + '.' +
                                                         Indexes1[i].IndexName);
          if not Indexes1[i].IsPrimary then
            FScript.Add('ALTER TABLE `' + TableName + '` DROP INDEX `'
                                                + Indexes1[i].IndexName + '`;');
          FScript.Add(GenerateIndexDefinition(TableName, Indexes1[i]) + ';');
          FScript.Add('');
        end;
        Break;
      end;
    end;
    // Índice nuevo
    if not Found then
    begin
      FScript.Add('-- Agregar índice: ' + TableName + '.' +
                                                         Indexes1[i].IndexName);
      FScript.Add(GenerateIndexDefinition(TableName, Indexes1[i]) + ';');
      FScript.Add('');
    end;
  end;
end;

procedure TDBComparer.CompareTables(const DB1, DB2: string);
var
  Tables1, Tables2: TStringList;
  i, j, k: Integer;
  Table1, Table2: TTableInfo;
  Found: Boolean;
  Col1, Col2: TColumnInfo;
begin
  Tables1 := GetTables(FConn1, DB1);
  Tables2 := GetTables(FConn2, DB2);
  try
    FScript.Add('-- ========================================');
    FScript.Add('-- COMPARACIÓN DE TABLAS');
    FScript.Add('-- ========================================');
    FScript.Add('');
    for i := 0 to Tables2.Count - 1 do
    begin
      if Tables1.IndexOf(Tables2[i]) = -1 then
      begin
        FScript.Add('-- Tabla eliminada: ' + Tables2[i]);
        FScript.Add('DROP TABLE IF EXISTS `' + Tables2[i] + '`;');
        FScript.Add('');
      end;
    end;
    for i := 0 to Tables1.Count - 1 do
    begin
      if Tables2.IndexOf(Tables1[i]) = -1 then
      begin
        FScript.Add('-- Tabla nueva: ' + Tables1[i]);
        Table1 := GetTableStructure(FConn1, DB1, Tables1[i]);
        try
          FScript.Add('CREATE TABLE `' + Tables1[i] + '` (');
          for j := 0 to Table1.Columns.Count - 1 do
          begin
            Col1 := Table1.Columns[j];
            if j < Table1.Columns.Count - 1 then
              FScript.Add('  ' + GenerateColumnDefinition(Col1) + ',')
            else
              FScript.Add('  ' + GenerateColumnDefinition(Col1));
          end;
          FScript.Add(');');
          FScript.Add('');
        finally
          Table1.Free;
        end;
      end
      else
      begin
        Table1 := GetTableStructure(FConn1, DB1, Tables1[i]);
        Table2 := GetTableStructure(FConn2, DB2, Tables1[i]);
        try
          for j := 0 to Table1.Columns.Count - 1 do
          begin
            Col1 := Table1.Columns[j];
            Found := False;
            for k := 0 to Table2.Columns.Count - 1 do
            begin
              if Table2.Columns[k].ColumnName = Col1.ColumnName then
              begin
                Found := True;
                Col2 := Table2.Columns[k];
                if not ColumnsAreEqual(Col1, Col2) then
                begin
                  FScript.Add('-- Modificar columna: ' + Tables1[i] + '.'
                                                             + Col1.ColumnName);
                  FScript.Add('ALTER TABLE `' + Tables1[i] +
                              '` MODIFY COLUMN ' +
                             GenerateColumnDefinition(Col1) + ';');
                  FScript.Add('');
                end;
                Break;
              end;
            end;
            if not Found then
            begin
              FScript.Add('-- Agregar columna: ' + Tables1[i] + '.'
                                                             + Col1.ColumnName);
              FScript.Add('ALTER TABLE `' + Tables1[i] + '` ADD COLUMN ' +
                         GenerateColumnDefinition(Col1) + ';');
              FScript.Add('');
            end;
          end;
          for j := 0 to Table2.Columns.Count - 1 do
          begin
            Col2 := Table2.Columns[j];
            Found := False;
            for k := 0 to Table1.Columns.Count - 1 do
            begin
              if Table1.Columns[k].ColumnName = Col2.ColumnName then
              begin
                Found := True;
                Break;
              end;
            end;
            if not Found then
            begin
              FScript.Add('-- Eliminar columna: ' + Tables1[i] + '.'
                                                             + Col2.ColumnName);
              FScript.Add('ALTER TABLE `' + Tables1[i] +
                          '` DROP COLUMN `' + Col2.ColumnName + '`;');
              FScript.Add('');
            end;
          end;
        finally
          Table1.Free;
          Table2.Free;
        end;
        // Comparar índices de la tabla
        CompareIndexes(FConn1, FConn2, DB1, DB2, Tables1[i]);
      end;
    end;
  finally
    Tables1.Free;
    Tables2.Free;
  end;
end;

procedure TDBComparer.CompareViews(const DB1, DB2: string);
var
  Views: TStringList;
  i: Integer;
  ViewDef: string;
begin
  Views := GetViews(FConn1, DB1);
  try
    FScript.Add('-- ========================================');
    FScript.Add('-- VISTAS (DROP + CREATE)');
    FScript.Add('-- ========================================');
    FScript.Add('');
    for i := 0 to Views.Count - 1 do
    begin
      FScript.Add('DROP VIEW IF EXISTS `' + Views[i] + '`;');
      FScript.Add('');
      ViewDef := GetViewDefinition(FConn1, DB1, Views[i]);
      FScript.Add(ViewDef + ';');
      FScript.Add('');
    end;
  finally
    Views.Free;
  end;
end;

procedure TDBComparer.CompareProcedures(const DB1, DB2: string);
var
  Procedures: TStringList;
  i: Integer;
  ProcDef: string;
begin
  Procedures := GetProcedures(FConn1, DB1);
  try
    FScript.Add('-- ========================================');
    FScript.Add('-- PROCEDIMIENTOS (DROP + CREATE)');
    FScript.Add('-- ========================================');
    FScript.Add('');
    for i := 0 to Procedures.Count - 1 do
    begin
      FScript.Add('DROP PROCEDURE IF EXISTS `' + Procedures[i] + '`;');
      FScript.Add('');
      FScript.Add('DELIMITER $$');
      FScript.Add('');
      ProcDef := GetProcedureDefinition(FConn1, DB1, Procedures[i]);
      FScript.Add(ProcDef + ' $$');
      FScript.Add('');
      FScript.Add('DELIMITER ;');
      FScript.Add('');
    end;
  finally
    Procedures.Free;
  end;
end;

function TDBComparer.GenerateScript(const DB1, DB2: string): string;
begin
  FScript.Clear;
  FScript.Add('-- ========================================');
  FScript.Add('-- SCRIPT DE SINCRONIZACIÓN');
  FScript.Add('-- Base Origen: ' + DB1);
  FScript.Add('-- Base Destino: ' + DB2);
  FScript.Add('-- Generado: ' + DateTimeToStr(Now));
  FScript.Add('-- ========================================');
  FScript.Add('');
  FScript.Add('USE `' + DB2 + '`;');
  FScript.Add('');
  FScript.Add('SET FOREIGN_KEY_CHECKS = 0;');
  FScript.Add('');
  CompareTables(DB1, DB2);
  CompareViews(DB1, DB2);
  CompareProcedures(DB1, DB2);
  FScript.Add('SET FOREIGN_KEY_CHECKS = 1;');
  FScript.Add('');
  Result := FScript.Text;
end;
// ============================================================================
// PROGRAMA PRINCIPAL
// ============================================================================
procedure ShowUsage;
begin
  Writeln('Uso:');
  Writeln('  DBComparer servidor1:puerto1\database1 usuario1\password1 '+
          'servidor2:puerto2\database2 usuario2\password2');
  Writeln('');
  Writeln('Ejemplo:');
  Writeln('  DBComparer localhost:3306\midb_prod root\pass123 '+
          'localhost:3306\midb_dev root\pass456');
  Writeln('');
  Writeln('El resultado se imprime por la salida estándar. '+
          'Para guardarlo en archivo:');
  Writeln('  DBComparer ... > script.sql');
  Writeln('');
  Halt(1);
end;

procedure ParseConnectionString(const ConnStr: string;
                                out Server, Port, Database: string);
var
  Parts: TArray<string>;
  ServerPort: TArray<string>;
begin
  Parts := ConnStr.Split(['\']);
  if Length(Parts) <> 2 then
    raise Exception.Create('Formato incorrecto. '+
                           'Use: servidor:puerto\database');
  ServerPort := Parts[0].Split([':']);
  if Length(ServerPort) = 2 then
  begin
    Server := ServerPort[0];
    Port := ServerPort[1];
  end
  else
  begin
    Server := Parts[0];
    Port := '3306';
  end;
  Database := Parts[1];
end;

procedure ParseCredentials(const CredStr: string; out User, Password: string);
var
  Parts: TArray<string>;
begin
  Parts := CredStr.Split(['\']);
  if Length(Parts) <> 2 then
    raise Exception.Create('Formato incorrecto. Use: usuario\password');
  User := Parts[0];
  Password := Parts[1];
end;
var
  Comparer: TDBComparer;
  Server1, Port1, DB1, User1, Pass1: string;
  Server2, Port2, DB2, User2, Pass2: string;
  Script: string;
begin
  try
    if ParamCount <> 4 then
      ShowUsage;
    // Parsear parámetros
    ParseConnectionString(ParamStr(1), Server1, Port1, DB1);
    ParseCredentials(ParamStr(2), User1, Pass1);
    ParseConnectionString(ParamStr(3), Server2, Port2, DB2);
    ParseCredentials(ParamStr(4), User2, Pass2);
    Writeln(ErrOutput, 'Conectando a servidores...');
    Writeln(ErrOutput, 'Origen: ' + Server1 + ':' + Port1 + '\' + DB1);
    Writeln(ErrOutput, 'Destino: ' + Server2 + ':' + Port2 + '\' + DB2);
    Writeln(ErrOutput, '');
    // Crear comparador
    Comparer := TDBComparer.Create(
      Server1, User1, Pass1, Port1, DB1,
      Server2, User2, Pass2, Port2, DB2
    );
    try
      Writeln(ErrOutput, 'Generando script de comparación...');
      Script := Comparer.GenerateScript(DB1, DB2);
      // Imprimir por salida estándar
      Write(Script);
      Writeln(ErrOutput, '');
      Writeln(ErrOutput, 'Script generado exitosamente.');
    finally
      Comparer.Free;
    end;
  except
    on E: Exception do
    begin
      Writeln(ErrOutput, 'ERROR: ' + E.Message);
      Halt(1);
    end;
  end;
end.
