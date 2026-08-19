unit Core.Engine;

interface
uses Core.Interfaces, Core.Types, System.Classes, Core.Helpers, Core.Resources,
     Generics.Collections, Providers.MySQL.Helpers, Data.DB, System.StrUtils;
type
  TDBComparerEngine = class
  private
    FSourceDB: IDBMetadataProvider;
    FTargetDB: IDBMetadataProvider;
    FWriter: IScriptWriter;
    FOptions: TComparerOptions;
    FHelpers: IDBHelpers;
    procedure CompareTableStructure(const TableName: string);
    procedure CompareTableIndexes(const TableName: string);
    procedure CompareTables;
    procedure CompareViews;
    procedure CompareTriggers;
    procedure CompareProcedures;
    procedure CompareFunctions;
    procedure CompareSequences;
    procedure CreateNewTable(const TableName: string);
    procedure CompareData(const TableName: string);
    procedure CopyAllData(const TableName: string);
    function SortViewsByDependencies(const SourceViews: TStringList;
      const ViewDefinitions: TDictionary<string, string>): TStringList;
    function ViewDefinitionReferences(const Definition, ViewName: string): Boolean;
    function BuildWhereClause(const PKCols: TStringList; DataSet: TDataSet): string;
  public
    constructor Create(Source, Target: IDBMetadataProvider;
                       Writer: IScriptWriter;
                       Helpers: IDBHelpers;
                       Options: TComparerOptions);
    procedure GenerateScript;
  end;
implementation
uses
  System.SysUtils, Providers.MySQL;
constructor TDBComparerEngine.Create(Source, Target: IDBMetadataProvider;
                                     Writer: IScriptWriter;
                                     Helpers: IDBHelpers;
                                     Options: TComparerOptions);
begin
  FSourceDB := Source;
  FTargetDB := Target;
  FWriter := Writer;
  FOptions := Options;
  FHelpers := Helpers;
end;
procedure TDBComparerEngine.CompareSequences;
var
  SourceSeqs, TargetSeqs: TStringList;
  i: Integer;
begin
  // Obtenemos listas de ambos lados
  SourceSeqs := FSourceDB.GetSequences;
  TargetSeqs := FTargetDB.GetSequences;
  try
    // Normalizamos para comparaciones (opcional, pero recomendado)
    SourceSeqs.CaseSensitive := False;
    TargetSeqs.CaseSensitive := False;
    if (SourceSeqs.Count > 0) or ((TargetSeqs.Count > 0) and not FOptions.NoDelete) then
      FWriter.AddComment(TRes.MsgHeaderSequences);
    // 1. CREAR: Existe en Origen, pero NO en Destino
    for i := 0 to SourceSeqs.Count - 1 do
    begin
      if TargetSeqs.IndexOf(SourceSeqs[i]) = -1 then
      begin
        FWriter.AddComment(TRes.MsgSeqCreate + SourceSeqs[i]);
        FWriter.AddCommand(FHelpers.GenerateCreateSequence(SourceSeqs[i]));
      end;
      // NOTA: Si ya existe, NO la tocamos.
      // Recrearla reiniciaría el contador a 1, lo cual es peligroso en producción.
    end;
    // 2. BORRAR: Existe en Destino, pero NO en Origen (si --nodelete no está activo)
    if not FOptions.NoDelete then
    begin
      for i := 0 to TargetSeqs.Count - 1 do
      begin
        if SourceSeqs.IndexOf(TargetSeqs[i]) = -1 then
        begin
          FWriter.AddComment(TRes.MsgSeqDrop + TargetSeqs[i]);
          FWriter.AddCommand(FHelpers.GenerateDropSequence(TargetSeqs[i]));
        end;
      end;
    end;
  finally
    SourceSeqs.Free;
    TargetSeqs.Free;
  end;
end;
procedure TDBComparerEngine.CreateNewTable(const TableName: string);
var
  Table: TTableInfo;
  Indexes: TArray<TIndexInfo>;
  Idx: TIndexInfo;
  // Variables eliminadas: i, PKList, ColDef (ya no hacen falta aquí)
begin
  FWriter.AddComment(TRes.MsgNewTable + TableName);
  // 1. Obtener la estructura y los índices desde el Origen
  Table := FSourceDB.GetTableStructure(TableName);
  Indexes := FSourceDB.GetTableIndexes(TableName);
  try
    // -----------------------------------------------------------------------
    // CORRECCIÓN: Delegar al Provider (MySQL, Firebird, etc)
    // -----------------------------------------------------------------------
    // En lugar de construir el string manualmente aquí, llamamos a la interfaz.
    // Esto ejecutará TMySQLHelpers.GenerateCreateTableSQL
    FWriter.AddCommand(FHelpers.GenerateCreateTableSQL(Table, Indexes));
    // -----------------------------------------------------------------------
    // 3. Crear índices secundarios (No Primary Key)
    // -----------------------------------------------------------------------
    // Nota: GenerateCreateTableSQL (en MySQL Helper) incluye la Primary Key,
    // pero habitualmente los índices secundarios se agregan después
    // o el helper de creación de tabla solo devuelve el 'CREATE TABLE'.
    // Mantenemos este bucle para asegurar que se crean los índices UNIQUE/KEY.
    if not FOptions.MariaDB10Compat then
      for Idx in Indexes do
      begin
        if not Idx.IsPrimary then
        begin
          FWriter.AddComment(TRes.MsgAddIndex + TableName + '.' + Idx.IndexName);
          FWriter.AddCommand(FHelpers.GenerateIndexDefinition(TableName, Idx));
        end;
      end;
  finally
    Table.Free;
    // Indexes es un array dinámico gestionado automáticamente por el compilador,
    // pero si TIndexInfo contuviera objetos habría que liberarlos.
    // Al ser Records, no es necesario liberar el array explícitamente.
  end;
end;
procedure TDBComparerEngine.GenerateScript;
begin
  FWriter.AddComment('========================================');
  FWriter.AddComment(Format(TRes.GeneratedHeader, [DateTimeToStr(Now)]));
  FWriter.AddComment('========================================');
  FWriter.AddCommand('');
  if FOptions.MariaDB10Compat then
  begin
    FWriter.AddCommand('SET @OLD_SQL_NOTES=@@SQL_NOTES;');
    FWriter.AddCommand('SET SQL_NOTES=0;');
    FWriter.AddCommand('SET NAMES utf8mb4 COLLATE utf8mb4_spanish_ci;');
    FWriter.AddCommand('SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS;');
    FWriter.AddCommand('SET FOREIGN_KEY_CHECKS=0;');
    FWriter.AddCommand('SET @OLD_SQL_MODE=@@SQL_MODE;');
    FWriter.AddCommand('SET SQL_MODE=''NO_AUTO_VALUE_ON_ZERO'';');
  end;
  CompareTables;
  CompareViews;
  CompareProcedures;
  CompareFunctions;
  if FOptions.WithTriggers then
    CompareTriggers;
  if FOptions.MariaDB10Compat then
  begin
    FWriter.AddCommand('SET SQL_MODE=@OLD_SQL_MODE;');
    FWriter.AddCommand('SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;');
    FWriter.AddCommand('SET SQL_NOTES=@OLD_SQL_NOTES;');
  end;
end;
procedure TDBComparerEngine.CompareTables;
var
  SourceTables, TargetTables: TStringList;
  i: Integer;
  SkipTable: Boolean;
begin
  SourceTables := FSourceDB.GetTables;
  TargetTables := FTargetDB.GetTables;
  try
    // 1. Tablas eliminadas (si no está --nodelete)
    if not FOptions.NoDelete then
    begin
      for i := 0 to TargetTables.Count - 1 do
      begin
        if SourceTables.IndexOf(TargetTables[i]) = -1 then
        begin
          FWriter.AddComment(TRes.MsgTableDeleted + TargetTables[i]);
          FWriter.AddCommand(FHelpers.GenerateDropTableSQL(TargetTables[i]));
        end;
      end;
    end;
    // 2. Tablas nuevas o modificadas
    for i := 0 to SourceTables.Count - 1 do
    begin
      SkipTable := False;
      if (FOptions.IncludeTables.Count > 0) and
         (FOptions.IncludeTables.IndexOf(SourceTables[i]) = -1) then
        SkipTable := True;
      if (FOptions.ExcludeTables.IndexOf(SourceTables[i]) >= 0) then
        SkipTable := True;
      if SkipTable then
        Continue;
      if TargetTables.IndexOf(SourceTables[i]) = -1 then
      begin
        // Tabla nueva - Crear completa
        CreateNewTable(SourceTables[i]);
        // Si hay opción de copiar datos, lo hacemos inmediatamente
        if FOptions.WithData or FOptions.WithDataDiff then
          CopyAllData(SourceTables[i]);
      end
      else
      begin
        // Tabla existente - Comparar estructura
        CompareTableStructure(SourceTables[i]);
      end;
    end;
    // --- INTEGRACIÓN DE DATOS ---
    if FOptions.WithData or FOptions.WithDataDiff then
    begin
       for i := 0 to SourceTables.Count - 1 do
       begin
         // Lógica de Filtro (Include/Exclude Tables)
         var SkipData := False;
         if (FOptions.IncludeTables.Count > 0) and
            (FOptions.IncludeTables.IndexOf(SourceTables[i]) = -1) then
           SkipData := True;
         if (FOptions.ExcludeTables.IndexOf(SourceTables[i]) >= 0) then
           SkipData := True;
         if not SkipData then
         begin
           // Solo procesamos aquí las tablas que ya existían en ambos lados
           if TargetTables.IndexOf(SourceTables[i]) > -1 then
           begin
             if FOptions.WithData then
               CopyAllData(SourceTables[i])
             else if FOptions.WithDataDiff then
               CompareData(SourceTables[i]);
           end;
           // Las tablas nuevas ya se procesaron arriba, junto con CreateNewTable
         end;
       end;
    end;
  finally
    SourceTables.Free;
    TargetTables.Free;
  end;
end;
procedure TDBComparerEngine.CompareTableStructure(const TableName: string);
var
  Table1, Table2: TTableInfo;
  Col1, Col2: TColumnInfo;
  FoundIdx: Integer;
  function FindColumn(List: TList<TColumnInfo>; const Name: string): Integer;
  var
    k: Integer;
  begin
    Result := -1;
    for k := 0 to List.Count - 1 do
      if SameText(List[k].ColumnName, Name) then
        Exit(k);
  end;
begin
  Table1 := FSourceDB.GetTableStructure(TableName);
  Table2 := FTargetDB.GetTableStructure(TableName);
  try
    // ---------------------------------------------------------
    // 1. Recorrer Origen (Table1) para buscar NUEVAS o MODIFICADAS
    // ---------------------------------------------------------
    for var i := 0 to Table1.Columns.Count - 1 do
    begin
      Col1 := Table1.Columns[i];
      FoundIdx := FindColumn(Table2.Columns, Col1.ColumnName);
      if FoundIdx = -1 then
      begin
        // CASO A: La columna no existe en Destino -> CREAR
        FWriter.AddComment(TRes.MsgAddColumn + TableName + '.' +
                           Col1.ColumnName);
        // El Helper se encarga del dialecto SQL (ADD COLUMN vs ADD ...)
        FWriter.AddCommand(FHelpers.GenerateAddColumnSQL(TableName, Col1));
      end
      else
      begin
        // CASO B: La columna existe -> COMPARAR
        Col2 := Table2.Columns[FoundIdx];
        // Usamos la lógica universal del Helper abstracto
        if not FHelpers.ColumnsAreEqual(Col1, Col2) then
        begin
          FWriter.AddComment(TRes.MsgModColumn + TableName + '.' +
                              Col1.ColumnName);
          FWriter.AddCommand(FHelpers.GenerateModifyColumnSQL(TableName, Col1));
        end;
      end;
    end;
    // ---------------------------------------------------------
    // 2. Recorrer Destino (Table2) para buscar ELIMINADAS
    // ---------------------------------------------------------
    if not FOptions.NoDelete then
    begin
      for var i := 0 to Table2.Columns.Count - 1 do
      begin
        Col2 := Table2.Columns[i];
        FoundIdx := FindColumn(Table1.Columns, Col2.ColumnName);
        if FoundIdx = -1 then
        begin
          // CASO C: La columna sobra en destino -> BORRAR
          FWriter.AddComment(TRes.MsgDelColumn + TableName + '.' +
                             Col2.ColumnName);
          // El Helper sabe cómo borrar (DROP COLUMN)
          FWriter.AddCommand(FHelpers.GenerateDropColumnSQL(TableName,
                                                            Col2.ColumnName));
        end;
      end;
    end;
    // ---------------------------------------------------------
    // 3. Comparar Índices (Llamada separada)
    // ---------------------------------------------------------
    CompareTableIndexes(TableName);
  finally
    Table1.Free;
    Table2.Free;
  end;
end;
procedure TDBComparerEngine.CompareTriggers;
var
  SourceTriggers, TargetTriggers: TArray<TTriggerInfo>;
  i, j: Integer;
  Found: Boolean;
  TriggerDef: string;
begin
  // Obtener los arrays de triggers (Records)
  SourceTriggers := FSourceDB.GetTriggers;
  TargetTriggers := FTargetDB.GetTriggers;
  FWriter.AddComment(TRes.MsgHeaderTrig);
  FWriter.AddCommand('');
  // 1. TRIGGERS ELIMINADOS (Existen en Destino, pero no en Origen)
  if not FOptions.NoDelete then
  begin
    for i := Low(TargetTriggers) to High(TargetTriggers) do
    begin
      Found := False;
      // Buscar si el trigger de destino existe en el origen
      for j := Low(SourceTriggers) to High(SourceTriggers) do
      begin
        if SameText(SourceTriggers[j].TriggerName, TargetTriggers[i].TriggerName) then
        begin
          Found := True;
          Break;
        end;
      end;
      if not Found then
      begin
        FWriter.AddComment(TRes.MsgTriggerDel + TargetTriggers[i].TriggerName);
        FWriter.AddCommand(FHelpers.GenerateDropTrigger(TargetTriggers[i].TriggerName));
      end;
    end;
  end;
  // 2. TRIGGERS NUEVOS O MODIFICADOS
  for i := Low(SourceTriggers) to High(SourceTriggers) do
  begin
    Found := False;
    for j := Low(TargetTriggers) to High(TargetTriggers) do
    begin
      // Comparamos por nombre
      if SameText(SourceTriggers[i].TriggerName, TargetTriggers[j].TriggerName) then
      begin
        Found := True;
        // Si existen en ambos, comparamos su contenido usando el Helper
        if not FHelpers.TriggersAreEqual(SourceTriggers[i], TargetTriggers[j]) then
        begin
          FWriter.AddComment(TRes.MsgTriggerMod + SourceTriggers[i].TriggerName);
          // Para modificar un trigger, generalmente se borra y se crea de nuevo
          FWriter.AddCommand(FHelpers.GenerateDropTrigger(
                                                SourceTriggers[i].TriggerName));
          // Obtenemos el SQL completo del trigger desde la BD origen
          TriggerDef := FSourceDB.GetTriggerDefinition(
                                                 SourceTriggers[i].TriggerName);
          // OJO: En algunos clientes MySQL se necesita 'DELIMITER $$',
          // pero UniDAC/ScriptWriter suelen manejar comandos individuales.
          // Si vas a ejecutar esto en Workbench, podrías necesitar añadir delimitadores.
          FWriter.AddCommand(TriggerDef);
        end;
        Break;
      end;
    end;
    // Si no se encontró en destino, es NUEVO
    if not Found then
    begin
      FWriter.AddComment(TRes.MsgTriggerNew + SourceTriggers[i].TriggerName);
      TriggerDef := FSourceDB.GetTriggerDefinition(
                                                 SourceTriggers[i].TriggerName);
      FWriter.AddCommand(TriggerDef);
    end;
  end;
end;
procedure TDBComparerEngine.CompareViews;
var
  SourceViews, SortedViews: TStringList;
  ViewDefinitions: TDictionary<string, string>;
  ViewDefinition: string;
  i: Integer;
begin
  SourceViews := FSourceDB.GetViews;
  ViewDefinitions := TDictionary<string, string>.Create;
  try
    for i := 0 to SourceViews.Count - 1 do
    begin
      ViewDefinition := FSourceDB.GetViewDefinition(SourceViews[i]);
      ViewDefinitions.AddOrSetValue(LowerCase(SourceViews[i]), ViewDefinition);
    end;
    SortedViews := SortViewsByDependencies(SourceViews, ViewDefinitions);
    try
      FWriter.AddComment(TRes.MsgHeaderViews);
      // Primero se borran las dependientes y despues sus dependencias.
      for i := SortedViews.Count - 1 downto 0 do
        if FOptions.PreserveViews.IndexOf(SortedViews[i]) = -1 then
          FWriter.AddCommand(FHelpers.GenerateDropView(SortedViews[i]));
      // Para crear, cada dependencia debe existir antes que quien la usa.
      for i := 0 to SortedViews.Count - 1 do
      begin
        if FOptions.PreserveViews.IndexOf(SortedViews[i]) >= 0 then
          Continue;
        FWriter.AddComment(TRes.MsgRecreateView + SortedViews[i]);
        if ViewDefinitions.TryGetValue(LowerCase(SortedViews[i]),
                                       ViewDefinition) then
          FWriter.AddCommand(ViewDefinition);
      end;
    finally
      SortedViews.Free;
    end;
  finally
    ViewDefinitions.Free;
    SourceViews.Free;
  end;
end;

function TDBComparerEngine.SortViewsByDependencies(
  const SourceViews: TStringList;
  const ViewDefinitions: TDictionary<string, string>): TStringList;
var
  VisitState: TDictionary<string, Integer>;

  procedure VisitView(const ViewName: string);
  var
    State: Integer;
    Key: string;
    Definition: string;
    i: Integer;
    DependencyName: string;
  begin
    Key := LowerCase(ViewName);
    if VisitState.TryGetValue(Key, State) then
    begin
      if State <> 0 then
        Exit;
    end;
    VisitState.AddOrSetValue(Key, 1);
    if ViewDefinitions.TryGetValue(Key, Definition) then
    begin
      for i := 0 to SourceViews.Count - 1 do
      begin
        DependencyName := SourceViews[i];
        if not SameText(DependencyName, ViewName) and
           ViewDefinitionReferences(Definition, DependencyName) then
          VisitView(DependencyName);
      end;
    end;
    VisitState.AddOrSetValue(Key, 2);
    if Result.IndexOf(ViewName) = -1 then
      Result.Add(ViewName);
  end;

var
  i: Integer;
begin
  Result := TStringList.Create;
  Result.CaseSensitive := False;
  VisitState := TDictionary<string, Integer>.Create;
  try
    for i := 0 to SourceViews.Count - 1 do
      VisitView(SourceViews[i]);
  finally
    VisitState.Free;
  end;
end;

function TDBComparerEngine.ViewDefinitionReferences(
  const Definition, ViewName: string): Boolean;
var
  LowerDefinition, LowerViewName: string;
  FoundPos, AfterPos: Integer;

  function IsIdentifierChar(const Value: Char): Boolean;
  begin
    Result := CharInSet(Value, ['A'..'Z', 'a'..'z', '0'..'9', '_', '$']);
  end;

  function ContainsQuotedView(const OpenQuote, CloseQuote: Char): Boolean;
  begin
    Result := Pos(string(OpenQuote) + LowerViewName + string(CloseQuote),
                  LowerDefinition) > 0;
  end;

begin
  Result := False;
  LowerDefinition := LowerCase(Definition);
  LowerViewName := LowerCase(ViewName);
  if ContainsQuotedView('`', '`') or
     ContainsQuotedView('"', '"') or
     ContainsQuotedView('[', ']') then
  begin
    Result := True;
    Exit;
  end;
  FoundPos := Pos(LowerViewName, LowerDefinition);
  while FoundPos > 0 do
  begin
    AfterPos := FoundPos + Length(LowerViewName);
    if ((FoundPos = 1) or
        not IsIdentifierChar(LowerDefinition[FoundPos - 1])) and
       ((AfterPos > Length(LowerDefinition)) or
        not IsIdentifierChar(LowerDefinition[AfterPos])) then
    begin
      Result := True;
      Exit;
    end;
    FoundPos := PosEx(LowerViewName, LowerDefinition, FoundPos + 1);
  end;
end;

procedure TDBComparerEngine.CopyAllData(const TableName: string);
var
  SourceData: TDataSet;
  Fields, Values: TStringList;
  i: Integer;
begin
  FWriter.AddComment(TRes.MsgCopyAllData + TableName);
  SourceData := FSourceDB.GetData(TableName);
  Fields := TStringList.Create;
  Values := TStringList.Create;
  try
    // Preparamos lista de campos una vez (asumiendo coincidencia por nombre)
    // Nota: En una versión robusta, verificaríamos que campos existen en destino
    // pero para CopyData asumimos estructura idéntica recién creada o validada.
    while not SourceData.Eof do
    begin
      Fields.Clear;
      Values.Clear;
      for i := 0 to SourceData.FieldCount - 1 do
      begin
        Fields.Add(FHelpers.QuoteIdentifier(SourceData.Fields[i].FieldName));
        Values.Add(FHelpers.ValueToSQL(SourceData.Fields[i]));
      end;
      // Usamos INSERT IGNORE o similar si fuera necesario, aquí INSERT estándar
      FWriter.AddCommand(FHelpers.GenerateInsertSQL(TableName, Fields, Values));
      SourceData.Next;
    end;
  finally
    SourceData.Free; // El provider nos dio el dataset, pero somos dueños de liberarlo
    Fields.Free;
    Values.Free;
  end;
end;

procedure TDBComparerEngine.CompareData(const TableName: string);
var
  SourceData, TargetData, TempSource: TDataSet;
  PKCols: TStringList;
  TableStruct: TTableInfo;
  Col: TColumnInfo;
  WhereClause, SetClause: string;
  Fields, Values: TStringList;
  i: Integer;
  HasIdentity: Boolean;
  // Extended insert
  FieldList: string;
  ValueRows: TStringList;
  RowsInBatch: Integer;
  BatchSize: Integer;

  procedure FlushExtendedInsert;
  var
    j: Integer;
    SQL: string;
  begin
    if ValueRows.Count = 0 then Exit;
    SQL := FieldList + sLineBreak;
    for j := 0 to ValueRows.Count - 1 do
    begin
      if j < ValueRows.Count - 1 then
        SQL := SQL + '  (' + ValueRows[j] + '),' + sLineBreak
      else
        SQL := SQL + '  (' + ValueRows[j] + ');';
    end;
    FWriter.AddCommand(SQL);
    ValueRows.Clear;
    RowsInBatch := 0;
  end;

begin
  TableStruct := FSourceDB.GetTableStructure(TableName);
  PKCols := TStringList.Create;
  ValueRows := TStringList.Create;
  try
    HasIdentity  := False;
    FieldList    := '';
    RowsInBatch  := 0;
    BatchSize    := FOptions.ExtendedInsertRows;

    for Col in TableStruct.Columns do
    begin
      if SameText(Col.ColumnKey, 'PRI') then
        PKCols.Add(Col.ColumnName);
      if ContainsText(Col.Extra, 'IDENTITY') or
         ContainsText(Col.Extra, 'AUTO_INCREMENT') then
        HasIdentity := True;
    end;

    if PKCols.Count = 0 then
    begin
      FWriter.AddComment(Format(TRes.MsgWarnNoPK, [TableName]));
      Exit;
    end;

    FWriter.AddComment(TRes.MsgSyncData + TableName +
                       IfThen(HasIdentity, TRes.MsgWithIdentity, ''));

    // =========================================================================
    // FASE A: Recorrer ORIGEN -> Insertar o Actualizar en DESTINO
    // =========================================================================
    SourceData := FSourceDB.GetData(TableName);
    Fields := TStringList.Create;
    Values := TStringList.Create;
    try
      while not SourceData.Eof do
      begin
        WhereClause := BuildWhereClause(PKCols, SourceData);
        TargetData := FTargetDB.GetData(TableName, WhereClause);
        try
          if TargetData.IsEmpty then
          begin
            // --- CASO 1: INSERT ---
            Fields.Clear;
            Values.Clear;
            for i := 0 to SourceData.FieldCount - 1 do
            begin
              Fields.Add(FHelpers.QuoteIdentifier(SourceData.Fields[i].FieldName));
              Values.Add(FHelpers.ValueToSQL(SourceData.Fields[i]));
            end;

            if FOptions.ExtendedInsert then
            begin
              // Construir cabecera una sola vez
              if FieldList = '' then
              begin
                var FieldPart := '';
                for i := 0 to Fields.Count - 1 do
                begin
                  if i > 0 then FieldPart := FieldPart + ', ';
                  FieldPart := FieldPart + Fields[i];
                end;
                FieldList := 'INSERT INTO ' + FHelpers.QuoteIdentifier(TableName) +
                             ' (' + FieldPart + ') VALUES';
              end;

              // Acumular fila
              var RowValues := '';
              for i := 0 to Values.Count - 1 do
              begin
                if i > 0 then RowValues := RowValues + ', ';
                RowValues := RowValues + Values[i];
              end;
              ValueRows.Add(RowValues);
              Inc(RowsInBatch);

              if (BatchSize > 0) and (RowsInBatch >= BatchSize) then
                FlushExtendedInsert;
            end
            else
            begin
              FWriter.AddComment(Format(TRes.MsgInsertNew, [WhereClause]));
              FWriter.AddCommand(FHelpers.GenerateInsertSQL(TableName, Fields, Values, HasIdentity));
            end;
          end
          else
          begin
            // --- CASO 2: UPDATE ---
            // Antes de un UPDATE, volcamos los INSERTs acumulados
            // para mantener el orden lógico en el script
            if FOptions.ExtendedInsert then
              FlushExtendedInsert;

            SetClause := '';
            for i := 0 to SourceData.FieldCount - 1 do
            begin
              if (PKCols.IndexOf(SourceData.Fields[i].FieldName) >= 0) then
                Continue;
              if (TargetData.FindField(SourceData.Fields[i].FieldName) <> nil) then
              begin
                if FHelpers.ValueToSQL(SourceData.Fields[i]) <>
                   FHelpers.ValueToSQL(TargetData.FieldByName(SourceData.Fields[i].FieldName)) then
                begin
                  if SetClause <> '' then
                    SetClause := SetClause + ', ';
                  SetClause := SetClause +
                               FHelpers.QuoteIdentifier(SourceData.Fields[i].FieldName) +
                               ' = ' + FHelpers.ValueToSQL(SourceData.Fields[i]);
                end;
              end;
            end;
            if SetClause <> '' then
            begin
              FWriter.AddComment(Format(TRes.MsgUpdateDiff, [WhereClause]));
              FWriter.AddCommand(FHelpers.GenerateUpdateSQL(TableName, SetClause, WhereClause));
            end;
          end;
        finally
          TargetData.Free;
        end;
        SourceData.Next;
      end;

      // Volcar los INSERTs pendientes al acabar la fase A
      if FOptions.ExtendedInsert then
        FlushExtendedInsert;

    finally
      SourceData.Free;
      Fields.Free;
      Values.Free;
    end;

    // =========================================================================
    // FASE B: Recorrer DESTINO -> Eliminar lo que sobre
    // =========================================================================
    if not FOptions.NoDelete then
    begin
      TargetData := FTargetDB.GetData(TableName);
      try
        while not TargetData.Eof do
        begin
          WhereClause := BuildWhereClause(PKCols, TargetData);
          TempSource := FSourceDB.GetData(TableName, WhereClause);
          try
            if TempSource.IsEmpty then
            begin
              FWriter.AddComment(Format(TRes.MsgDeleteObs, [WhereClause]));
              FWriter.AddCommand(FHelpers.GenerateDeleteSQL(TableName, WhereClause));
            end;
          finally
            TempSource.Free;
          end;
          TargetData.Next;
        end;
      finally
        TargetData.Free;
      end;
    end;

  finally
    TableStruct.Free;
    PKCols.Free;
    ValueRows.Free;
  end;
end;

function TDBComparerEngine.BuildWhereClause(const PKCols: TStringList;
                                            DataSet: TDataSet): string;
var
  i: Integer;
  Field: TField;
begin
  Result := '';
  for i := 0 to PKCols.Count - 1 do
  begin
    if i > 0 then Result := Result + ' AND ';
    Field := DataSet.FieldByName(PKCols[i]);
    Result := Result + FHelpers.QuoteIdentifier(PKCols[i]) + ' = ' +
              FHelpers.ValueToSQL(Field);
  end;
end;
procedure TDBComparerEngine.CompareProcedures;
var
  SourceProcs: TStringList;
  i: Integer;
  RoutineMetadata: IMySQLRoutineMetadataProvider;
  SQLMode: string;
  HasRoutineMetadata: Boolean;
begin
  SourceProcs := FSourceDB.GetProcedures;
  try
    HasRoutineMetadata := FOptions.MariaDB10Compat and
      Supports(FSourceDB, IMySQLRoutineMetadataProvider, RoutineMetadata);
    FWriter.AddComment(TRes.MsgHeaderProcs);
    for i := 0 to SourceProcs.Count - 1 do
    begin
      FWriter.AddComment(TRes.MsgRecreateProc + SourceProcs[i]);
      FWriter.AddCommand(FHelpers.GenerateDropProcedure(SourceProcs[i]));
      if HasRoutineMetadata then
      begin
        SQLMode := RoutineMetadata.GetRoutineSQLMode(SourceProcs[i],
          'PROCEDURE');
        FWriter.AddCommand('SET SQL_MODE=' + QuotedStr(SQLMode) + ';');
      end;
      var strProc := FSourceDB.GetProcedureDefinition(SourceProcs[i]);
      FWriter.AddCommand(FHelpers.GenerateCreateProcedureSQL(strProc));
      if HasRoutineMetadata then
        FWriter.AddCommand('SET SQL_MODE=''NO_AUTO_VALUE_ON_ZERO'';');
    end;
  finally
    SourceProcs.Free;
  end;
end;
procedure TDBComparerEngine.CompareFunctions;
var
  SourceFuncs: TStringList;
  i: Integer;
  RoutineMetadata: IMySQLRoutineMetadataProvider;
  SQLMode: string;
  HasRoutineMetadata: Boolean;
begin
  SourceFuncs := FSourceDB.GetFunctions;
  try
    HasRoutineMetadata := FOptions.MariaDB10Compat and
      Supports(FSourceDB, IMySQLRoutineMetadataProvider, RoutineMetadata);
    FWriter.AddComment(TRes.MsgHeaderFunc);
    for i := 0 to SourceFuncs.Count - 1 do
    begin
      FWriter.AddComment(TRes.MsgRecreateFunc + SourceFuncs[i]);
      // Borrar y crear
      FWriter.AddCommand(FHelpers.GenerateDropFunction(SourceFuncs[i]));
      if HasRoutineMetadata then
      begin
        SQLMode := RoutineMetadata.GetRoutineSQLMode(SourceFuncs[i],
          'FUNCTION');
        FWriter.AddCommand('SET SQL_MODE=' + QuotedStr(SQLMode) + ';');
      end;
      var strFunc := FSourceDB.GetFunctionDefinition(SourceFuncs[i]);
      FWriter.AddCommand(FHelpers.GenerateCreateFunctionSQL(strFunc));
      if HasRoutineMetadata then
        FWriter.AddCommand('SET SQL_MODE=''NO_AUTO_VALUE_ON_ZERO'';');
    end;
  finally
    SourceFuncs.Free;
  end;
end;
procedure TDBComparerEngine.CompareTableIndexes(const TableName: string);
var
  SourceIndexes, TargetIndexes: TArray<TIndexInfo>;
  Found: Boolean;
begin
  SourceIndexes := FSourceDB.GetTableIndexes(TableName);
  TargetIndexes := FTargetDB.GetTableIndexes(TableName);
  // 1. Eliminar índices que ya no existen
  if not FOptions.NoDelete then
  begin
    for var i := 0 to High(TargetIndexes) do
    begin
      // No borrar PRIMARY KEY aquí (se maneja diferente)
      if TargetIndexes[i].IsPrimary then
        Continue;
      Found := False;
      for var j := 0 to High(SourceIndexes) do
      begin
        if SameText(SourceIndexes[j].IndexName, TargetIndexes[i].IndexName) then
        begin
          Found := True;
          Break;
        end;
      end;
      if not Found then
      begin
        FWriter.AddComment(TRes.MsgDelIndex + TableName + '.' +
                          TargetIndexes[i].IndexName);
        FWriter.AddCommand(FHelpers.GenerateDropIndexSQL(TableName,
                                                   TargetIndexes[i].IndexName));
      end;
    end;
  end;
  // 2. Crear o modificar índices
  for var i := 0 to High(SourceIndexes) do
  begin
    Found := False;
    for var j := 0 to High(TargetIndexes) do
    begin
      if SameText(SourceIndexes[i].IndexName, TargetIndexes[j].IndexName) then
      begin
        Found := True;
        // Si son diferentes, recrear
        if not FHelpers.IndexesAreEqual(SourceIndexes[i], TargetIndexes[j]) then
        begin
          FWriter.AddComment(TRes.MsgModIndex + TableName + '.' +
                            SourceIndexes[i].IndexName);
          FWriter.AddCommand(FHelpers.GenerateDropIndexSQL(TableName,
                                                             SourceIndexes[i].IndexName));
          FWriter.AddCommand(FHelpers.GenerateIndexDefinition(TableName,
                                                              SourceIndexes[i]));
        end;
        Break;
      end;
    end;
    // Índice nuevo
    if not Found then
    begin
      FWriter.AddComment(TRes.MsgAddIndex + TableName + '.' +
                        SourceIndexes[i].IndexName);
      FWriter.AddCommand(FHelpers.GenerateIndexDefinition(TableName,
                                                          SourceIndexes[i]));
    end;
  end;
end;
end.
