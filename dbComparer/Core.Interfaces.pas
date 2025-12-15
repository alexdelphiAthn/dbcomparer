unit Core.Interfaces;

interface

uses System.Classes, Core.Types;

type
  // Contrato para leer la estructura de la BD
  IDBMetadataProvider = interface
    ['{GUID-GENERATE-ONE-HERE}'] // Presiona Ctrl+Shift+G en Delphi
    function GetTables: TStringList;
    function GetTableStructure(const TableName: string): TTableInfo;
    function GetTableIndexes(const TableName: string): TArray<TIndexInfo>;
    function GetTriggers: TArray<TTriggerInfo>;
    function GetTriggerDefinition(const TriggerName: string): string;
    function GetViews:TStringList;
    function GetViewDefinition(const ViewName:string):string;
    function GetProcedures:TStringList;
    function GetProcedureDefinition(const ProcedureName):string;
  end;

  // Contrato para escribir el script
  IScriptWriter = interface
    ['{GUID-GENERATE-ANOTHER-HERE}']
    procedure AddComment(const Text: string);
    procedure AddCommand(const SQL: string);
    function GetScript: string;
  end;

implementation
end.