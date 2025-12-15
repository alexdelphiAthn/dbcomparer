unit ScriptWriters;

type
  TStringListScriptWriter = class(TInterfacedObject, IScriptWriter)
  private
    FScript: TStringList;
  public
    constructor Create;
    destructor Destroy; override;
    procedure AddComment(const Text: string);
    procedure AddCommand(const SQL: string);
    function GetScript: string;
  end;
  
  