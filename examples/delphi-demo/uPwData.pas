unit uPwData;

interface

uses DB, ADODB;

type
    TPwDatabase = class(TADOConnection)
    public
        class function ExecStr(func: string;
                               names: array of string;
                               values: array of const): string;
    end;

    TPwDataCmd = class(TObject)
    private
        db: TPwDatabase;
    protected
        function MakeRawSql: string; virtual; abstract;
    public
        property RawSql: string  read MakeRawSql;

        constructor Create(db: TPwDatabase); overload;
        procedure Select(ds: TADOQuery); overload;
        procedure Exec;
    end;

    TPwDateWrap = class(TObject)
    private
        val: TDateTime;
    public
        constructor Create(val: TDateTime);
        function StringifyAndFree: string;
    end;

const
    vfalse: integer = 0;
    vtrue: integer = 1;
    nilstr: string = '__!NIL!__';
    nilint: integer = low(integer)+42;
    nilbool: integer = low(integer)+42;
    nildate: TDateTime = low(integer)+42;
    nildouble: double = 1e-42;
    nilcurrency: currency = -90000000000.0042;

implementation

uses SysUtils;

function isnilstr(s: string): boolean;
var
    nilptr, sptr: PChar;
begin
    nilptr := PChar(nilstr);
    sptr := PChar(s);

    // the 's = nilstr' is a cop-out, only needed because you can't pass nilstr
    // itself as the default value of a function parameter, sigh.
    result := (@nilptr[0] = @sptr[0]) or (s = nilstr);
end;

function isnilint(i: integer): boolean;
begin
    result := (i = nilint);
end;

function isnildate(d: TDateTime): boolean;
begin
    result := (d = nildate);
end;

function isnildouble(d: double): boolean;
begin
    result := (d = nildouble);
end;

function isnilcur(c: currency): boolean; overload;
begin
    result := (c = nilcurrency);
end;


function stringrender(s: string): string;
begin
    if isnilstr(s) then
        result := 'NULL'
    else
        result := QuotedStr(s);
end;


function intrender(i: integer): string;
begin
    if isnilint(i) then
        result := 'NULL'
    else
        result := IntToStr(i);
end;


function floatrender(f: extended): string;
begin
    if isnildouble(f) then
        result := 'NULL'
    else
        result := FloatToStr(f);
end;


function currrender(c: currency): string;
begin
    if isnilcur(c) then
        result := 'NULL'
    else
        result := CurrToStr(c);
end;


function boolrender(b: boolean): string;
const
    boolstr: array[Boolean] of string = ('0', '1');
begin
    result := boolstr[b];
end;


function SqlEscape(c: TVarRec; errsuffix: string = ''): string;
begin
    case c.VType of
        vtInteger:    result := intrender(c.VInteger);
        //vtInt64:      result := intrender(c.VInt64^);
        vtExtended:   result := floatrender(c.VExtended^);
        vtCurrency:   result := currrender(c.VCurrency^);
        vtBoolean:    result := boolrender(c.VBoolean);
        vtChar:       result := stringrender(c.VChar);
        vtString:     result := stringrender(c.VString^);
        vtPChar:      result := stringrender(c.VPChar);
        vtAnsiString: result := stringrender(string(c.VAnsiString));
        vtPointer: begin
            if not assigned(c.VPointer) then
                result := 'NULL'
            else
                raise EVariantError.Create('Unsupported pointer' + errsuffix);
        end;
        vtObject: begin
            if c.VObject is TPwDateWrap then
                result := TPwDateWrap(c.VObject).StringifyAndFree
            {else if c.VObject is TGraphicField then
                result := TGraphicField(c.VObject).AsWhat? }
            else if not assigned(c.VObject) then
                result := 'NULL'
            else
                raise EVariantError.Create('Unsupported object' + errsuffix +
                                ' of type '+ c.VObject.ClassName);
        end;
    else
        raise EVariantError.Create('Unsupported data type');
    end;
end;


function SqlEscapeV(c: array of const): string;
begin
    result := SqlEscape(c[0]);
end;


function _ExecStr(func: string;
                             names: array of string;
                             values: array of string): string;
var
    i: Integer;
    sql: string;
    valstr: string;
begin
    assert(pos('[', func) = 0);
    assert(pos(']', func) = 0);

    sql := '[' + func + '] ';

    if high(names) <> high(values) then
        raise ERangeError.Create('number of names must match values!');

    for i := 0 to high(names) do begin
        valstr := values[i];

        if i > 0 then
            sql := sql + ', ';
        sql := sql + '@' + names[i] + '=' + valstr;
    end;

    result := sql;
end;


function ExecStr(func: string;
                 names: array of string;
                 values: array of const): string;
var
    i: Integer;
    outv: array of string;
begin
    if high(names) <> high(values) then
        raise ERangeError.Create('number of names must match values!');

    SetLength(outv, Length(values));

    for i := 0 to high(names) do begin
        outv[i] := SqlEscape(values[i], Format(' at %d (%s)', [i, names[i]]));
    end;

    result := _ExecStr(func, names, outv);
end;


{ TPwDatabase }

class function TPwDatabase.ExecStr(func: string; names: array of string;
          values: array of const): string;
begin
    result := uPwData.ExecStr(func, names, values);
end;


{ TPwDataCmd }

constructor TPwDataCmd.Create(db: TPwDatabase);
begin
    inherited Create;
    self.db := db;
end;

procedure TPwDataCmd.Select(ds: TADOQuery);
begin
    try
        ds.Connection := db;
        ds.SQL.Clear;
        ds.SQL.Add(RawSql);
        ds.Open;
    finally
        Free;
    end;
end;

procedure TPwDataCmd.Exec;
var
    c: TADOCommand;
begin
    c := TADOCommand.Create(nil);
    try
        c.Connection := db;
        c.CommandText := RawSql;
        c.Execute;
    finally
        FreeAndNil(c);
        Free;
    end;
end;

{ TPwDateWrap }

constructor TPwDateWrap.Create(val: TDateTime);
begin
    self.val := val;
end;

function TPwDateWrap.StringifyAndFree: string;
begin
    if isnildate(val) then
        result := 'NULL'
    else
        result := QuotedStr(FormatDateTime('yyyy/mm/dd hh:mm:ss', val));
    Free;
end;


end.
