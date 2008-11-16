object DemoMain: TDemoMain
  Left = 0
  Top = 0
  Caption = 'DemoMain'
  ClientHeight = 255
  ClientWidth = 419
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  OnCreate = FormCreate
  DesignSize = (
    419
    255)
  PixelsPerInch = 96
  TextHeight = 13
  object DBGrid1: TDBGrid
    Left = 4
    Top = 6
    Width = 413
    Height = 205
    Anchors = [akLeft, akTop, akRight, akBottom]
    DataSource = DataSource1
    TabOrder = 0
    TitleFont.Charset = DEFAULT_CHARSET
    TitleFont.Color = clWindowText
    TitleFont.Height = -11
    TitleFont.Name = 'Tahoma'
    TitleFont.Style = []
  end
  object bRefresh: TButton
    Left = 260
    Top = 222
    Width = 75
    Height = 25
    Anchors = [akRight, akBottom]
    Caption = 'Refresh'
    TabOrder = 1
    OnClick = bRefreshClick
    ExplicitLeft = 276
    ExplicitTop = 232
  end
  object bTestAdd: TButton
    Left = 340
    Top = 222
    Width = 75
    Height = 25
    Anchors = [akRight, akBottom]
    Caption = 'Test Add'
    TabOrder = 2
    OnClick = bTestAddClick
    ExplicitLeft = 356
    ExplicitTop = 232
  end
  object ADOConnection1: TADOConnection
    Connected = True
    ConnectionString = 
      'Provider=SQLOLEDB.1;Password=scs;Persist Security Info=True;User' +
      ' ID=sa;Data Source=localhost'
    DefaultDatabase = 'demo1'
    LoginPrompt = False
    Provider = 'SQLOLEDB.1'
    Left = 40
    Top = 36
  end
  object DataSource1: TDataSource
    DataSet = ADOQuery1
    Left = 280
    Top = 24
  end
  object ADOQuery1: TADOQuery
    Parameters = <>
    Left = 212
    Top = 22
  end
end
