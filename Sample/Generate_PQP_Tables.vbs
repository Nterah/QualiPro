' Generate_PQP_Tables.vbs
Option Explicit

Const SRC_PATH = "C:\Users\Ntembeko Zifuku\source\repos\pqp_module\Sample\Project Quality Plan Sample.xlsx"
Const OUT_PATH = "C:\Users\Ntembeko Zifuku\source\repos\pqp_module\Sample\Project Quality Plan Sample - PQP Tables.xlsx"
Const MAX_ROWS = 120
Const MAX_COLS = 26  ' A..Z
Const SRC_RANGE = "A1:Z400"

Dim xl, wb, ws, i, j

Sub EnsureSheetRemoved(wb, sheetName)
  On Error Resume Next
  wb.Worksheets(sheetName).Delete
  On Error GoTo 0
End Sub

Function EscDbl(s)
  EscDbl = Replace(s, """", """""")
End Function

Function BuildFormula(secPat, keyHdr, headerText, rowOffset)
  Dim headerWild
  headerWild = "*" & headerText & "*"

  BuildFormula = "=IFERROR(LET(" & _
    "fs, FirstSheetName," & _
    "data, INDIRECT(""'""&fs&""'!" & SRC_RANGE & """)," & _
    "rowsConcat, BYROW(data, LAMBDA(r, TEXTJOIN(""|"",TRUE,r)))," & _
    "secRow, XMATCH(""" & EscDbl(secPat) & """, rowsConcat, 2)," & _
    "block, TAKE(DROP(data, secRow), 15)," & _
    "hdrRel, XMATCH(TRUE, BYROW(block, LAMBDA(r, SUM(--ISNUMBER(SEARCH(""" & EscDbl(keyHdr) & """, r)))>0 )), 0)," & _
    "hdrRow, secRow + hdrRel," & _
    "hdrRange, INDEX(data, hdrRow, 1):INDEX(data, hdrRow, " & MAX_COLS & ")," & _
    "col, XMATCH(""" & EscDbl(headerWild) & """, hdrRange, 2)," & _
    "val, INDEX(data, hdrRow + " & CStr(rowOffset) & ", col)," & _
    "IF(val="""","""",val)" & _
  "),"""")"
End Function

Sub PutHeaders(ws, r, headers)
  Dim c
  For c = 0 To UBound(headers)
    ws.Cells(r, c+1).Value = headers(c)
  Next
End Sub

Sub FillSection(ws, titleRow, headerRow, headers, secPat, keyHdr, tableName)
  Dim r, c, f, lastRow, lastCol
  ws.Cells(titleRow, 1).Value = tableName

  ' headers
  Call PutHeaders(ws, headerRow, headers)

  ' id column (A)
  For r = 1 To MAX_ROWS
    ws.Cells(headerRow + r, 1).Formula = "=IF($B$2="""","""",$B$2)"
  Next

  ' other columns with dynamic formulas
  For c = 2 To UBound(headers)+1
    For r = 1 To MAX_ROWS
      f = BuildFormula(secPat, keyHdr, headers(c-1), r)
      ws.Cells(headerRow + r, c).Formula = f
    Next
  Next

  ' make table
  lastRow = headerRow + MAX_ROWS
  lastCol = UBound(headers) + 1
  ws.ListObjects.Add 1, ws.Range(ws.Cells(headerRow,1), ws.Cells(lastRow,lastCol)), , 1
  ws.ListObjects(ws.ListObjects.Count).Name = Replace(tableName, " ", "_")
End Sub

' --- MAIN ---
If CreateObject("Scripting.FileSystemObject").FileExists(SRC_PATH) = False Then
  WScript.Echo "Source not found:" & vbCrLf & SRC_PATH
  WScript.Quit 1
End If

Set xl = CreateObject("Excel.Application")
xl.Visible = False
xl.DisplayAlerts = False

Set wb = xl.Workbooks.Open(SRC_PATH)

' Add named formula to get first sheet name
On Error Resume Next
wb.Names("FirstSheetName").Delete
On Error GoTo 0
wb.Names.Add "FirstSheetName", "=REPLACE(INDEX(GET.WORKBOOK(1),1),1,FIND(""]"",INDEX(GET.WORKBOOK(1),1)),"""")"

' Remove and re-create PQP Tables sheet
Call EnsureSheetRemoved(wb, "PQP Tables")
Set ws = wb.Worksheets.Add
ws.Name = "PQP Tables"

' Project ID helper (to the right of "HN Project Number")
ws.Range("A2").Value = "Project ID"
ws.Range("B2").Formula = "=LET(" & _
 "fs, FirstSheetName," & _
 "data, INDIRECT(""'""&fs&""'!A1:Z60"")," & _
 "rowsConcat, BYROW(data, LAMBDA(r, TEXTJOIN(""|"",TRUE,r)))," & _
 "r, XMATCH(""*HN Project Number*"", rowsConcat, 2)," & _
 "hdrRange, INDEX(data, r, 1):INDEX(data, r, 26)," & _
 "c, XMATCH(""*HN Project Number*"", hdrRange, 2)," & _
 "INDEX(data, r, c+1)" & _
 ")"

' ---- Section configs ----
Dim s1, s2, s3, s4, s5, s6, s7, s8
s1 = Array("id","Project Description","Location","Client Organisation","Primary Contact Name","VAT Number","Designation","Invoice Address")
s2 = Array("id","Role","Req'd","Organisation","Representative Name","Email","Cell","Subconsultant to HN?","Subconsultant Agreement?","CPG Partner?","CPG %","Comments")
s3 = Array("id","In Place","Date","Filing Location","Notes","HN Roles","ECSA Project Stage","HN Appointment? (Signed?)","Appointment Date","Expected Duration","Contract/Ref No","Comments")
s4 = Array("id","Design Criteria/Requirements","Planning & Design Risks","Project-specific Risks","Mitigating Measures","Record of Action Taken","Design Deliverable Accepted?","Approved?","Scope Register Location","Design Notes")
s5 = Array("id","Client Tender Doc Requirements","Form of Contract","Standard Specifications","Project-specific Risks","Mitigating Measures","Record of Action Taken","Tender Phase Notes")
s6 = Array("id","Construction Description","Contractor Organisation","Contract No","Commencement Date","Completion Date","Construction Risks","Mitigating Measures","Record of Action Taken","Construction Phase Notes")
s7 = Array("id","Additional Services Done","Project-specific Risks","Mitigating Measures","Record of Action Taken","Notes")
s8 = Array("id","Date CSQ Submitted","Date CSQ Received","CSQ Rating","Location","Comments on Feedback","Actual Close-Out Date","General Remarks/Lessons Learned")

' Layout rows for headers
Dim startRows
startRows = Array(0, 6, 130, 254, 378, 502, 626, 750, 874) ' index 1..8 used

' Fill sections (title row is headerRow-1)
Call FillSection(ws, startRows(1)-1, startRows(1), s1, "1.*PROJECT*OVERVIEW*", "Project Description", "PQP Sec1")
Call FillSection(ws, startRows(2)-1, startRows(2), s2, "2.*PROJECT*TEAM*", "Role", "PQP Sec2")
Call FillSection(ws, startRows(3)-1, startRows(3), s3, "3.*APPOINTMENT*", "In Place", "PQP Sec3")
Call FillSection(ws, startRows(4)-1, startRows(4), s4, "4.*(PLANNING|DESIGN)*", "Design", "PQP Sec4")
Call FillSection(ws, startRows(5)-1, startRows(5), s5, "5.*(DOCUMENTATION|PROCUREMENT|TENDER)*", "Client", "PQP Sec5")
Call FillSection(ws, startRows(6)-1, startRows(6), s6, "6.*(WORKS|HANDOVER|CONSTRUCTION)*", "Construction", "PQP Sec6")
Call FillSection(ws, startRows(7)-1, startRows(7), s7, "7.*(ADDITIONAL|OTHER)*", "Additional", "PQP Sec7")
Call FillSection(ws, startRows(8)-1, startRows(8), s8, "8.*(CLOSE|FEEDBACK)*", "Date CSQ Submitted", "PQP Sec8")

' Save as new workbook alongside the source
wb.SaveCopyAs OUT_PATH
wb.Close False
xl.Quit

WScript.Echo "Created: " & OUT_PATH
