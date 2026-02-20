[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared billing = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a4389d3c-ef32-4e21-9bf6-7c54eeb16bd4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "billing", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared departments = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a4389d3c-ef32-4e21-9bf6-7c54eeb16bd4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "departments", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared doctors = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a4389d3c-ef32-4e21-9bf6-7c54eeb16bd4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "doctors", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared patient_visits = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a4389d3c-ef32-4e21-9bf6-7c54eeb16bd4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "patient_visits", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared patients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a4389d3c-ef32-4e21-9bf6-7c54eeb16bd4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "patients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_doctors_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared silver_doctors = let
  Source = doctors,
  #"Trimmed text" = Table.TransformColumns(Source, {{"DoctorName", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text", {{"Specialization", each Text.Proper(_), type nullable text}})
in
  #"Capitalized each word";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_departments_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared silver_departments = let
  Source = departments,
  #"Capitalized each word" = Table.TransformColumns(Source, {{"DepartmentName", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"DepartmentName", each Text.Proper(_), type nullable text}})
in
  #"Capitalized each word 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_billing_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared silver_billing = let
  Source = billing,
  #"Capitalized each word" = Table.TransformColumns(Source, {{"PaymentMode", each Text.Proper(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Capitalized each word", {"VisitID"}, patient_visits, {"VisitID"}, "patient_visits", JoinKind.LeftOuter),
  #"Expanded patient_visits" = Table.ExpandTableColumn(#"Merged queries", "patient_visits", {"TotalCost"}, {"TotalCost"}),
  #"Added custom" = Table.AddColumn(#"Expanded patient_visits", "OutstandingAmount", each [TotalCost] - [PaidAmount]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"OutstandingAmount", type number}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Changed column type", {{"PaymentMode", each Text.Proper(_), type nullable text}})
in
  #"Capitalized each word 1";
shared silver_billing_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "44cc4b61-5fbe-4eeb-9f5a-bb37bc04bb27"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_billing", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_patients_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared silver_patients = let
  Source = patients,
  #"Replaced value" = Table.ReplaceValue(Source, null, "Unknown", Replacer.ReplaceValue, {"InsuranceType"}),
  #"Uppercased text" = Table.TransformColumns(#"Replaced value", {{"InsuranceType", each Text.Upper(_), type nullable text}}),
  #"Inserted conditional column" = Table.AddColumn(#"Uppercased text", "Insurance_Standardized", each if [InsuranceType] = null then "Unknown" else if [InsuranceType] = "PRIVATE" then "Private" else if [InsuranceType] = "GOVERNMENT" then "Government" else if [InsuranceType] = "SELFPAY" then "SelfPay" else "Unknown"),
  #"Added custom" = Table.AddColumn(#"Inserted conditional column", "Age", each Date.Year(DateTime.LocalNow()) - Date.Year([DateOfBirth])),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"Insurance_Standardized", type text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Changed column type 1", {{"Age", Int64.Type}, {"Insurance_Standardized", type text}}, "en-IN")
in
  #"Changed column type";
shared silver_patients_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "44cc4b61-5fbe-4eeb-9f5a-bb37bc04bb27"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_patients", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared silver_departments_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "44cc4b61-5fbe-4eeb-9f5a-bb37bc04bb27"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_departments", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared silver_doctors_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "44cc4b61-5fbe-4eeb-9f5a-bb37bc04bb27"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_doctors", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_patient_visits_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared silver_patient_visits = let
  Source = patient_visits,
  #"Capitalized each word" = Table.TransformColumns(Source, {{"AdmissionType", each Text.Proper(_), type nullable text}}),
  #"Renamed columns" = Table.RenameColumns(#"Capitalized each word", {{"AdmissionType", "AdmissionType_Standardized"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns", "DateValidationStatus", each if [DischargeDate] < [VisitDate] then "Invalid" else "Valid"),
  #"Added custom" = Table.AddColumn(#"Inserted conditional column", "LengthOfStay", each Duration.Days([DischargeDate] - [VisitDate])),
  #"Inserted conditional column 3" = Table.AddColumn(#"Added custom", "StayCategory", each if [LengthOfStay] <= 3 then "Short Stay" else if [LengthOfStay] <= 7 then "Moderate Stay" else "Long Stay"),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 3", "RiskCategory", each if [LengthOfStay] > 10 then "High" else if [LengthOfStay] >= 5 then "Medium" else "Low"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column 2", "NegativeStayFlag", each if [LengthOfStay] < 0 then 1 else 0),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted conditional column 1", {{"DateValidationStatus", type text}, {"LengthOfStay", Int64.Type}, {"StayCategory", type text}, {"RiskCategory", type text}, {"NegativeStayFlag", Int64.Type}})
in
  #"Changed column type";
shared silver_patient_visits_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "44cc4b61-5fbe-4eeb-9f5a-bb37bc04bb27"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_patient_visits", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
