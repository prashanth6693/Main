[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "retail_events_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared retail_events = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "fd6dfac9-3786-4827-9b43-812872ba642f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "retail_events", ItemKind = "Table"]}[Data],
  #"Split column by delimiter" = Table.SplitColumn(#"Navigation 2", "raw_text", Splitter.SplitTextByDelimiter(";"), {"raw_text.1", "raw_text.2", "raw_text.3", "raw_text.4"}),
  #"Renamed columns" = Table.RenameColumns(#"Split column by delimiter", {{"raw_text.1", "Store"}, {"raw_text.2", "Product"}, {"raw_text.3", "Qty"}, {"raw_text.4", "Inventory"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns", "IsFaultTxn", each if [txn_status] = "FAULT" then true else false),
  #"Replaced value" = Table.ReplaceValue(#"Inserted conditional column", "Inv=", "", Replacer.ReplaceText, {"Inventory"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value", {{"Inventory", Int64.Type}}),
  #"Inserted conditional column 1" = Table.AddColumn(#"Changed column type", "IsStockOut", each if [Inventory] <= 0 then true else false),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "IsOverStock", each if [Inventory] > 100 then true else false),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted conditional column 2", {{"event_time", type datetime}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Changed column type 1", "Qty=", "", Replacer.ReplaceText, {"Qty"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 1", {{"Qty", Int64.Type}}),
  #"Replaced value 2" = Table.ReplaceValue(#"Changed column type 2", "Store=", "", Replacer.ReplaceText, {"Store"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "Prod=", "", Replacer.ReplaceText, {"Product"})
in
  #"Replaced value 3";
shared retail_events_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ed55df3f-b99a-4de5-bc0a-8bd843a8d91a"]}[Data],
  TableNavigation = Navigation_2{[Id = "retail_clean", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
