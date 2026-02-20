[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared raw_accounts = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d86d63ba-5b84-43ca-8d41-e401ced70808"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_accounts", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"opened_date", type date}}),
  #"Filled up" = Table.FillUp(#"Changed column type", {"account_type"}),
  #"Filled up 1" = Table.FillUp(#"Filled up", {"account_status"}),
  #"Filled down" = Table.FillDown(#"Filled up 1", {"current_balance"}),
  #"Filled down 1" = Table.FillDown(#"Filled down", {"interest_rate"}),
  #"Filled up 2" = Table.FillUp(#"Filled down 1", {"opened_date"})
in
  #"Filled up 2";
shared raw_branches = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d86d63ba-5b84-43ca-8d41-e401ced70808"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_branches", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"branch_id", type text}, {"branch_name", type text}, {"opened_date", type date}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type", {"branch_id"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed duplicates", {{"opened_date", type date}})
in
  #"Changed column type 1";
shared raw_credit_bureau = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d86d63ba-5b84-43ca-8d41-e401ced70808"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_credit_bureau", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([credit_score] = null)),
  #"Filled down" = Table.FillDown(#"Filtered rows", {"credit_utilization"}),
  #"Filled down 1" = Table.FillDown(#"Filled down", {"num_open_credit_lines"}),
  #"Filled down 2" = Table.FillDown(#"Filled down 1", {"num_delinquencies_2y"}),
  #"Filled up" = Table.FillUp(#"Filled down 2", {"num_inquiries_6m"}),
  #"Filled down 3" = Table.FillDown(#"Filled up", {"credit_history_years"}),
  #"Replaced value" = Table.ReplaceValue(#"Filled down 3", null, 5.5, Replacer.ReplaceValue, {"credit_score"})
in
  #"Replaced value";
shared raw_customers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d86d63ba-5b84-43ca-8d41-e401ced70808"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_customers", ItemKind = "Table"]}[Data],
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Navigation 2", {"full_name"}),
  #"Filled down" = Table.FillDown(#"Removed errors", {"full_name"}),
  #"Removed duplicates" = Table.Distinct(#"Filled down", {"customer_id"}),
  #"Filled up" = Table.FillUp(#"Removed duplicates", {"gender"}),
  #"Replaced value" = Table.ReplaceValue(#"Filled up", "O", "M", Replacer.ReplaceValue, {"gender"}),
  #"Filled up 1" = Table.FillUp(#"Replaced value", {"dob"}),
  #"Filled down 1" = Table.FillDown(#"Filled up 1", {"age"}),
  #"Filled up 2" = Table.FillUp(#"Filled down 1", {"phone"}),
  #"Filled down 2" = Table.FillDown(#"Filled up 2", {"email"}),
  #"Removed duplicates 1" = Table.Distinct(#"Filled down 2", {"email"}),
  #"Removed duplicates 2" = Table.Distinct(#"Removed duplicates 1", {"pan"}),
  #"Filled down 3" = Table.FillDown(#"Removed duplicates 2", {"pan"}),
  #"Removed duplicates 3" = Table.Distinct(#"Filled down 3", {"pan"}),
  #"Filled down 4" = Table.FillDown(#"Removed duplicates 3", {"city"}),
  #"Filled down 5" = Table.FillDown(#"Filled down 4", {"state"}),
  #"Filled down 6" = Table.FillDown(#"Filled down 5", {"occupation"}),
  #"Filled up 3" = Table.FillUp(#"Filled down 6", {"education"}),
  #"Filled up 4" = Table.FillUp(#"Filled up 3", {"marital_status"}),
  #"Filled down 7" = Table.FillDown(#"Filled up 4", {"customer_segment"}),
  #"Filled up 5" = Table.FillUp(#"Filled down 7", {"annual_income"}),
  #"Filled up 6" = Table.FillUp(#"Filled up 5", {"assigned_branch_id"}),
  #"Filled down 8" = Table.FillDown(#"Filled up 6", {"customer_since"})
in
  #"Filled down 8";
shared raw_loan_payments = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d86d63ba-5b84-43ca-8d41-e401ced70808"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_loan_payments", ItemKind = "Table"]}[Data],
  #"Filled down" = Table.FillDown(#"Navigation 2", {"due_date"}),
  #"Filled up" = Table.FillUp(#"Filled down", {"due_date"}),
  #"Removed duplicates" = Table.Distinct(#"Filled up", {"due_date"}),
  #"Filled down 1" = Table.FillDown(#"Removed duplicates", {"paid_date"}),
  #"Filled down 2" = Table.FillDown(#"Filled down 1", {"amount_due"}),
  #"Filled down 3" = Table.FillDown(#"Filled down 2", {"amount_paid"}),
  #"Removed columns" = Table.RemoveColumns(#"Filled down 3", {"late_days"}),
  #"Filled down 4" = Table.FillDown(#"Removed columns", {"outstanding_principal"})
in
  #"Filled down 4";
shared raw_loans = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d86d63ba-5b84-43ca-8d41-e401ced70808"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_loans", ItemKind = "Table"]}[Data],
  #"Removed duplicates" = Table.Distinct(#"Navigation 2", {"loan_id"}),
  #"Removed duplicates 1" = Table.Distinct(#"Removed duplicates", {"customer_id"}),
  #"Filled up" = Table.FillUp(#"Removed duplicates 1", {"loan_type"}),
  #"Filled down" = Table.FillDown(#"Filled up", {"purpose"}),
  #"Filled up 1" = Table.FillUp(#"Filled down", {"collateral_type"}),
  #"Filled down 1" = Table.FillDown(#"Filled up 1", {"employment_type"}),
  #"Filled down 2" = Table.FillDown(#"Filled down 1", {"loan_amount"}),
  #"Filled down 3" = Table.FillDown(#"Filled down 2", {"interest_rate"}),
  #"Filled down 4" = Table.FillDown(#"Filled down 3", {"tenure_months"}),
  #"Filled up 2" = Table.FillUp(#"Filled down 4", {"tenure_months"}),
  #"Filled up 3" = Table.FillUp(#"Filled up 2", {"emi"}),
  #"Filled down 5" = Table.FillDown(#"Filled up 3", {"dti"}),
  #"Filled down 6" = Table.FillDown(#"Filled down 5", {"start_date"}),
  #"Filled down 7" = Table.FillDown(#"Filled down 6", {"end_date"}),
  #"Filled down 8" = Table.FillDown(#"Filled down 7", {"loan_status"}),
  #"Filled down 9" = Table.FillDown(#"Filled down 8", {"default_prob"})
in
  #"Filled down 9";
shared raw_transactions = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d86d63ba-5b84-43ca-8d41-e401ced70808"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_transactions", ItemKind = "Table"]}[Data],
  #"Removed duplicates" = Table.Distinct(#"Navigation 2", {"txn_id"}),
  #"Removed duplicates 1" = Table.Distinct(#"Removed duplicates", {"account_id"}),
  #"Filled down" = Table.FillDown(#"Removed duplicates 1", {"txn_datetime"}),
  #"Filled up" = Table.FillUp(#"Filled down", {"txn_type"}),
  #"Removed columns" = Table.RemoveColumns(#"Filled up", {"channel"}),
  #"Filled down 1" = Table.FillDown(#"Removed columns", {"merchant_category"}),
  #"Filled down 2" = Table.FillDown(#"Filled down 1", {"amount"}),
  #"Filled down 3" = Table.FillDown(#"Filled down 2", {"currency"}),
  #"Filled up 1" = Table.FillUp(#"Filled down 3", {"is_fraud_suspected"})
in
  #"Filled up 1";
