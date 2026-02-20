[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared billing_raw = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3e7fe138-74f4-40e0-bfbd-64f0027c39e6"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "billing_raw", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared doctors_raw = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3e7fe138-74f4-40e0-bfbd-64f0027c39e6"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "doctors_raw", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared patients_raw = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3e7fe138-74f4-40e0-bfbd-64f0027c39e6"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "patients_raw", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared visits_raw = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0ef96a5d-6189-4a8b-a938-4fc18e107cf7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3e7fe138-74f4-40e0-bfbd-64f0027c39e6"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "visits_raw", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
