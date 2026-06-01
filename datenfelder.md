# Datenfelder 2026

Diese Datei beschreibt die Felder des aktuellen Exports `out/wohnlagen_brb_2026.csv` aus `wohnlagen_2026.ipynb`. Zwischenprodukte in `out/` koennen weitere technische Felder enthalten, die hier nicht vollstaendig dokumentiert werden.

| Feldname | Datentyp / Format | Bedeutung | Herleitung / Berechnung | Anmerkungen |
| --- | --- | --- | --- | --- |
| `fid` | int / str | Quell-ID der Wohnadresse | Aus `out/adressen_mit_zentrum_routen.csv` bzw. der geocodierten Adressbasis uebernommen | Identifikator aus den Eingangsdaten |
| `str-schluessel` | str | Strassenschluessel | Aus den Eingangsdaten uebernommen | Schreibweise stammt aus der Rohdatenquelle |
| `stand_der_daten` | str / Datum | Datenstand der Wohnadresse | Aus den Eingangsdaten uebernommen | Dokumentiert den Stand der Adressdaten |
| `Straßenname` | str | Strassenname der Wohnadresse | Aus den Eingangsdaten uebernommen | Teil der Adresszuordnung |
| `Hsnr` | str / int | Hausnummer | Aus den Eingangsdaten uebernommen | Wird fuer `Adresse_merge` genutzt |
| `HsnrZus` | str | Hausnummerzusatz | Aus den Eingangsdaten uebernommen | Leer, wenn kein Zusatz vorhanden ist |
| `lat` | float | Breitengrad in WGS84 | Geocoding bzw. Koordinatenumrechnung | Grundlage fuer Karten und Routing |
| `lon` | float | Laengengrad in WGS84 | Geocoding bzw. Koordinatenumrechnung | Grundlage fuer Karten und Routing |
| `center_distance` | float (m) | Fusswegdistanz zur Jahrtausendbruecke | `routing_zentrum.py`, im Notebook aus `distance_m` umbenannt | Niedriger ist besser |
| `center_route` | GeoJSON LineString als JSON-String | Fusswegroute zur Jahrtausendbruecke | `routing_zentrum.py`, im Notebook aus `geojson` umbenannt | Wird fuer Karten und Bahnbarriere genutzt |
| `Adresse_merge` | str | Normalisierte Adresse fuer Joins | `helper.make_merge_addr` aus Strasse, Hausnummer und Zusatz | Technischer Schluessel fuer Merges |
| `block` | str / int | Block-ID der Wohnadresse | Raeumlicher Join mit `data/2026-02-12-Blockgrenzen.gpkg`, Spalte `BLOCK` | Grundlage fuer Block-SKATER |
| `building_type` | str | Gebaeudetypologie | Merge aus `data/Gebaeudetypologie.csv`, normalisiert aus `Typologie`, `typologie` oder `GEBTYPGROESSE` | Fehlende Werte werden als `unbekannt` gesetzt |
| `building_age_class` | str | Baualtersklasse | Merge aus `data/Gebaeudetypologie.csv`, bevorzugt `Baualtersklassen_Zensus` oder `Baualtersklassen_Stadt` | Fehlende Werte werden als `unbekannt` gesetzt |
| `is_freistehend_building` | bool | Kennzeichen fuer freistehendes EFH/ZFH/ZWH oder vergleichbare Typologie | Abgeleitet aus `building_type` und `FREISTEHENDE_BUILDING_TYPES` | Dient als Karten-/Plausibilisierungsmerkmal |
| `einzelhandel_route` | GeoJSON LineString als JSON-String | Fusswegroute zum naechsten Einzelhandelsstandort | `routing.py --domain einzelhandel` | Technisches Kartenfeld |
| `einzelhandel_min_distance` | float (m) | Fusswegdistanz zum naechsten Einzelhandelsstandort | `routing.py --domain einzelhandel` | Niedriger ist besser |
| `einzelhandel_target_name` | str | Name des naechsten Einzelhandelsziels | `routing.py --domain einzelhandel` | Wird in Karten-Popups genutzt |
| `einzelhandel_count_within_500m` | int | Anzahl Einzelhandelsstandorte bis 500 m Fussweg | `routing.py --domain einzelhandel` | Hoeher ist besser |
| `laerm_index_tag` | float | Tages-Laermindex am Standort | Aus `out/adressen_mit_laerm.csv`, Spalte `Laerm_index_tag` | Hoeher bedeutet staerkere Laermbelastung |
| `haltestellen_route` | GeoJSON LineString als JSON-String | Fusswegroute zur naechsten OePNV-Haltestelle | `routing.py --domain haltestellen` | Technisches Kartenfeld |
| `haltestellen_min_distance` | float (m) | Fusswegdistanz zur naechsten OePNV-Haltestelle | `routing.py --domain haltestellen` | Niedriger ist besser |
| `haltestellen_target_name` | str | Name der naechsten Haltestelle | `routing.py --domain haltestellen` | Wird in Karten-Popups genutzt |
| `haltestellen_count_within_500m` | int | Anzahl Haltestellen bis 500 m Fussweg | `routing.py --domain haltestellen` | Hoeher ist besser |
| `haltestellen_linien_count` | int | Anzahl Linien an der naechsten Haltestelle | Aus `Anzahl der Linien` bzw. `linien_count` in den Haltestellen-Routingdaten umbenannt | Hoeher ist besser |
| `behind_rail_from_center` | int (0/1) | Kennzeichnet Adressen hinter einer Bahnbarriere vom Zentrum aus | Schnittpruefung der `center_route` mit Bahnlinien | 1 fuehrt zu einem Bahnbarriere-Malus |
| `z_centrality` | float | Standardisierte Zentralitaet | `-zscore(center_distance)` | Hoeher ist besser |
| `z_einzelhandel_distance` | float | Standardisierte Einzelhandelsdistanz | `-zscore(einzelhandel_min_distance)` | Hoeher ist besser |
| `z_einzelhandel_near_500` | float | Standardisierte Einzelhandelsanzahl im 500-m-Radius | `zscore(einzelhandel_count_within_500m)` | Hoeher ist besser |
| `z_laerm_index_tag` | float | Standardisierter Laermindex | `-zscore(laerm_index_tag)`, fehlende Werte werden mit 0 gefuellt | Hoeher ist besser, weil weniger Laerm |
| `z_haltestelle_distance` | float | Standardisierte Haltestellendistanz | `-zscore(haltestellen_min_distance)` | Hoeher ist besser |
| `z_haltestellen_count_within_500m` | float | Standardisierte Haltestellenanzahl im 500-m-Radius | `zscore(haltestellen_count_within_500m)` | Hoeher ist besser |
| `z_haltestellen_linien_count` | float | Standardisierte Linienanzahl an der naechsten Haltestelle | `zscore(haltestellen_linien_count)` | Hoeher ist besser |
| `z_noise_penalty` | float | Laerm-Malus fuer ueberdurchschnittliche Laermbelastung | `-0.40 * zscore(laerm_index_tag).clip(lower=0)` | Geht direkt in `score_total` ein |
| `z_rail_penalty` | float | Malus fuer Bahnbarriere | `-0.35 * behind_rail_from_center` | Geht direkt in `score_total` ein |
| `score_zentralitaet` | float | Score-Komponente Zentralitaet | Gewichtete Summe aus `z_centrality` | Unskalierter Z-Score-artiger Wert |
| `score_versorgung` | float | Score-Komponente Versorgung | 50 % `z_einzelhandel_distance`, 50 % `z_einzelhandel_near_500` | Unskalierter Z-Score-artiger Wert |
| `score_mobilitaet` | float | Score-Komponente Mobilitaet | Je 1/3 `z_haltestelle_distance`, `z_haltestellen_count_within_500m`, `z_haltestellen_linien_count` | Unskalierter Z-Score-artiger Wert |
| `score_total` | float | Gesamtbewertung der Wohnlage | Je 1/3 Zentralitaet, Versorgung und Mobilitaet plus `z_noise_penalty` und `z_rail_penalty` | Grundlage fuer Karten und Auswertung |
| `score_umwelt_diagnose` | float | Diagnosewert fuer Umwelt/Laerm | Entspricht `z_laerm_index_tag` | Wird nicht zusaetzlich in `score_total` gewichtet |
| `score_total_scaled` | float (0-10) | Skalierte Gesamtbewertung | `(score_total + 3) / 6 * 10`, auf 0-10 begrenzt und auf eine Nachkommastelle gerundet | Hoeher ist besser |
| `score_zentralitaet_scaled` | float (0-10) | Skalierte Zentralitaet | Skalierung von `score_zentralitaet` | Hoeher ist besser |
| `score_versorgung_scaled` | float (0-10) | Skalierte Versorgung | Skalierung von `score_versorgung` | Hoeher ist besser |
| `score_mobilitaet_scaled` | float (0-10) | Skalierte Mobilitaet | Skalierung von `score_mobilitaet` | Hoeher ist besser |
| `score_umwelt_diagnose_scaled` | float (0-10) | Skalierter Laerm-/Umweltdiagnosewert | Skalierung von `score_umwelt_diagnose` | Hoeher ist besser |
| `address_id` | int | Laufende technische Adress-ID | Index des GeoDataFrames nach Umprojektion, falls noch nicht vorhanden | Stabil innerhalb eines Notebook-Laufs |
| `cluster_skater_block` | int | Blockbasierter SKATER-Cluster | SKATER auf aggregierten Blockfeatures mit `DEFAULT_CLUSTER_COUNT` | Aktuelles Hauptclusterfeld |
| `cluster_skater` | int | Adressseitige SKATER-Clusterzuordnung | Aus `cluster_skater_block` auf Adressen uebertragen | Derzeit identisch zu `cluster_skater_block` |
