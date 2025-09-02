# Wohnlagen-Analyse

## Geocoding
Für die Geocodierung der Adressen wird ein Nominatim-Server in einem lokalen Docker-Container verwendet, sodass Anfragen in dieser Art gestellt werden können:
```
GET http://localhost:8081/search.php?addressdetails=0&q=Hauptstraße,brandenburg%20an%20der%20havel&format=jsonv2
```
Mit dem Skript ```geocoder.py``` können für beliebige CSV-Dateien mit den Spalten "Straßenname" (oder "Straßennamen"), "Hsnr" und "HsnrZus" Längen- und Breitengrad-Koordinaten ermittelt werden. Die CSV-Datei muss im gleichen Verzeichnis liegen wie das Skript und die Ausgabe wird in einer neuen Datei mit dem Suffix "_geocoded" gespeichert.
Bus- und Straßenbahnhaltestellen sind mit lokalem Nominatim und OSM-Daten schwer zu finden. Dafür wurde der offizielle Nominatim-Server von [OpenStreetMap](https://nominatim.openstreetmap.org/) unter Einhaltung der Fair Use Policy verwendet.

## Routing
Das Routing erfolgt über die [OpenRouteService API](https://openrouteservice.org/), die in einem lokalen Docker-Container betrieben wird. Die API kann über folgende URL aufgerufen werden:
```
POST http://localhost:8080/ors/v2/directions/foot-walking/geojson
```
Im HTTP Body werden die Koordinaten (erste = Startpunkt, zweite = Jahrtausendbrücke) und Parameter für die Rückgabe (geometry = Wegpunkte) übergeben:
```json
{
    "coordinates": [
        [
            12.547222024194422,
            52.43275265
        ],
        [
            12.547222024194422,
            52.43275265
        ]
    ],
    "instructions": false,
    "geometry": true,
    "preference": "recommended"
}
```

## ToDos und Verbesserungsmöglichkeiten
- POIs (Points of Interest) wie Schulen, Kindergärten, Haltestellen, Ärzte, Apotheken, Supermärkte und Restaurants könnten über die Overpass API von OpenStreetMap ermittelt werden. Dadurch entfällt die manuelle Pflege der CSV-Dateien. Es müssen nur jeweils die aktuellen Daten aus OSM-Karten bezogen werden, die bedarfsgerecht gepflegt werden können (z. B., ob Kitas tatsächlich noch geöffnet sind).
- Wesentliche Faktoren für Wohnlagenbestimmung, die noch nicht einbezogen werden:
   - Makrolage (Nähe Berlin, Autobahn, Flughafen, Risiko von Naturkatastrophen, Entwicklungsprognose laut [Zukuntsatlas](https://www.prognos.com/de/projekt/zukunftsatlas-2019) oder [Wegweiser Kommune](https://www.wegweiser-kommune.de)
   - Interne Merkmale der Adressen (Ausstattung, Baujahr, Energieeffizienz, Modernisierung, Denkmalschutz)
   - Umwelt (Nähe zu Badeseen und Grünflächen, Luftqualität)
   - Aussicht (Blick auf Wasser, Wald, Stadt, Sehenswürdigkeiten)