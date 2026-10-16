# Lokaler Pruefstand

Stand: 16. Oktober 2026. Ausgefuehrt unter Windows mit Python 3.12.

| Pruefung | Ergebnis |
| --- | --- |
| `python -m pytest -q` | 25 Tests bestanden |
| `ruff check .` | bestanden |
| `ruff format --check .` | bestanden |
| Lokaler Szenariolauf | sechs Stufen mit echten Delta-Schreibvorgaengen bestanden |
| ZIP- und Excel-Fixtures | Normalisierung und Herkunftsmanifeste lokal geprueft |
| Optionaler Framework-Adapter | synthetische Contracts mit dem vorhandenen privaten Compiler kompiliert |
| Offline-Bundle-Schema | mit Databricks CLI 1.12.1 geprueft |
| Workspace-Bundle-Validierung | nicht erfolgreich: keine Zugangsdaten vorhanden |
| Native Pipeline-Ausfuehrung | nicht ausgefuehrt |

Die Testlaufzeit lag beim Abschlusslauf bei etwa zwei Sekunden. Das ist kein
Performancevergleich mit Lakeflow. Der lokale Polars-Algorithmus bildet fachliche
Erwartungen ab; er fuehrt weder SDP-META noch AUTO CDC aus.

Mock-basierte Orchestrierungstests pruefen insbesondere, dass Fehler und fehlende
Nachweise nicht als bestandene native Szenarien gemeldet werden. Sie ersetzen
keinen Workspace-Lauf. Das Kompilieren privater Contracts belegt ebenfalls noch
nicht die Ausfuehrbarkeit der privaten Runtime auf Databricks.

Builds erzeugen ein Manifest mit Commit, Dirty-Status und Wheel-Pruefsummen.
Laufberichte und Wheels bleiben bewusst ausserhalb der Versionsverwaltung.
Die Befehle zur Reproduktion stehen in der [Anleitung](README.md).
