# Entscheidungsvorlage

Stand: 16. Oktober 2026. Bisher gibt es lokale Test- und Build-Evidence.
Es wurde kein Databricks-Workspace angesprochen und kein nativer Vergleich bestanden.
Eine Bundle-Validierung wurde versucht; sie scheiterte an fehlenden Zugangsdaten.
Die separate Offline-Pruefung verwendet das JSON-Schema der installierten CLI.

## Festgestellte Grenzen

- Der gepinnte SDP-META-Code reicht `system_sequence_by` nicht an AUTO CDC weiter.
  Bitemporalitaet ist damit kein abgedecktes Standardpattern dieses PoC.
- Der CDC-Aufruf verwendet unter anderem `where` und `flow_name`. Deren
  Kompatibilitaet mit dem konkreten Lakeflow-API muss der Workspace-Lauf zeigen.
  Der PoC veraendert oder versteckt diese Aufrufe nicht durch Monkeypatching.
- Die schnelle Polars-Ausfuehrung prueft das fachliche Modell und Delta-I/O.
  Sie fuehrt SDP-META und AUTO CDC nicht aus.
- Der optionale Framework-Arm prueft den realen Silver-Compiler und dessen Runtime,
  benutzt aber fuer Bronze die direkte Kontrollgruppe. Vollstaendige Governance-
  Paritaet ist damit nicht belegt.
- Der absichtlich ausgeloeste Spark-Fehler prueft Recovery nach einem Updatefehler.
  Prozessabbruch, Netzwerkausfall und konkurrierende Writer bleiben ungetestet.
- Die zwei Engines laufen pro Stufe sequenziell. Laufzeiten sind erste Messwerte,
  kein kontrollierter Performancebenchmark: Warm-up, Caches und Umgebung wirken mit.

## Auszufuellende Bewertung nach dem nativen Lauf

| Kriterium | Nachweis | Stand |
| --- | --- | --- |
| Bronze-Daten und Herkunft korrekt | initial/incremental, Tabellen und Dateimanifeste | offen |
| SCD1/SCD2 fachlich korrekt | alle unabhaengigen Solltabellen | offen |
| Duplikate und verspaetete Ereignisse korrekt | replay/late | offen |
| Wiederanlauf ohne verlorene oder doppelte Ergebnisse | injected_failure/recovery | offen |
| Regelwechsel und verwendete Metadaten nachvollziehbar | rule_change, Specs vor/nach Lauf | offen |
| DQX ohne Aenderung des Upstream-Codes integrierbar | Lauf mit --dqx | offen |
| Vergleich mit vorhandenem Silver-Framework | Lauf mit privatem Wheel | offen |
| Vollstaendige fachliche Governance erhalten | separate Fachabnahme | offen |
| Native Bitemporalitaet oder tragfaehiger separater Pfad | gesonderter Test | offen |
| Weniger eigener Code und geringerer Betriebsaufwand | konkret entfernbarer Code, Aufwand protokollieren | offen |

## Entscheidungsschwellen

1. **Uebernahme fuer Standard-Bronze/Silver:** relevante native Szenarien bestehen;
   benoetigte Erweiterungen funktionieren ueber oeffentliche Hooks; die Aufstellung
   entfernbarer eigener Module zeigt eine reale Vereinfachung.
2. **Gezielte Nutzung nur fuer Bronze:** Ingestion ueberzeugt, komplexe Historisierung
   und fachliche Governance bleiben sinnvoll getrennt. Genau ein Writer je Zieltabelle.
3. **Beim eigenen Unterbau bleiben:** notwendige Anpassungen verlangen dauerhaften
   Fork-Code oder umfangreiche Uebersetzungslogik; Debugging und Releases werden teurer.

Das Fork-Repository dient der Evaluation. Es ist keine Entscheidung, dauerhaft
einen eigenen SDP-META-Fork als Produkt zu pflegen.

## Quellen fuer die zu pruefenden Annahmen

- [SDP-META-Architektur](https://databrickslabs.github.io/sdp-meta/docs/concepts/architecture/)
- [CDC-Konfiguration](https://databrickslabs.github.io/sdp-meta/docs/guides/cdc/)
- [Native Expectations](https://databrickslabs.github.io/sdp-meta/docs/concepts/data-quality/)
- [Gepinnter CDC-Code](https://github.com/databrickslabs/sdp-meta/blob/17f5a954c87d460acfed3d815ef9046bd2ea5b72/src/databricks/labs/sdp_meta/dataflow_pipeline.py)
- [Pipeline-Umgebungen](https://docs.databricks.com/aws/en/ldp/developer/environment-versions)
