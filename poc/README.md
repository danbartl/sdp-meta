# Vergleichspilot: SDP-META und native Lakeflow-Pipelines

Dieser PoC prueft mit synthetischen Daten, ob SDP-META unseren selbst gepflegten
Bronze-/Silver-Unterbau ersetzen kann. Er aendert keinen SDP-META-Produktionscode.
Basis ist Upstream-Commit `17f5a954c87d460acfed3d815ef9046bd2ea5b72`.

**Stand: lokal ausfuehrbar und getestet; kein Databricks-Lauf erfolgt.**
Ein gruener lokaler Test ist kein Nachweis fuer AUTO CDC oder Streaming-Recovery.

## Vergleichsarme

| Arm | Ausgefuehrter Code im Workspace |
| --- | --- |
| `sdp` | Unveraendertes SDP-META aus dem Fork: Onboarding, Auto Loader, Expectations, Quarantaene, SCD1/SCD2 |
| `native` | Kleine direkte Lakeflow-Implementierung derselben Anforderungen als Kontrollgruppe |
| `framework` | Optional: echter Compiler und echte Silver-Runtime aus einem separat installierten privaten Wheel |

Der dritte Arm verwendet die direkte Bronze-Kontrollgruppe als Eingang. Er prueft
den vorhandenen Silver-Compiler und die SCD-Runtime, **nicht** die gesamte private
Bronze-/Governance-Implementierung. Deren komplette Migration bleibt ein eigener
Abnahmeschritt. Ohne `--framework-wheel` bleibt der Vergleich mit dem bestehenden
Framework im Ergebnis explizit offen. Privater Quellcode wird nicht hierher kopiert.

## Szenarien und unabhaengige Sollwerte

`src/sdp_meta_poc/cases.py` enthaelt kleine Lieferungen und von Hand festgelegte
Solltabellen. Die Ausgaben einer Engine erzeugen niemals die Sollwerte der anderen.
Verglichen werden Werte, Typen, Mehrfachzeilen, historische Intervalle, Herkunft
und die Mengen akzeptierter bzw. ausgesonderter Ereignisse.

| Stufe | Pruefung |
| --- | --- |
| initial | ZIP mit CSV; zwei gueltige und zwei fehlerhafte Datensaetze |
| incremental | Excel; Update, Delete, neuer Schluessel, exaktes Duplikat, ungueltige Operation |
| late | Verspaetete Aenderungen, auch vor einer bereits bekannten Loeschung |
| replay | Dieselbe Excel-Lieferung unter neuem Dateinamen; Bronze waechst, Silver bleibt gleich |
| no_new_files | Pipeline-Neustart ohne neue Dateien; keine neuen fachlichen Ergebnisse |
| injected_failure | Neue Lieferung mit bewusst ausgeloestem Spark-Ausfuehrungsfehler |
| recovery | Fehler deaktivieren; gleiche Lieferung ohne Full Refresh erneut verarbeiten |
| rule_change | Regelrevision 2 begrenzt neue Betraege; vorhandene Ergebnisse bleiben bestehen |

Betragswerte sind ganzzahlige Minor Units, Zeitfolgen sind BIGINT. Dezimalarithmetik,
Zeitzonen, Null-Updates und konkurrierende widerspruechliche Ereignisse brauchen
weitere native Qualifikation. Der lokale Referenzalgorithmus lehnt widerspruechliche
Ereignis-IDs oder identische Schluessel/Zeitfolgen mit verschiedenen Werten ab.

ZIP/Excel werden vor beiden Pipelines mit denselben kleinen Fixture-Adaptern
normalisiert. Ein Manifest verbindet Originaldatei und normalisierte CSV per SHA256.
Das ist kein allgemeiner Legacy-Dateiadapter und kein Malware-Scanner.

## Lokal starten

Voraussetzungen: Python 3.12 und uv. Alle Befehle ab hier im Verzeichnis `poc/`:

```powershell
uv sync --frozen --group dev
uv run --frozen python -m pytest -q
uv run --frozen python -m sdp_meta_poc.cli local
uv run --frozen python -m sdp_meta_poc.cli inspect
```

Der lokale Lauf schreibt echte Delta-Tabellen in ein neues `.state/<id>/` und
prueft alte Versionen. `report.json` kennzeichnet den Umfang als lokales Modell
und `native_status: not_run`. PySpark/JVM wird dafuer nicht gestartet.

## Databricks vorbereiten

Benutzt werden ein vorhandener Testkatalog, Unity Catalog, serverless Jobs und
serverless Lakeflow-Pipelines. Die ausfuehrende Identitaet benoetigt Rechte zum
Erstellen von Schemas, Volumes und Tabellen sowie zum Starten/Lesen der Pipelines.
Der Job und alle Pipelines muessen mit derselben oder passend berechtigten
Identitaet laufen. CLI mindestens 0.294.0, getestet offline mit 1.12.1.

Python-Umgebung: Environment Version 4 fuer Job und Pipelines, Pipeline-Kanal
`CURRENT`. Environment-Versionen fuer Pipelines sind laut
[Databricks-Dokumentation](https://docs.databricks.com/aws/en/ldp/developer/environment-versions)
Public Preview. Diese Auswahl pinnt Python/Clientbibliotheken, nicht die
serverseitige DBR-Version. Die tatsaechliche Laufzeit wird mit den Events erfasst.

```powershell
uv run --frozen python -m sdp_meta_poc.cli prepare --catalog sandbox --namespace poc_trial001
uv run --frozen python -m sdp_meta_poc.cli build
uv run --frozen python -m sdp_meta_poc.cli validate-offline
databricks bundle validate -t dev -p bank
databricks bundle deploy -t dev -p bank
databricks bundle run compare -t dev -p bank
```

`sandbox` ist ein Beispiel und muss durch euren Testkatalog ersetzt werden.
Ohne `--namespace` wird eine zufaellige Kennung erzeugt. Es werden nur Schemas
mit diesem Praefix angelegt. Bereits vorhandene Schemas werden abgelehnt.
Fuer einen neuen vollstaendigen Vergleich eine neue Namespace-Kennung waehlen,
erneut `prepare` und `deploy` ausfuehren. Der Vergleichsjob testet Wiederanlauf
innerhalb seines eigenen Ablaufs; er ist selbst kein fortsetzbarer Test-Runner.

`build` erzeugt beide Wheels unter `vendor/` und ein Manifest mit Commit und
Wheel-Hashes. Bestehende Wheels werden nicht still ueberschrieben. Bei Codeaenderungen
eine neue Paketversion und einen frischen Build verwenden, um Serverless-Caches
nicht mit verschiedenen Inhalten derselben Paketversion zu betreiben.
`resources.generated.yml`, Wheels und lokale Ergebnisse werden nicht eingecheckt.

### Bestehendes Framework und DQX hinzunehmen

Das private Framework-Wheel in einem geschuetzten UC Volume bereitstellen. Dann
statt des obigen `prepare`-Befehls:

```powershell
uv run --frozen python -m sdp_meta_poc.cli prepare --catalog sandbox --namespace poc_trial002 --framework-wheel /Volumes/private/build/wheels/lakeflow_governance-0.1.0-py3-none-any.whl --dqx
```

Wheelname und Pfad muessen zum tatsaechlichen privaten Build passen. Der Adapter
erwartet `compile_repository` und Contract-Version `governance.bank/v1alpha2`.
Die zwei synthetischen Contracts wurden lokal gegen den vorhandenen Compiler
geprueft. Der private Wheel-Hash wird beim Remote-Lauf zusaetzlich erfasst.

`--dqx` installiert DQX 0.16.0 und prueft ueber den oeffentlichen Bronze-Transform-Hook
eine konkrete DQX-Regel. Native Expectations uebernehmen weiterhin das Routing.
Geprueft wird die Annotation fehlerhafter Operationen in der Quarantaene.
Das beweist nach erfolgreichem Workspace-Lauf den Erweiterungspunkt, nicht die
vollstaendige DQX-Metrik-/Governance-Integration unseres bestehenden Frameworks.

## Ergebnisse lesen

Remote-Evidence liegt unter
`/Volumes/<catalog>/<namespace>_control/fixtures/evidence/report.json`.
Pro Stufe/Arm werden zusaetzlich einzelne Dateien geschrieben. Erfasst werden:

- Update-ID, Status, Laufzeit und Pipeline-Events inklusive Fehlerdetails;
- Pipeline-Konfiguration, Build-Hashes und verwendete DataflowSpec-Zeilen/-Versionen;
- Konfigurationsdateien und Lieferdatei-Hashes;
- Ist-Ergebnisse fuer SCD1/SCD2 sowie Herkunft und Quarantaene;
- offene Entscheidungskriterien, auch wenn alle ausgefuehrten Stufen erfolgreich sind.

Jeder Arm muss gegen die unabhaengigen Sollwerte bestehen. Ein Fehler wie fehlende
Berechtigung zaehlt nicht als erfolgreicher Fehlertest: dafuer muss der Marker
`POC_INJECTED_FAILURE` in den Events vorhanden sein. Fehlende Event-Evidence fuehrt
ebenfalls zu einem fehlgeschlagenen Test. Nach einer fehlgeschlagenen Stufe werden
die spaeteren Stufen nicht als ausgefuehrt gewertet.

Keine automatische Loeschung: erst Evidence sichern, dann Testressourcen gezielt
entfernen. `bundle destroy` allein ist keine zugesicherte Datenbereinigung.
Remote-Evidence bleibt im privaten Workspace, nicht im oeffentlichen Fork.

## Entscheidung

Siehe [Entscheidungsvorlage](DECISION.md). Der PoC gibt keine automatische
Produktionsfreigabe. Ohne Databricks-Verbindung lautet das Ergebnis weiterhin
**native Bewertung ausstehend**.
