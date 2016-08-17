# publication_analysis
Dieses Repo ist für das Thema "Large-scale Publikationsanalyse und Geodaten-Visualisierung" aus dem Modul BigData Praktikum SS2016 der Uni Leipzig.

----
## Dokumentation
Die Dokumentation ist in diesem Repository zu finden unter 
[Doku](https://github.com/BalthoriusRex/publication_analysis/blob/master/Dokumente/BigDataPraktikum_README.pdf)
.

----
## Installation
benötigte Programme:
* [Apache Spark](http://spark.apache.org/)

optionale Programme:
* [Apache Hadoop](http://hadoop.apache.org/) (ermöglicht Koppelung von Spark und HDFS)

----
## Datenquelle
Eine Kopie des [MAG](http://research.microsoft.com/en-us/projects/mag/) (Microsoft Academic Graph) wird zwingend benötigt.
Dieser ist in einem Verzeichnis eigener Wahl zu entpacken.
Das betreffende Verzeichnis muss dem Programm anschließend noch bekannt gemacht werden.

MAG - Microsoft Academic Graph
> The Microsoft Academic Graph is a heterogeneous graph containing scientific publication records, citation relationships between those publications, as well as authors, institutions, journals, conferences, and fields of study. This graph is used to power experiences in Bing, Cortana, and in Microsoft Academic.

----
## Verwendung der Java-Klassen

### SparkUtility
Innerhalb einer Session kann nur eine Spark-Instanz existieren.
Diese wird über die Klasse SparkUtility verwaltet.
Am Anfang eines Programms ist daher die Session mit *SparkUtility.init* zu erstellen und am Ende mit *SparkUtility.close* zu schließen.
Der *JavaSparkContext*, der für viele Arbeitsschritte mit Spark benötigt wird, wird ebenfalls von der SparkUtility erzeugt und ist mit 
der Methode *SparkUtility.getSC* auszulesen.
Ebenso ist der *SQLContext* über den Getter *SparkUtility.getSQL* erreichbar.
Mit diesen beiden lässt sich eine vollständige Spark-Pipeline implementieren, ohne dass sich der Entwickler um die sonstigen Details der Spark-Konfiguration kümmern muss.

Bevor es losgehen kann, muss Spark wissen, wo die einzelnen Tabellen des MAG zu finden sind.
Dazu sind in der Klasse SparkUtility die Dateipfade anzupassen.
Jede Tabelle des MAG wird in einer einzelnen Datei gespeichert, ebenso gibt es für jede Tabelle eine Variable, die ihren Dateipfad beinhaltet.
Die Variablennamen orietieren sich an den Namen der Tabellen, sodass diese leicht zu erkennen sind.
Der Speicherort der *Authors*-Tabelle wird so beispielsweise durch die Variable *file_Authors* repräsentiert.
Mit den übrigens Tabellen verhält es sich analog.
Die Pfadbezeichnungen werden in der Methode *setFileNames* gesetzt.

Zu jeder Tabelle existiert eine Java-Klasse im *database*-Package, die zum Mapping der Tabellen auf Java-Objekte mittels Spark genutzt wird.
Bei Bedarf kann also eine neue Klasse im *database*-Package erstellt werden.
Das entsprechende Mapping wird dann in der Methode *buildTables* realisiert.
Eine neue Tabelle ist in dieser Methode an Spark zu übergeben.

Zusätzlich zu den "natürlich" vorkommenden Tabellen kann der Entwickler auch Teile als SQL-Tables definieren.
Dies wird hier per Konvention in der *buildViews*-Methode erledigt.

Die Schritte sind nötig, damit die Herkunft der Daten für andere Programmteile transparent ist.

Eine weitere sehr nützliche Funktionalität der SparkUtility-Klasse ist die *printResults*-Methode, mit der ein Resultset aus der Spark-Pipeline
(erzeugt mittels *collect*-Methode eines *DataFrame*-Objektes) als CSV-Datei materialisiert werden kann.
Hierbei ist allerdings zu beachten, dass bei sehr großen Resultsets schnell der Speicher überlaufen kann.
An dieser Methode besteht noch Verbesserungsbedarf. Details dazu sind an entsprechender Stelle vermerkt.

### FieldOfStudyAnalytics
Die Klasse FieldOfStudyAnalytics ermöglicht das Auslesen von Ko-Autorschaft-Beziehungen aus dem MAG, die durch ein Forschungsgebiet eingegrenzt sind.
Die Klasse greift dazu auf die folgenden Tabellen zurück:
* FieldsOfStudy
* FieldOfStudyHierarchy
* PaperKeywords
* PaperAuthorAffiliations

Diese Klasse ist als eine vollständige Spark-Pipeline implementiert, die in der Methode *getCoAuthorShips* umgesetzt ist.
Dabei wird zu Beginn ein FieldOfStudy (kurz *FoS*) angegeben (bspw. Mathematics).
Das Programm liest dann alle Unterkategorien des angegebenen FoS aus und verwirft entsprechend der Stichworte zu jedem Paper solche Paper, die nicht dem FoS entsprechen.
Als Resultat der Verarbeitung wird eine Datei erzeugt, die als Materialisierung der Ko-Autorschaft-Beziehungen dient.

Die Ergebnistabelle besitzt das Schema \[affiliationID_1, affiliationID_2, count\], 
wobei die affiliationID-Spalten Fremdschlüssel auf Affiliations darstellen 
und die count-Spalte eine Information über die Häufigkeit jeder Ko-Autorschaft bereitstellt.

### SimpleApp
Die Klasse SimpleApp ist in der Anfangsphase des Projekts entstanden.
Sie ist fast vollständig obsolet und sollte nicht mehr verwendet werden.
Allerdings stellt sie einige Funktionalitäten bereit, die noch nicht durch andere Programme umgesetzt sind.
So enthält diese Klasse beispielsweise Unterprogramme zum Auslesen der Top-1000-Affiliations unter Betrachtung aller enthaltenen Publikationen.
Diese wurde in der Entstehung des Projekts verwendet, um die Datenmenge zu begrenzen.

Suchbegriffe für interessante Anfragen:
* affiliations_top_1000  (Bestimmen der 1000 wichtigsten Affiliations)
* coauthorships_complete_top_100  (Auslesen aller Ko-Autorschaft-Beziehungen für die 100 wichtigsten Affiliations untereinander)
* coauthorships_by_country  (Auslesen aller Ko-Autorschaft-Beziehungen mittels Mapping der Affiliations auf Länder)

### LocationDecoder
Die Klasse LocationDecoder verwendet die Geocoding-Klasse, um die Namen der Affiliations in Geokoordinaten zu übersetzen.
Dies ist zwingend nötig, da das Projekt eine Darstellung der Ko-Autorschaften auf einer Landkarte vorsieht.
Analog werden auch Ländernamen in Geokoordinaten übersetzt.
Die Ergebnisse des Geocoding werden dann in separaten Tabellen materialisiert.

### Geocoding
Die Klasse Geocoding stellt Methoden zum Erzeugen von Geokoordinaten bereit.
Dabei wird der Dienst OpenCageData angefragt und die Anfrageergebnisse entsprechend interpretiert.
Zum Arbeiten vom OpenCageData ist die Verwendung von User-Keys nötig. Um diese zu bekommen muss ein Account bei [OpenCageData](http://opencagedata.com/) angelegt werden.
Diese Klasse enthält bereits vier solche Keys, die abwechselnd für Anfragen verwendet werden (1000 Anfragen pro Key pro Tag, 1 Anfrage pro Key pro Sekunde).
Möglicherweise werden die enthaltenen Keys mit der Zeit gesperrt oder inaktiv.
Mithilfe der vier Keys sind am Tag 4000 Geocoding-Anfragen möglich, also können pro Tag 4000 Orte angefragt werden.
Da jedoch ca. 20000 Affiliations in der Affiliations-Tabelle vorkommen, sind entweder weitere Keys nötig oder die Affiliations müssen über mehrere Tage hinweg mit Koordinaten versehen werden.
Aufgrund der Tatsache, dass dies ein einmaliger Vorgang ist, ist das aber akzeptabel.
Die gefundenen Koordinaten müssen in einem Mapping von Affiliationnamen zu Koordinaten materialisiert werden, dazu werden die Klassen *Location* für Affiliations und *Country* für Länder aus dem *database*-Package verwendet.

### GenerateMapForCountry
Die Klasse GenerateMapForCountry besitzt nur eine *main*-Methode.
Sie wird verwendet, um zuvor materialisierte Ko-Autorschaft-Beziehungen zur Anzeige zu bringen.

Dabei kann die *input*-Variable mit einem Ländernamen in englischer Sprache (z.B. "Germany") belegt werden, um den Darstellungsbereich der Karte
auf das jeweilige Land zu beschränken.
Wird hier der leere String eingesetzt, so wird kein Länderfilter eingesetzt und alle bekannten Affiliations werden dargestellt.
Als dritte Option kann der Begriff "Globus" übergeben werden, damit alle Affiliations zu ihren jeweiligen Ländern zusammengefasst werden.
Dadurch entstehen Darstellungen, die Kooperationen zwischen Ländern aufzeigen.

Als zweite Eingabemöglichkeit kann die Variable *FoS* mit einem Wert belegt werden (z.B. "Mathematics").
Sofern als input nicht "Globus" gesetzt wurde, werden dann die materialisierten Daten zu Ko-Autorschaften in einem bestimmten Forschungsgebiet angezeigt.

Die Methodenaufrufe innerhalb der *main*-Methode sorgen dafür, dass Dateien erzeugt werden, die Anzeige herangezogen werden.
Genauer gesagt werden Arrays von Kanten erzeugt, die in 9 Kategorien (0 bis 8, entsprechend ihrer Häufigkeit) unterteilt werden.
Die Karte ist eine [HTML-Datei](https://github.com/BalthoriusRex/publication_analysis/blob/master/geo-vis/Visualisierung/Karten/Karte.html),
die von einem JavaScript gestützt wird.
Nach Ausführung der *GenerateMapForCountry.main*-Methode ist die Karte in einem Webbrowser zu öffnen (getestet mit Firefox 48.0).