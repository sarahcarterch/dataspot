

library(openxlsx)
library(readxl)
library(dplyr)

data <- read.csv2("datasets_dataspot.csv")

#custom.publizierende.organisation = Departement in d.
#default.piblisher = Dienststelle in d. 
#Subsammlung = Sammlung in einer Dienststelle in d. 
#Subsubsammlung = Sammlung in einer Sammlung in d. 


#Neue Spalte Subsammlung erstellen (als Beispiel: Fachstelle OGD ist eine Subsammlung in dataspot. und keine Dienststelle)
#D.h. Werte wie "Fachstelle für OGD Basel-Stadt"von default.publisher (Dienststelle) zu Subsammlung kopieren
data <- data %>%
  mutate(Subsammlung = ifelse(default.publisher %in% c("Fachstelle für OGD Basel-Stadt","Parlamentsdienst des Grossen Rates", "Stadtreinigung", "Museum der Kulturen Basel", "Staatsarchiv Basel-Stadt"), 
                              default.publisher, 
                              ""))

#Fall: Immobilien Basel-Stadt
#Werte wie "Parkhäuser Basel-Stadt" von default.publisher zu Subsubsammlung kopieren
data <- data %>%
  mutate(Subsubsammlung = ifelse(default.publisher %in% c("Parkhäuser Basel-Stadt"), 
                                 default.publisher, 
                              ""))
#Immobilienbewirtschaftung als Subsammlung der Parkhäuser (Subsubsammlung) für d. ergänzen
data <- data %>%
  mutate(Subsammlung = ifelse(Subsubsammlung == "Parkhäuser Basel-Stadt", "Immobilienbewirtschaftung", Subsammlung))
#In default.publisher: Parkhäuser zu Immobilien Basel-Stadt umbenennen
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Parkhäuser Basel-Stadt", "Immobilien Basel-Stadt", default.publisher))

#Fall: Schulferien (keine Dienststelle, nur Departement)
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Erziehungsdepartement", "", default.publisher))


#Fall: Parlamentsdienst
#Parlamentsdienst des Grossen Rats zu "Parlamentsdienst" umbenennen
data <- data %>%
  mutate(Subsammlung = ifelse(Subsammlung == "Parlamentsdienst des Grossen Rates", "Parlamentsdienst", Subsammlung))
#"Parlamentsdienst des Grossen Rats" zu "Grosser Rat" umbenennen
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Parlamentsdienst des Grossen Rates", "Grosser Rat", default.publisher))
#"Grosser Rat" zu "Parlament" umbenennen
data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "Grosser Rat", "Parlament", custom.publizierende.organisation))



#Fall: öffentlich-rechtliche Orga
#In custom.publ
data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "Öffentlich-rechtliche Organisation", "Institutionen mit staatlichen Delegierten", custom.publizierende.organisation))



#Fall: Gemeindebehörden
#Umbenennen
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Gemeinde Bettingen",
                                    "Einwohnergemeinde Bettingen", default.publisher))
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Gemeinde Riehen",
                                    "Einwohnergemeinde Riehen", default.publisher))

#Subsubsammlung "Gemeindeverwaltung" zu Einwohnergemeinde Riehen hinzufügen
data <- data %>%
  mutate(Subsammlung = ifelse(default.publisher == "Einwohnergemeinde Riehen", "Gemeindeverwaltung", Subsammlung))
#Umbenennen
data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "Gemeinde Riehen", "Gemeindebehörden", custom.publizierende.organisation))
data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "Gemeinde Bettingen", "Gemeindebehörden", custom.publizierende.organisation))
data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "Bürgergemeinde der Stadt Basel", "Gemeindebehörden", custom.publizierende.organisation))

#Fall:  Museum der Kulturen und Staatsarchiv mit Abteilung Kultur ersetzen
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Museum der Kulturen Basel", "Abteilung Kultur", default.publisher))
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Staatsarchiv Basel-Stadt", "Abteilung Kultur", default.publisher))
data <- data %>%
  mutate(Subsammlung = ifelse(Subsammlung == "Staatsarchiv Basel-Stadt", "Staatsarchiv", Subsammlung))

#Fall: OGD
#Werte "Fachstelle für OGD Basel-Stadt" in default.publisher zu "Statistisches Amt" umbenennen
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Fachstelle für OGD Basel-Stadt", "Statistisches Amt", default.publisher))

#Wert "Fachstelle für OGD Basel-Stadt" umbennen, so wie es in dataspot. steht
data <- data %>%
  mutate(Subsammlung = ifelse(Subsammlung == "Fachstelle für OGD Basel-Stadt", "Fachstelle OGD", Subsammlung))

#Fall: Stadtreinigung mit Tiefbauamt ersetzen
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Stadtreinigung", "Tiefbauamt", default.publisher))


#dcat.accrualperiodicity behalten, die verdeutschte Version hinzufügen für Metadatenfeld "Aktualisierungsdatum" in dataspot. (auf Ebene Datenset)
anhäufungsdataset <- read.xlsx("OGD_Metadatenfelder.xlsx", sheet = "WL Aktualisierungszyklus")
anhäufungsdataset <- anhäufungsdataset %>%
  rename(dcat.accrualperiodicity = 'EU-Vokabular')
data <- data %>%
  left_join(anhäufungsdataset, by = "dcat.accrualperiodicity")

#anhäufungsperiodizität aufräumen
table(data$dcat.accrualperiodicity)
data <- data %>%
  mutate(dcat.accrualperiodicity = ifelse(dcat.accrualperiodicity == "triennial", "http://publications.europa.eu/resource/authority/frequency/TRIENNIAL", dcat.accrualperiodicity))
data <- data %>%
  mutate(dcat.accrualperiodicity = ifelse(dcat.accrualperiodicity == "daily", "http://publications.europa.eu/resource/authority/frequency/DAILY", dcat.accrualperiodicity))
data <- data %>%
  mutate(dcat.accrualperiodicity = ifelse(dcat.accrualperiodicity == "irregular", "http://publications.europa.eu/resource/authority/frequency/IRREG", dcat.accrualperiodicity))
#https to http
data <- data %>%
  mutate(dcat.accrualperiodicity = ifelse(dcat.accrualperiodicity == "https://publications.europa.eu/resource/authority/frequency/IRREG", "http://publications.europa.eu/resource/authority/frequency/IRREG", dcat.accrualperiodicity))


#Neue Lizenzstandard von DCAT übernehmen 
lizenzdataset <- read.xlsx("OGD_Metadatenfelder.xlsx", sheet = "WL Rechte")
data <- data %>%
  left_join(lizenzdataset, by = "dcat_ap_ch.rights")

#Übersicht aller Dienststellen 
table(data$default.publisher)

#clean default.publisher 
#Remove datasets from Bundesamt from Datenportal-Katalog-Datensatz und Sonstige etc. 
data <- data %>%
  filter(default.publisher != "Bundesamt für Gesundheit BAG")
data <- data %>%
  filter(default.publisher != "Bundesamt für Strassen ASTRA")
data <- data %>%
  filter(default.publisher != "Bundesamt für Umwelt BAFU")
data <- data %>%
  filter(default.publisher != "ETH Zurich, Department of Biosystems Science and Engineering")
data <- data %>%
  filter(default.publisher != "MeteoSchweiz")
data <- data %>%
  filter(default.publisher != "meteoblue AG")
data <- data %>%
  filter(default.publisher != "OpenZH")
data <- data %>%
  filter(default.publisher != "Nomenklaturkommission") #UNKLAR OB GEODATENSATZ ODER ZENTRALER RECHTSDIENST (JSD)


#Umbenennen wie im Staatskalender 
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "EuroAirport Basel Mulhouse Freiburg",
                                    "Flughafen Basel-Mulhouse EuroAirport", default.publisher))
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Basler Verkehrs-Betriebe",
                                    "Basler Verkehrs-Betriebe (BVB)", default.publisher))

table(data$custom.publizierende.organisation)

#custom.publizierende.organisation umbenennen 
data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "EuroAirport Basel Mulhouse Freiburg",
                                    "Institutionen mit staatlichen Delegierten", custom.publizierende.organisation))

data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "Finanzdepartement des Kantons Basel-Stadt",
                                                    "Finanzdepartement", custom.publizierende.organisation))

data <- data %>%
  mutate(custom.publizierende.organisation = ifelse(custom.publizierende.organisation == "Staatskanzlei",
                                                    "Präsidialdepartement", custom.publizierende.organisation))


#"World" in default.territory entfernen 
table(data$default.territory)
data <- data %>%
  mutate(default.territory = ifelse(default.territory == "World", "", default.territory))
#Switzerland, Germany auf Deutsch übersetzen
data <- data %>%
  mutate(default.territory = ifelse(default.territory == "Switzerland,Germany", "Schweiz, Deutschland", default.territory))

#default.license umbenennen
table(data$default.license)
data <- data %>%
  mutate(default.license = ifelse(default.license == "Freie Nutzung. Quellenangabe ist Pflicht. Kommerzielle Nutzung nur mit Bewilligung des Datenlieferanten zulässig.", "CC BY 3.0 CH", default.license))


#Kürzel wie in dataspot. hinzufügen 
data <- data %>%
  mutate(default.publisher = ifelse(default.publisher == "Amt für Umwelt und Energie",
                                    "Amt für Umwelt und Energie (AUE)", default.publisher))


#Neue Spalte mit notwendiger Struktur für die Spalte "Sammlung" im Datenkatalog-Excel von dataspot.
data$Sammlung <- paste(data$custom.publizierende.organisation, data$default.publisher, data$Subsammlung, data$Subsubsammlung, sep = "/")
#"/" am Ende der Werte entfernen, wenn nichts mehr folgt
data$Sammlung <- gsub("/+$", "", data$Sammlung)

## geht nicht mehr: write.xlsx(data, "dataset_ready_dataspot.xlsx")

#create new column for Zeitliche Abdeckung Enddatum 

data$dcat.temporal_end <- sub(".*/", "", data$dcat.temporal)
data$dcat.temporal <- sub("/.*", "", data$dcat.temporal)

data <- data %>%
  mutate(dcat.temporal = ifelse(dcat.temporal == "1098-2023",
                                    "1098-01-01", dcat.temporal))
data <- data %>%
  mutate(dcat.temporal_end = ifelse(dcat.temporal_end == "1098-2023",
                                "2023-10-05", dcat.temporal_end))

#write.xlsx(data, "dataset_dataspot_upload_V3.xlsx")





#Schlüsselwörter formatiert
datenkatalog <- read_xlsx("My R/20241230_Datennutzungskatalog.xlsx", sheet = "Datensets")

colnames(datenkatalog) <- datenkatalog[1,]
datenkatalog <- datenkatalog[-1,]

datenkatalog$Schlüsselwörter <- gsub(",", "\n", datenkatalog$Schlüsselwörter)
write.xlsx(datenkatalog, "datenkatalog_angepasst.xlsx")


#OGD-Datenportal-Katalog einlesen
data <- read_xlsx("My R/datenportal_datenkatalog.xlsx")

# Inhalte für Tags, Themen und Referenz hinzufügen

datenkatalog$`Datenportal-Identifikation` <- as.integer(datenkatalog$`Datenportal-Identifikation`)
datenkatalog <- datenkatalog %>% filter(!is.na(`Datenportal-Identifikation`))

#OGD-Datensätze in d. im Datenportal-Katalog ausfiltern
ogd_data <- data[data$datasetid %in% datenkatalog$`Datenportal-Identifikation`, ]

#Themen formatieren
ogd_data$default.theme <- gsub(", ", "\n", ogd_data$default.theme)

#Geo- und OGD-Datensätze kennzeichnen
ogd_data$Datensatztyp <- NA

ogd_data <- ogd_data %>% 
  mutate(Datensatztyp = case_when(
    default.attributions == "Geodaten Kanton Basel-Stadt" ~ "Geo- und OGD-Datensatz",
    TRUE ~ Datensatztyp
  ))

ogd_data <- ogd_data %>%
  mutate(Datensatztyp = ifelse(is.na(Datensatztyp), "OGD-Datensatz", Datensatztyp))


write.xlsx(ogd_data, "20241230_datenportal_katalog.xlsx")

#In Excel ; bei Referenzen entfernt, damit Internetseiten geöffnet werden können

