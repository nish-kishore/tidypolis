#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @import dplyr cli stringr lubridate sf
#' @importFrom cluster clusGap
#' @importFrom doFuture registerDoFuture
#' @importFrom foreach %dopar%
#' @importFrom foreach foreach
#' @importFrom future plan
#' @importFrom httr add_headers
#' @importFrom httr content
#' @importFrom httr GET
#' @importFrom httr RETRY
#' @importFrom httr set_config
#' @importFrom httr status_code
#' @importFrom jsonlite fromJSON
#' @importFrom lifecycle deprecated
#' @importFrom progressr handlers
#' @importFrom progressr progressor
#' @importFrom progressr with_progress
#' @importFrom purrr map_df
#' @importFrom purrr walk
#' @importFrom readr col_character
#' @importFrom readr cols
#' @importFrom readr read_csv
#' @importFrom readr read_delim
#' @importFrom readr read_rds
#' @importFrom readr write_csv
#' @importFrom readr write_rds
#' @importFrom sirfunctions edav_io
#' @importFrom sirfunctions get_azure_storage_connection
#' @importFrom sirfunctions load_clean_ctry_sp
#' @importFrom sirfunctions load_clean_dist_sp
#' @importFrom sirfunctions load_clean_prov_sp
#' @importFrom sirfunctions send_teams_message
#' @importFrom skimr skim
#' @importFrom stats kmeans
#' @importFrom stats rpois
#' @importFrom tidyr pivot_longer
#' @importFrom tidyr pivot_wider
#' @importFrom tidyr replace_na
#' @importFrom tidyr separate_rows
#' @importFrom tidyr uncount
#' @importFrom utils capture.output
#' @importFrom utils globalVariables
#' @importFrom utils unzip
#' @importFrom yaml read_yaml
#' @importFrom yaml write_yaml
## usethis namespace: end
NULL

utils::globalVariables(c(
  "last_sync", "polis_update_value", "last_user", "x", "ADM0_GUID", "ADM1_GUID", "ADM1_NAME",
  "ADM2_NAME", "API_Name", "Admin 2 Guid",
  "Admin0OfficialName", "Admin1OfficialName", "Admin2OfficialName", "Area Population",
  "DateFrom", "Doses", "EPID", "Env Sample Manual Edit Id", "GUID", "Id",
  "Number of Doses", "SIA Sub-Activity Code", "SIASubActivityCode", "Sample Id",
  "Site Code", "Site Name", "Table", "Targeted Population", "Virus ID", "Virus Sequenced",
  "Virus Type(s)", "active.year.01", "name", ":=",
  ".", "ADM0_NAME", "ADM0_NAME.rep", "Admin1GUID", "Admin2GUID", "ISO_3_CODE",
  "SHAPE", "activity.end.date", "activity.start.date", "adm0guid", "adm1guid",
  "adm2guid", "admin.0", "admin.0.guid", "admin.1", "admin.1.guid", "admin.1.officialname",
  "admin.2", "admin.2.guid", "admin.2.officialname", "admin.coverage.%", "admin0guid",
  "admin1", "admin1guid", "admin2", "admin2guid", "advanced.notification", "afp_dup",
  "age.group", "age.group.%", "area.targeted.%", "calculated.age.(months)",
  "camp.diff.days", "case.miss.guid", "categorical_response_set.x", "categorical_response_set.y",
  "cdc.classification.all", "character.n_unique", "classification", "classificationvdpv",
  "clinical.admitted.date", "collect.date", "collect.yr", "collection.date",
  "combine.distinct.01", "congo_wild1", "country", "country.population.%", "ctime",
  "ctry", "ctry.guid", "dataset.lab", "date.notification.to.hq", "date.onset",
  "dateinvest", "datenotificationtohq", "datenotify", "dateonset", "datestool1",
  "datestool2", "diagnosis.final", "diff.distinct.01", "dist.guid", "doses",
  "doses.total", "dup", "dup_epid", "emergence.group", "emergence.group(s)",
  "emergencegroup", "empty.01", "env.sample.id", "env.sample.manual.edit.id",
  "epid", "epid.in.polis", "es.dups", "final.cell.culture.result", "followup.date",
  "head", "hot.case", "im.loaded", "in_new_not_old", "in_old_not_new",
  "investigation.date", "lat", "latitude", "linked.outbreak(s)", "lng", "lon",
  "longitude", "lqas.loaded", "match01", "missing.guid", "new_categorical_response_set",
  "no_match", "notification.date", "nperarm", "nt.changes", "ntchanges", "old.distinct.01",
  "old_categorical_response_set", "ontonot", "ontonot.14", "ontonot.60", "ontostool2",
  "ontostool2.03", "paralysis.asymmetric", "paralysis.onset.fever", "paralysis.rapid.progress",
  "person.sex", "place.admin.0", "place.admin.1", "place.admin.2", "poliovirustypes",
  "polis.case.id", "polis.latitude", "polis.longitude", "pons.epid", "prov.guid",
  "region.name", "response", "results.seq.date.to.program", "sabin", "same",
  "sample.condition", "sample.id", "sia.sub-activity.code", "sia.sub.activity.code",
  "site.code", "site.id", "site.name", "skim_type", "skim_variable", "status",
  "stool.1.collection.date", "stool.1.condition", "stool.2.collection.date", "stool.2.condition",
  "stool.adequacy", "stool.date.sent.to.ic.lab", "stool.date.sent.to.lab", "sub-activity.end.date",
  "sub-activity.initial.planned.date", "sub-activity.last.updated.date", "sub-activity.start.date",
  "sub.activity.end.date", "sub.activity.start.date", "surveillance.type", "surveillancetypename",
  "timeliness.01", "vaccine.source", "vaccine.type", "var_class.x", "var_class.y", "var_name",
  "variable", "vdpv.classification(s)", "vdpv.classification.id(s)", "virus.cluster",
  "virus.cluster(s)", "virus.date", "virus.id", "virus.type", "virus.type(s)", "virus.type.01",
  "virus_dup", "virustype", "virustypename", "vtype", "vtype.fixed", "wastage.factor",
  "who.region", "whoregion", "write.csv", "wrongAdmin1GUID", "wrongAdmin2GUID", "yr.sia", "yr.st", "yronset",
  "corrupted.rds", "createddate", "date_time", "dest", "emergence_group", "event", "event_type",
  "full_name", "measurement", "short_name", "time", "vaccine_source", "vdpvclassificationchangedate",
  "vdpvclassificationcode", "vdpvclassificationcode", "case.date", "last.updated.date", "sia.code", "specimen.date",
  "stool1tostool2", "sub.activity.initial.planned.date", "sub.activity.last.updated.date",
  "corrupted.rds", "time.to.response", "planned.campaigns", "ipv.campaigns", "yr.end",
  "stool1missing", "stool2missing", "Admin0GUID", "cdc.max.round", "cluster_method",
  "cdc.round.num", "cdc.last.camp", "STARTDATE", "ENDDATE", "yr.end", "Shape", "empty",
  "no.of.shapes", "report_date", "DosesOPVNumber", "geo.corrected", "wrongAdmin0GUID",
  "Admin 0 Id", "Admin 1 Id", "Admin 2 Id", "WHO Region", "WHORegion", "X",
  "cluster", "datasource", "epid_fixed", "file_name", "value", "old", "new",
  "y"
))
