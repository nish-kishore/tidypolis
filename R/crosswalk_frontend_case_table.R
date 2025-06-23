#' Transform front-end case table names to backend case table equivalent
#'
#' @description
#' Transforms the front-end case table columns to the equivalent column of the
#' backend. Note, the front-end case table contains fewer columns compared to the backend.
#' The outputted file can be written as an RDS, csv, parquet, or qs2.
#'
#'
#' @param case_table_path `str` Path to the front-end case table. Must be a csv file.
#' @param output_dir_path `str` Path to the directory the case table should be outputted to.
#' Defaults to the directory of the front-end case table.
#' @param output_type `str` The format the file should be outputted as. Valid outputs are:
#' rds, rda, csv, parquet. Defaults to rds.
#'
#' @returns `NULL` quietly upon success.
#' @export
#'
#' @examples
#' \dontrun{
#' case_table_path <- "C:/Users/ABC1/Downloads/front_end_case_table.csv"
#' crosswalk_frontend_case_table(case_table_path)
#' }
crosswalk_frontend_case_table <- function(case_table_path,
                                          output_dir_path = dirname(case_table_path),
                                          output_type = "rds") {
  # Check the case table is a csv
  if (!endsWith(case_table_path, ".csv")) {
    cli::cli_abort("The case table must be a csv.")
  }

  cli::cli_progress_bar(type = "tasks", total = 4)

  # Load case table
  case <- readr::read_delim(case_table_path,
    delim = ";",
    col_types = readr::cols(.default = readr::col_character())
  )
  cli::cli_progress_update()

  # Rename variables
  case <- dplyr::rename_with(case, recode,
    `Admin 0 Guid` = "Admin0GUID",
    `Admin 0 ShapeId` = "Admin0ShapeId",
    `Admin 1 Guid` = "Admin1GUID",
    `Admin 1 ShapeId` = "Admin1ShapeId",
    `Admin 2 Guid` = "Admin2GUID",
    `Admin 2 ShapeId` = "Admin2ShapeId",
    `Advanced Notification` = "AdvancedNotification",
    `AFP Reporting Week` = "ReportingWeekAndYear",
    `CalcDosesRISI` = "CalcDosesRISI",
    `Calculated Age (months)` = "CalculatedAgeInMonth",
    `Case Date` = "CaseDate",
    `Classification` = "Classification",
    `Clinical Admitted` = "ClinicalAdmitted",
    `Clinical Admitted Date` = "ClinicalAdmittedDate",
    `Country Iso3` = "CountryISO3Code",
    `Created Date` = "CreatedDate",
    `Dataset AFP` = "DatasetAfp",
    `Dataset Lab` = "DatasetLab",
    `Dataset Manual` = "DatasetManual",
    `Dataset WVL` = "DatasetWild",
    `Date Notification To HQ` = "DateNotificationtoHQ",
    `Date Onset` = "ParalysisOnsetDate",
    `Diagnosis Final` = "DiagnosisFinal",
    `Diagnosis Other` = "DiagnosisOther",
    `Diagnosis Other Specified` = "DiagnosisOtherSpecified",
    `Doses Date Of 1st` = "DosesDateOf1st",
    `Doses Date Of 2nd` = "DosesDateOf2nd",
    `Doses Date Of 3rd` = "DosesDateOf3rd",
    `Doses Date Of 4th` = "DosesDateOf4th",
    `Doses IPV Date of Last` = "DosesIPVDateofLast",
    `Doses IPV Number` = "DosesIPVNumber",
    `Doses IPV Routine` = "DosesIPVRoutine",
    `Doses IPV SIA` = "DosesIPVSIA",
    `Doses OPV Date of Last` = "DosesOPVDateofLast",
    `Doses OPV Number` = "DosesOPVNumber",
    `Doses OPV Routine` = "DosesOPVRoutine",
    `Doses OPV SIA` = "DosesOPVSIA",
    `Emergence Group(s)` = "VdpvEmergenceGroupNames",
    `EPID` = "EPID",
    `Event Comments` = "EventComment",
    `Event Direct Importation` = "EventDirectImportation",
    `Event Immediate Source` = "EventImmediateSource",
    `Event Name` = "EventName",
    `Exact Latitude` = "ExactLatitude",
    `Exact Longitude` = "ExactLongitude",
    `Final Cell Culture Result` = "FinalCultureResult",
    `Followup Date` = "FollowupDate",
    `Followup Findings` = "FollowupFindings",
    `Investigation Date` = "InvestigationDate",
    `Is Breakthrough` = "IsBreakthrough",
    `IST` = "ISTName",
    `Last Ipv SIA Date` = "DateLastIpvSIA",
    `Last Opv SIA Date` = "DateLastOpvSIA",
    `Last Updated By` = "LastUpdateBy",
    `Last Updated Date` = "LastUpdateDate",
    `Notification Date` = "NotificationDate",
    `NPEV` = "NPEV",
    `Nt Changes` = "NtChanges",
    `nVaccine 2` = "nVACCINE2",
    `nVDPV 2` = "nVDPV2",
    `Paralysis Asymmetric` = "ParalysisAsymmetric",
    `Paralysis Confirmed` = "ParalysisConfirmed",
    `Paralysis Hot Case` = "ParalysisHotCase",
    `Paralysis Left Arm` = "ParalysisLeftArm",
    `Paralysis Left Leg` = "ParalysisLeftLeg",
    `Paralysis Onset Fever` = "ParalysisOnsetFever",
    `Paralysis Rapid Progress` = "ParalysisRapidProgress",
    `Paralysis Right Arm` = "ParalysisRightArm",
    `Paralysis Right Leg` = "ParalysisRightLeg",
    `Paralysis Site` = "ParalysisSite",
    `Paralysis Sudden` = "ParalysisSudden",
    `Person Age In Months` = "PersonAgeInMonths",
    `Person Age In Years` = "PersonAgeInYears",
    `Person Sex` = "PersonSex",
    `Place Admin 0` = "Admin0Name",
    `Place Admin 0 ISO3` = "PlaceEURAdmin0CodeISO3",
    `Place Admin 1` = "Admin1Name",
    `Place Admin 2` = "Admin2Name",
    `Place Admin 3` = "PlaceAdmin3",
    `Place EUR Admin 1 Code` = "PlaceEURAdmin1Code",
    `Place EUR Prov Distr Code` = "PlaceEURProvDistrCode",
    `Place Nearest Facility` = "PlaceNearestFacility",
    `PolIS Case ID` = "CaseManualEditId",
    `PoNS Administration Type` = "PoNS_AdministrationType",
    `PoNS Closest Match` = "PoNS_ClosestMatch",
    `PoNS Closest Match Dash` = "PoNS_ClosestMatchDash",
    `PoNS Comments` = "PoNS_Comments",
    `PoNS Country` = "PoNS_Country",
    `PoNS Dash Id` = "PoNS_DashId",
    `PoNS Environment` = "PoNS_Environment",
    `PoNS Epid` = "PoNS_EpiNum",
    `PoNS Epid formated` = "PoNS_EpiNumTransformed",
    `PoNS Expr 1005` = "PoNS_Expr1005",
    `PoNS FileName` = "PoNS_FileName",
    `PoNS Genotype` = "PoNS_Genotype",
    `PoNS Genotype2` = "PoNS_Genotype2",
    `PoNS Id` = "PoNS_Id",
    `PoNS ISO3` = "PoNS_ISO3",
    `PoNS Latitude` = "PoNS_Latitude",
    `PoNS Locality` = "PoNS_Locality",
    `PoNS Longitude` = "PoNS_Longitude",
    `PoNS Map Dist Edit` = "PoNS_MapDistEdit",
    `PoNS Matching Quality` = "PoNS_MatchingQuality",
    `PoNS Mismatches` = "PoNS_Mismatches",
    `PoNS NT Changes` = "PoNS_NTChanges",
    `PoNS OnSet Date` = "PoNS_OnSetDate",
    `PoNS Passage` = "PoNS_Passage",
    `PoNS Patient` = "PoNS_Patient",
    `PoNS Receipt Date` = "PoNS_ReceiptDate",
    `PoNS Ref Lab Id` = "PoNS_RefLabId",
    `PoNS Reference` = "PoNS_Reference",
    `PoNS Reference Name` = "PoNS_ReferenceName",
    `PoNS Region` = "PoNS_Region",
    `PoNS Seq By` = "PoNS_SeqBy",
    `PoNS Seq Date` = "PoNS_SeqDate",
    `PoNS Seq Region` = "PoNS_SeqRegion",
    `PoNS Serotype` = "PoNS_Serotype",
    `PoNS Serotype2` = "PoNS_Serotype2",
    `PoNS Source` = "PoNS_Source",
    `PoNS Spec Date` = "PoNS_SpecDate",
    `PoNS Spec Type` = "PoNS_SpecType",
    `PoNS State Province` = "PoNS_StateProv",
    `PoNS WHO Country Code` = "PoNS_WHOCountryCode",
    `PoNS Wild Cluster Name` = "PoNS_WildClusterName",
    `PoNS_ReferenceNam` = "PoNS_ReferenceName",
    `Positive Contact Case Manual Edit Id` = "PositiveContactCaseManualEditId",
    `Positive Contact Date SEQ Results to Program` = "PositiveContactDateSEQResultstoProgram",
    `Positive contact EPID` = "PositiveContactEPID",
    `Positive Contact Nt Changes` = "PositiveContactNtChanges",
    `Positive Contact Stool 1 Collection Date` = "PositiveContactStool1CollectionDate",
    `Positive Contact Vaccine Origin` = "PositiveContactVaccineOrigin",
    `Positive Contact Virus Type Id` = "PositiveContactVirusTypeId",
    `Positive Contact Virus Type Name` = "PositiveContactVirusTypeName",
    `Provisional Diagnosis` = "ProvisionalDiagnosis",
    `PV 1` = "PV1",
    `PV 2` = "PV2",
    `PV 3` = "PV3",
    `Results Seq Date to program` = "DateSEQResultstoProgram",
    `Source Advanced Notification` = "SourceAdvancedNotification",
    `Source RI Vaccination Information` = "SourceRIVaccinationInfoCode",
    `Specimen Date` = "SpecDateReceivedByNatLab",
    `Stool 1 Collection Date` = "Stool1CollectionDate",
    `Stool 1 Condition` = "Stool1Condition",
    `Stool 2 Collection Date` = "Stool2CollectionDate",
    `Stool 2 Condition` = "Stool2Condition",
    `Stool Adequacy` = "AdequateStool",
    `Stool Adequacy with Condition` = "AdequateStoolWithCondition",
    `Stool Date Sent To IC Lab` = "StoolDateSentToICLab",
    `Stool Date Sent To Lab` = "StoolDateSentToLab",
    `Surveillance Type` = "SurveillanceTypeName",
    `Total Number Of IPV / OPV Doses` = "TotalNumberOfDoses",
    `Total Number Of IPV / OPV Doses` = "DosesTotal",
    `Vaccine 1` = "VACCINE1",
    `Vaccine 2` = "VACCINE2",
    `Vaccine 3` = "VACCINE3",
    `VaccineOrigins` = "VaccineOrigins",
    `Vdpv 1` = "VDPV1",
    `Vdpv 2` = "VDPV2",
    `Vdpv 3` = "VDPV3",
    `VDPV Classification Id(s)` = "VdpvClassificationIds",
    `VDPV Classification(s)` = "VdpvClassifications",
    `Virus Cluster(s)` = "VirusClusters",
    `Virus Genotypes` = "VirusGenoTypes",
    `Virus Homology Percent` = "VirusHomologyPercent",
    `Virus Is Orphan` = "VirusIsOrphan",
    `Virus Origin` = "VirusOrigin",
    `Virus Sequenced` = "VirusSequenced",
    `Virus Source Linked to` = "VirusSourceLinkedto",
    `Virus Type(s)` = "PolioVirusTypes",
    `WHO Region` = "WHORegion",
    `Wild 1` = "WILD1",
    `Wild 2` = "WILD2",
    `Wild 3` = "WILD3",
    `X` = "Longitude",
    `Y` = "Latitude",
    `Epid in Polis` = "PoNS_EpiNumPolis"
  )
  cli::cli_progress_update()

  # Make selected columns logical
  case <- case |>
    dplyr::mutate(dplyr::across(
      dplyr::any_of(c(
        "IsBreakthrough", "NPEV",
        "VACCINE1", "VACCINE2", "VACCINE3",
        "VDPV1", "VDPV2", "VDPV3",
        "WILD1", "WILD2", "WILD3",
        "AdvancedNotification"
      )),
      \(col_name) ifelse(col_name == "Yes",
        TRUE, FALSE
      )
    ))
  cli::cli_progress_update()

  # Output path
  tidypolis_io(case, "write",
    file.path(output_dir_path, paste0("case.", output_type)),
    edav = FALSE
  )
  cli::cli_progress_update()

  invisible()
}
