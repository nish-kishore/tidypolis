# source("./special_projects/obx_unit/obx_packages_01.R", local = T)
library(here)

# Inputs


#package usethis

# Option 1: Generate data files

source("./R/ob_functions.R", local = T)

raw.data <- sirfunctions::get_all_polio_data(size="medium")

v_type <- c("cVDPV 2", "cVDPV 3", "cVDPV 1")
v_type1 <- c("cVDPV 2", "cVDPV 3", "cVDPV 1",
             "cVDPV1andcVDPV2", "cVDPV2andcVDPV3","VDPV1andcVDPV2", "CombinationWild1-cVDPV 2")

start.date <- lubridate::as_date("2016-01-01")

end.date <-   lubridate::as_date("2025-05-31")



set_parameters( breakthrough_min_date = 21,
                breakthrough_middle_date = 180,
                breakthrough_max_date = 365,
                detection_pre_sia_date = 90,
                start_date = start.date,
                end_date = end.date,
                recent_sia_start_year = lubridate::year(Sys.Date())-2)
                #This is used to restrict "Recent SIA with breakthrough transmission" figures to 'recent' SIAs in f.geompoint.case())

# AFP - 3543 / Post-AFP 3561 (8)
# ENV has 236 duplicates in Post
# Contact has 6 dup epids


positives.clean.01 <- raw.data[["pos"]] |>
  dplyr::filter(dateonset >=start.date & dateonset <=end.date) |>
  dplyr::filter(measurement %in% v_type) |>
  dplyr::arrange(place.admin.0, measurement, dateonset) |>
  dplyr::select(place.admin.0, place.admin.1, place.admin.2, adm0guid, adm1guid, admin2guid, epid, measurement, dateonset, emergencegroup,
         is.breakthrough, source, report_date, admin0whocode) |>
  dplyr::rename(adm2guid = admin2guid) |>
  dplyr::distinct(epid, measurement, .keep_all = T) |>
  dplyr::mutate(surv = dplyr::case_when(
    source == "AFP" ~ "AFP",
    source == "ENV" ~ "ES",
    source %in% c("Community", "Contact", "Healthy") ~ "Other"))



# This may not be the same as the first virus in the country - can match on onsets after
df_ob <- positives.clean.01 |>
  dplyr::arrange(place.admin.0, measurement, report_date) |>
  dplyr::mutate(
    report_date  = lubridate::as_date(report_date),
    ob_flag = ifelse(place.admin.0 == dplyr::lag(place.admin.0, default = dplyr::first(place.admin.0)) &
                       measurement == dplyr::lag(measurement, default = dplyr::first(measurement)),
                     "TRUE", "FALSE"),
    ob_diff = report_date - dplyr::lag(report_date, default = dplyr::first(report_date))) |>
  dplyr::filter((dplyr::row_number()==1) |
           ob_flag == "FALSE") |>
  dplyr::filter(is.na(emergencegroup)==F)

# Add in most recent virus overall


# All seconday outbreaks
df_sec <- positives.clean.01 |>
  dplyr::arrange(place.admin.0, measurement, dateonset) |>
  dplyr::mutate(
    ob_flag = ifelse(place.admin.0 == dplyr::lag(place.admin.0, default = dplyr::first(place.admin.0)) &
                       measurement == dplyr::lag(measurement, default = dplyr::first(measurement)),
                     "TRUE", "FALSE"),
    ob_diff = dateonset - dplyr::lag(dateonset, default = dplyr::first(dateonset))) |>
  dplyr::filter((
    (ob_flag == "TRUE" & ob_diff > 395))) |>
  dplyr:: filter(is.na(emergencegroup)==F)


# test <- df_sec |>filter(place.admin.0== "KENYA")

df_all <- dplyr::bind_rows(df_ob, df_sec)


# Stoppings 120 days or note
# Importations / Exportations

df_all <- df_all |>
  dplyr::rename(
    ob_country = place.admin.0,
    ob_srt_admin1 = place.admin.1,
    ob_srt_admin2 = place.admin.2,
    ob_srt_epid = epid,
    ob_srt_onset = dateonset,
    ob_type = measurement,
    ob_srt_eg = emergencegroup,
    ob_srt_source = source,
    ob_srt_d0 = report_date) |>
  dplyr::group_by(ob_country, ob_type) |>
  dplyr::mutate(ob_count = dplyr::row_number(),
         ob_type2 = stringr::str_replace_all(ob_type, " ", ""),
         ob_id = paste0(admin0whocode, "-", ob_type2, "-", ob_count, sep = "")) |>
  dplyr::select(ob_id, ob_country, ob_srt_admin1,ob_srt_admin2, ob_srt_epid, ob_srt_onset,
         ob_type, ob_srt_eg,ob_srt_source, ob_srt_d0) |>
  dplyr::group_by(ob_country, ob_type) |>
  dplyr::mutate(
    ob_count = dplyr::row_number(),
    ob_overall = dplyr::n(),
    ob_status = ifelse(ob_count == ob_overall, "latest_ob", "prev_ob"))

# Load in first virus

df_first <- positives.clean.01 |>
  dplyr::arrange(place.admin.0, measurement, dateonset) |>
  dplyr::mutate(
    ob_flag = ifelse(place.admin.0 == dplyr::lag(place.admin.0, default = dplyr::first(place.admin.0)) &
                       measurement == dplyr::lag(measurement, default = dplyr::first(measurement)),
                     "TRUE", "FALSE"),
    diff = dateonset - dplyr::lag(dateonset, default = dplyr::first(dateonset))) |>
  dplyr::filter((dplyr::row_number()==1) |
           ob_flag == "FALSE" |
           (ob_flag == "TRUE" & diff > 395)) |>
  dplyr::filter(is.na(emergencegroup)==F) |>
  dplyr::group_by(place.admin.0, measurement) |>
  dplyr::mutate(ob_count = dplyr::row_number(),
         ob_type2 = stringr::str_replace_all(measurement, " ", ""),
         ob_id = paste0(admin0whocode, "-", ob_type2, "-", ob_count, sep = "")) |>
  dplyr::ungroup() |>
  dplyr::select(ob_id,
         epid,
         place.admin.1,
         place.admin.2,
         dateonset,
         source,
         emergencegroup,
         report_date) |>
  dplyr::rename(
    fv_epid = epid,
    fv_onset = dateonset,
    fv_eg = emergencegroup,
    fv_admin1 = place.admin.1,
    fv_admin2 = place.admin.2,
    fv_source = source,
    fv_rdate = report_date)

df_all <- dplyr::left_join(df_all, df_first, by = "ob_id")


# Pull in upper date limit:
# Pull in the date_upper limit of each range

last_ob <- df_all |>
  dplyr::filter(ob_count == ob_overall) |>
  dplyr::select(ob_id, ob_country, ob_type)

t1a <- positives.clean.01 |>
  dplyr::arrange(place.admin.0, measurement, dateonset) |>
  dplyr::group_by(place.admin.0, measurement) |>
  dplyr::summarise(date_upper = dplyr::last(dateonset)) |>
  dplyr::filter(!(place.admin.0 == "LAO PEOPLE'S DEMOCRATIC REPUBLIC" & measurement == "cVDPV 1")) |>
  dplyr::mutate(sia_date_upper = lubridate::today())

last_ob <- dplyr::left_join(last_ob, t1a, by = c("ob_country"="place.admin.0", "ob_type"="measurement")) |>
  dplyr::ungroup() |>
  dplyr::select(ob_id, date_upper, sia_date_upper)

# Fill in the Upper limit of the date range for the outbreak

df_all <- dplyr::left_join(df_all, last_ob, by = "ob_id") |>
  dplyr::arrange(ob_id) |>
  dplyr::mutate(date_upper = dplyr::if_else(is.na(date_upper)==T, dplyr::lead(fv_onset), date_upper),
         sia_date_upper = dplyr::if_else(is.na(sia_date_upper)==T, dplyr::lead(fv_onset), sia_date_upper))


#Remove secondary data frames
rm(df_first, df_ob, df_sec, last_ob,t1a)



# Add in nearest neighbor data
# Get epids for polis download:
epids_first <- paste(df_all$fv_epid, sep="' '", collapse=", ")


nn_first <- sirfunctions::edav_io(io = "read", default_dir = "GID/GIDMEA/giddatt", file_loc = "data_raw/nn_fv_obs.rds")


# Load in match for first nearest neighbor -
nn_first <- nn_first |>
  dplyr::select(EPID, `Virus Type(s)`,  `Virus Date`, VdpvNtChangesClosestMatch, EPIDClosestMatch) |>
  dplyr::filter(!(EPID == "ENV-DJI-ART--DOU-25-003" & `Virus Type(s)` == "VDPV2")) |>
  dplyr::select(-`Virus Type(s)`, -`Virus Date`) |>
  dplyr::rename(fv_nn_epid = EPIDClosestMatch,
         fv_nn_nt = VdpvNtChangesClosestMatch) |>
  dplyr::distinct(EPID, .keep_all = T) # duplicated MAA, missing anyway, took care of additional cVDPV2 detections in DJI samples above



# Merge First virus NN
df_all <- dplyr::left_join(df_all, nn_first, by = c("fv_epid"="EPID")) |>
          dplyr::mutate(fv_yr = lubridate::year(fv_onset))


rm(nn_first)

ob_epi <- list()



for (i in 1:nrow(df_all)){
  x <- df_all$ob_id[i]
  df_sub <- df_all |> dplyr::filter(ob_id == x)

  id <- df_sub$ob_id
  date_start <- df_sub$fv_onset
  date_end <- df_sub$date_upper
  ctry <- df_sub$ob_country
  sero <- df_sub$ob_type

  ob_check <- positives.clean.01 |>
    dplyr::filter(place.admin.0 == ctry &
                  measurement == sero &
                  dateonset >= date_start)


  if(nrow(ob_check)==1){

    ob_viruses <- positives.clean.01 |>
      dplyr::filter(place.admin.0 == ctry &
               measurement == sero &
               dateonset >= date_start)
  }else{
    if(df_sub$ob_count == df_sub$ob_overall){
      ob_viruses <-  positives.clean.01 |>
        dplyr::filter(place.admin.0 == ctry &
                 measurement == sero &
                 dateonset >= date_start &
                 dateonset <= date_end)
    }else{
      ob_viruses <-  positives.clean.01 |>
        dplyr::filter(place.admin.0 == ctry &
                 measurement == sero &
                 dateonset >= date_start &
                 dateonset < date_end)
    }
  }

  ob_data <- ob_viruses |>
    dplyr::arrange(dateonset) |>
    dplyr::summarise(no_detects = dplyr::n(),
              no_emerg = dplyr::n_distinct(emergencegroup),
              across(surv, list(cases = ~ sum(. == "AFP"),
                                es = ~ sum(. == "ES"),
                                other = ~ sum(. == "Other"))),
              most_recent = dplyr::last(dateonset)) |>
    dplyr::mutate(ob_id = id)

  ob_epi[[i]] <- ob_data
  rm(ob_check, ob_data, ob_viruses, df_sub)
}

op_epi2 = do.call(rbind, ob_epi)

# Merge with overall dataset, include event vs outbreak definition variable

df_all <- dplyr::left_join(df_all, op_epi2, by = "ob_id") |>
  dplyr::mutate(ob_cat = dplyr::case_when(
    no_detects == 1 & surv_es == 1 ~ "evt",
    no_detects == 1 & surv_cases == 1 ~ "evt_check",
    no_detects >= 2 ~ "obx",
    TRUE ~ "obx_check")) |>
  dplyr::select(- ob_count, -ob_overall)

rm(ob_epi, op_epi2)

# Export
df_all <- df_all |> dplyr::select(ob_cat, ob_status, fv_yr, everything() )

### Add in SIA data now - #
sia_data  <- raw.data$sia |>
  dplyr::filter(yr.sia >= lubridate::year(start.date) &
                  status == "Done")
ob_sias <- list()


# Test
sia_ken <- sia_data |>
              dplyr::filter(place.admin.0 == "KENYA") |>
              dplyr::distinct(sia.code, .keep_all=T)

for (i in 1:nrow(df_all)){

x <- "KEN-cVDPV2-3"
x <- df_all$ob_id[i]
df_sub <- df_all |> dplyr::filter(ob_id == x)

ob_start <- df_sub$ob_srt_d0
first_virus <- df_sub$fv_onset
date_end <- df_sub$sia_date_upper
ctry <- df_sub$ob_country
sero <- df_sub$ob_type
mr_virus <- df_sub$most_recent

# Pull out number of Admin1 regions
dist <- raw.data$global.dist |>
           dplyr::filter(ADM0_NAME == ctry &
                         ENDDATE == "9999-12-31")
dist <- nrow(dist)


if(sero == "cVDPV 2"){

  # Extract total number of rounds listed



  # Extract total rounds over 10% of the country covered to avoid
  sia_rds <- sia_data |>
            dplyr::filter(
              place.admin.0 == ctry &
                activity.start.date >= ob_start &
                activity.start.date <= date_end &
                vaccine.type %in% c("mOPV2", "tOPV", "nOPV2")) |>
           dplyr::group_by(sia.code) |>
           dplyr::summarise(
                 total_prov = p_count,
                 sia_prov = dplyr::n_distinct(place.admin.2),
                 cov_pct = round(sia_prov / total_prov * 100)) |>
           dplyr::filter(cov_pct >= 10)



    sia_list <- sia_rds$sia.code

  # Extract first / second / third rounds dates
  # Did virus stop after rounds
  # total number of larger rounds
  sia_list <- sia_data |>
    filter(place.admin.0 == ctry &
             activity.start.date >= date_start &
             activity.start.date <= date_end &
             vaccine.type %in% c("mOPV2", "tOPV", "nOPV2")) |>
    distinct(sia.code, .keep_all = T)

  sia_check <- sia_data |>
    filter(place.admin.0 == ctry &
             activity.start.date >= date_start &
             activity.start.date <= date_end &
             vaccine.type %in% c("mOPV2", "tOPV", "nOPV2")) |>
    group_by(sia.code) |>
    count()

}else if (sero %in% c("cVDPV 1" , "WILD 1")){
  sia_sub <- sia_data |>
    filter(place.admin.0 == ctry &
             activity.start.date >= date_start &
             activity.start.date <= date_end &
             vaccine.type %in% c("bOPV", "tOPV", "IPV + bOPV", "mOPV1")) |>
    distinct(sia.code, .keep_all = T)

  sia_check <- sia_data |>
    filter(place.admin.0 == ctry &
             activity.start.date >= date_start &
             activity.start.date <= date_end &
             vaccine.type %in% c("bOPV", "tOPV", "IPV + bOPV", "mOPV1")) |>
    group_by(sia.code) |>
    count()

}else if(sero == "cVDPV 3"){
  sia_sub <- sia_data |>
    filter(place.admin.0 == ctry &
             activity.start.date >= date_start &
             activity.start.date <= date_end &
             vaccine.type %in% c("bOPV", "tOPV", "IPV + bOPV")) |>
    distinct(sia.code, .keep_all = T)

}






# # Data Checking - All first report viruses are before
# test <- df_all |>
#           mutate(test = if_else(ob_srt_epid == fv_epid, "T", "F"),
#                  test_time = ob_srt_onset  - fv_onset)
# test <-
#   df_first <- positives.clean.01 |>
#   arrange(place.admin.0, measurement, dateonset) |>
#   mutate(
#     ob_flag = ifelse(place.admin.0 == lag(place.admin.0, default = first(place.admin.0)) &
#                        measurement == lag(measurement, default = first(measurement)),
#                      "TRUE", "FALSE"),
#     diff = dateonset - lag(dateonset, default = first(dateonset))) |>
#     filter(place.admin.0 == "DJIBOUTI" & measurement == "cVDPV 2")
# # Next validate 31 secondary outbreaks
# # Merge in nearest neighbor data
# Get outbreak stop date to add in epi data for each f






# df_ob <- positives.clean.01 |>
#   arrange(place.admin.0, measurement, report_date) |>
#   mutate(
#     report_date  = as_date(report_date),
#     ob_flag = ifelse(place.admin.0 == lag(place.admin.0, default = first(place.admin.0)) &
#                        measurement == lag(measurement, default = first(measurement)),
#                      "TRUE", "FALSE"),
#     ob_diff = report_date - lag(report_date, default = first(report_date))) |>
#   filter((row_number()==1) |
#            ob_flag == "FALSE")|>
#   filter(is.na(emergencegroup)==F)
#
#
#
#            () |>
#   rename(
#     obvirus_epid = epid,
#     obvirus_onset = dateonset,
#     ob_serotype = measurement,
#     obvirus_emergence = emergencegroup,
#     # Needs to be cleaned
#     timebtobs = ob_diff) |>
#   left_join(., ctry.abbrev, by = c("place.admin.0" = "ADM0_NAME")) |>
#   mutate(obvirus_emergence = if_else(obvirus_emergence == "CHN-SIC-1 ", "CHN-SIC-1", obvirus_emergence))




# # Load in outbreak data
# ob_data <- read_excel("./special_projects/orpg_monthly_update/obs_data_250623.xlsx")
# ob_data <- ob_data |>
#   rename_all(~str_replace_all(., "\\s+", "")) |>
#   #Remove summary data
#   slice(., 1:(n() - 3)) |>
#   #Remove WPV1 OB
#   filter(SeroType != "WILD1") |>
#   #Match to CORE
#   mutate(Country = ifelse(str_detect(Country, "IVOIRE"),"COTE D IVOIRE", Country))
#
# ob_short <- ob_data |>
#                mutate(outbreak_yr = year(OutbreakNoti.Date)) |>
#               # mutate(OutbreakNoti.Date = if_else(is.na(OutbreakNoti.Date)==F, FirstOnset, OutbreakNoti.Date)) |>
#               select(Country, EmergenceGroup, outbreak_yr, OutbreakNoti.Date)
#
#
# df <- left_join(df, ob_short, by = c("place.admin.0"="Country",  "firstvirus_emergence"="EmergenceGroup", "firstvirus_yr" = "outbreak_yr"))
#
#
# # Removes LOA which cVDPV1 outbreak started in 2015
# df <- df |>
#         filter(is.na(OutbreakNoti.Date)==F)
#
#
# # Pull in report_date and outbreak epid
# obvirus_set <- positives.clean.01 |>
#                  select(epid, place.admin.0, emergencegroup, dateonset, measurement, report_date) |>
#                  rename(
#                    obv_onset = dateonset,
#                    obv_epid = epid,
#                    obv_serotype = measurement,
#                    obv_emggrp = emergencegroup) |>
#
#
# df_test <- left_join(df, obvirus_set, by = c("place.admin.0" = "place.admin.0",
#                                              "serotype"="obv_serotype",
#                                              "OutbreakNoti.Date" = "report_date",
#                                              "firstvirus_onset" = "obv_onset"))



# OutbreakNoti.Date = if_else(is.na(OutbreakNoti.Date)==T, FirstVirus, OutbreakNoti.Date))

# ## Note to Nick: Some duplicates on Epids in the files when cross-referencing here:
#
# # Need to add in the reported date for all
# afp <- raw.data[["afp"]] |>
#   filter(dateonset >=start.date & dateonset <=end.date) |>
#   filter(cdc.classification.all %in% v_type1) |>
#   mutate(datenotificationtohq = as.Date.character(datenotificationtohq, "%Y-%m-%d")) |>
#   select(epid, cdc.classification.all2, datenotificationtohq)
#
# df1 <- left_join( positives.clean.01, afp, by = "epid")
#
# # Datacheck -> no epid match in AFP file
# qc1 <- anti_join( positives.clean.01, afp, by = "epid") |>
#   filter(source == "AFP")
#
# qc2 <- df1 |> filter(source == "AFP" & is.na(datenotificationtohq)==T)
#
# rm(afp)
#
# # Add in ES  ## One Duplicated ES Sample Num in ENV - Need to check later
#
# es <- raw.data[["es"]] |>
#   mutate(collection.date = dmy(collection.date)) |>
#   filter(collection.date >=start.date & collection.date <=end.date &
#            vdpv ==1 & vdpv.classification %in% c("Circulating", "Circulating, Pending", "Ambiguous, Circulating")) |>
#   select(env.sample.id, date.notification.to.hq) |>
#   mutate(date.notification.to.hq = dmy(date.notification.to.hq)) |>
#   rename(
#     epid = env.sample.id,
#     datenotificationtohq_es = date.notification.to.hq) |>
#   distinct(epid, .keep_all = TRUE)
#
# df1 <- left_join( df1, es, by = "epid")
# ###########################################
# # Two EPIDS may be miscodded - shouldn't imapct anlysis without dates.
# # qc3 <- anti_join(positives.clean.01, es, by = "epid") |>
# #   filter(source == "ENV")
# #
# # # All missing Date notification to HQ
# # qc4 <- df1 |> filter(source == "ENV" & is.na(datenotificationtohq_es)==T)
# # rm(es)
#
#
# # Contacts has one additional epid than the other
# # Fix coding style
# other <- raw.data[["other"]] |>
#   filter(cdc.classification.all %in% v_type &
#            datestool1 >=start.date & datestool1 <=end.date) |>
#   mutate(datenotificationtohq_other = parse_date_time(datenotificationtohq, orders = c('dmy', 'ymd'))) |>
#   select(epid, datenotificationtohq_other)
#
# df1 <- left_join( df1, other, by = "epid")

# # Other QC -> other needs to be added in
# qc3 <- anti_join(positives.clean.01, es, by = "epid") |>
#   filter(source == "ENV")
#
#
# rm(other)
#
# ##### Build out date variable
# df1 <- df1 |>
#   mutate(datehq = case_when(
#     source == "AFP"  ~ datenotificationtohq,
#     source == "ENV"  ~ datenotificationtohq_es,
#     source %in% c("Community", "Contact", "Healthy") ~ datenotificationtohq_other))
#

#create country abbreviations

ctry.abbrev <- raw.data[["ctry.pop"]] |>
  select(ADM0_NAME, ISO_3_CODE) |>
  distinct()



#linking SIAs, exluding r0s
#matching SIAs to cases based on SIA impact report methodology
#after case matching cluster analysis to get rounds

# source("./special_projects/obx_unit/timeliness_report/ob_sia_classify.R")
#source("./special_projects/obx_unit/timeliness_report/ob_sia_classify_01.R")

case.sia <- sia.case.clean |>
  select(-c("epid", "measurement", "dateonset", "yr.onset",
            "emergencegroup", "source", "is.breakthrough","timetocase")) |>
  distinct() |>
  cluster_dates_for_sias() |>
  #adding a hard fix to SIA data to fix Pakistan names to match virus
  #place admin1 names now GB and KP to match EOC names
  mutate(place.admin.1=ifelse(place.admin.1=="KHYBER PAKHTOON", "KP", place.admin.1),
         place.admin.1=ifelse(place.admin.1=="KPTD", "KP", place.admin.1),
         place.admin.1=ifelse(place.admin.1=="KPAKHTUNKHWA", "KP", place.admin.1),
         place.admin.1=ifelse(place.admin.1=="GBALTISTAN", "GB", place.admin.1),
         place.admin.1=ifelse(place.admin.1=="GILGIT BALTISTAN", "GB", place.admin.1))

#create df of just vax types by SIA for easy joining
vax.types <- case.sia |>
  select(sia.sub.activity.code, vaccine.type) |>
  distinct()

#sia.rounds <- sia.clean |>
#  distinct() |>
#  cluster_dates_for_sias() |>
#adding a hard fix to SIA data to fix Pakistan names to match virus
#place admin1 names now GB and KP to match EOC names
#  mutate(place.admin.1=ifelse(place.admin.1=="KHYBER PAKHTOON", "KP", place.admin.1),
#         place.admin.1=ifelse(place.admin.1=="KPTD", "KP", place.admin.1),
#         place.admin.1=ifelse(place.admin.1=="KPAKHTUNKHWA", "KP", place.admin.1),
#         place.admin.1=ifelse(place.admin.1=="GBALTISTAN", "GB", place.admin.1),
#         place.admin.1=ifelse(place.admin.1=="GILGIT BALTISTAN", "GB", place.admin.1))



#rejoin the dropped vars from the previous step
case.sia.01 <- sia.case.clean |>
  left_join(., case.sia, by = c("sia.sub.activity.code", "place.admin.0", "place.admin.1", "place.admin.2",
                                "sub.activity.start.date", "sub.activity.end.date", "vaccine.type",
                                "adm0guid", "adm1guid", "adm2guid", "yr.sia", "admin.coverage.%",
                                "sia.type"))



#after clustering SIAs join to positives before applying breakthrough def
#case.sia.01 <- full_join(sia.rounds, positives.clean.01,
#                         by = c("adm0guid",
#                                "adm1guid",
#                                "adm2guid" = "admin2guid",
#                                "place.admin.1",
#                                "place.admin.0",
#                                "place.admin.2"))|>
#  filter(!is.na(sia.sub.activity.code) == T) |>
#  mutate(timetocase = dateonset-sub.activity.start.date)



#Apply Breakthrough definitions at district level
# and incorporate first breakthrough case and emergence

case.sia.02 <- create_case_sia_02(case.sia.01, breakthrough_min_date = load_parameters()$breakthrough_min_date)


#all breakthrough cases
all.breakthrough <- case.sia.01 |>
  mutate(break.case = ifelse(timetocase >= load_parameters()$breakthrough_min_date & timetocase <= load_parameters()$breakthrough_max_date, 1, 0)) |>
  filter(break.case == 1)


#determining all breakthrough cases and the immediately preceding SIA
#all.breakthrough.first.sia <- case.sia.01 |>
#filter(timetocase >= 0) |>
#mutate(break.case = ifelse(timetocase >= load_parameters()$breakthrough_min_date & timetocase <= load_parameters()$breakthrough_max_date, 1, 0)) |>
#filter(break.case == 1) |>
#group_by(epid) |>
#arrange(timetocase, .by_group = T) |>
#slice(1) |>
#ungroup()

#from all breakthrough cases, determine the first SIA following onset date
#all.breakthrough.second.sia <- case.sia.01 |>
#filter(epid %in% all.breakthrough.first.sia$epid & timetocase < 0) |>
#group_by(epid) |>
#arrange(desc(timetocase), .by_group = T) |>
#slice(1) |>
#ungroup() |>
#select(epid, sia.sub.activity.code, sub.activity.start.date, sub.activity.end.date, vaccine.type, timetocase, round.num) |>
#rename(next.sia.code = sia.sub.activity.code,
#       next.sia.start.date = sub.activity.start.date,
#       next.sia.end.date = sub.activity.end.date,
#       next.sia.vax.type = vaccine.type,
#       time.to.next.sia = timetocase,
#       next.round.num = round.num)


# Need to add in overall virus count to limit difference between countries as a QC
# Need to count breakthrough virsus only - use 90 days as a proxy


######### Make outbreak data base based on 13 months between detections.


df <- positives.clean.01 |>
  mutate(
    ob_flag = ifelse(place.admin.0 == lag(place.admin.0, default = first(place.admin.0)) &
                       measurement == lag(measurement, default = first(measurement)),
                     "TRUE", "FALSE"),
    diff = dateonset - lag(dateonset, default = first(dateonset))) |>
  filter((row_number()==1) |
           ob_flag == "FALSE" |
           (ob_flag == "TRUE" & diff > 395)) |>
  rename(
    firstvirus_epid = epid,
    firstvirus_onset = dateonset,
    serotype = measurement,
    firstvirus_emergence = emergencegroup,
    # Needs to be cleaned
    timebtobs = diff) |>
  left_join(., ctry.abbrev, by = c("place.admin.0" = "ADM0_NAME"))

# Load in Outbreak Notification Date







# Add Heading
mutate(ob.code = str_replace_all(paste(ISO_3_CODE,"-",serotype,"-1"), " ", ""))





## Notes from Meeting:

# Code NIE-Serotype-OutbreakNo
# Adding Subsequent Breakthroughs NIE-cVDPV2-1-1
# Linking to SIAs excluding R0s
#is.breakthrough


#for Keri
#needs to keep country, ob.code (for first ob in country), date of onset, date notif to hq, date first sia, date second sia, is.breakthrough
first.sia <- case.sia.01 |>
  filter(timetocase < 0) |>
  group_by(epid) |>
  arrange(desc(timetocase), .by_group = T) |>
  slice(1) |>
  ungroup()

second.sia <- case.sia.01 |>
  filter(timetocase < 0) |>
  group_by(epid) |>
  arrange(desc(timetocase), .by_group = T) |>
  slice(2) |>
  ungroup()

df.01 <- left_join(df, first.sia |>
                     select(sia.sub.activity.code, sub.activity.start.date, epid),
                   by = c("firstvirus_epid" = "epid")) |>
  left_join(., second.sia |>
              select(sia.sub.activity.code, sub.activity.start.date, epid) |>
              rename(next.sia.sub.activity.code = sia.sub.activity.code, next.sia.start.date = sub.activity.start.date),
            by = c("firstvirus_epid" = "epid")) |>
  left_join(vax.types, by = "sia.sub.activity.code") |>
  rename(first.vax.type = vaccine.type) |>
  left_join(vax.types, by = c("next.sia.sub.activity.code" = "sia.sub.activity.code")) |>
  rename(second.vax.type = vaccine.type)

ctry.region <- raw.data$ctry.pop |>
  select(ADM0_NAME, WHO_REGION, ISO_3_CODE) |>
  group_by(ADM0_NAME) |>
  distinct()

df.02 <- left_join(df.01, df1 |>
                     select(epid, datehq), by = c("firstvirus_epid" = "epid")) |>
  left_join(., ctry.region |> select(-ISO_3_CODE), by=c("place.admin.0" = "ADM0_NAME"))


# trying to create df where each df1 case is the start of an outbreak,link in other breakthroughs
# with SIA info

#mutating renaming and subsetting vars to get into format we can use better
df.03 <- df.02 |>
  mutate(ob.code = paste0(ISO_3_CODE, "-", serotype, "-", yr.onset),
         ob.id = ob.code,
         epid = firstvirus_epid) |>
  select(firstvirus_epid, epid, serotype, firstvirus_onset, firstvirus_emergence, yr.onset, source, WHO_REGION,
         place.admin.0, place.admin.1, place.admin.2, adm0guid, adm1guid, adm2guid, ob.code, ob_flag,
         ob.id, first.sia = sia.sub.activity.code, first.sia.start.date = sub.activity.start.date, first.vax.type,
         second.sia = next.sia.sub.activity.code, second.sia.start.date = next.sia.start.date, second.vax.type, datehq)

#identifying breakthrough cases from all.breakthrough to bring into df.03
#pulling out outbreak "parent" ids in order to appropriately assign to breakthrough cases
ob.parent.ids <- df.03 |>
  select(ob.code, first.sia, second.sia)


all.breakthrough.to.link <- all.breakthrough |>
  filter(sia.sub.activity.code %in% df.01$sia.sub.activity.code | sia.sub.activity.code %in% df.01$next.sia.sub.activity.code) |>
  left_join(ctry.region, by = c("place.admin.0" = "ADM0_NAME")) |>
  arrange(sia.sub.activity.code) |>
  slice(1, .by = epid) |>
  select(sia.sub.activity.code, place.admin.0, place.admin.1, place.admin.2, sub.activity.start.date, vaccine.type,
         timetocase, epid, yr.onset, serotype = measurement, dateonset, emergencegroup, source,
         sia.type) |>
  #join in the outbreak ids by first and second sia codes in order to appropriately assing ids to breakthrough cases
  left_join(ob.parent.ids, by = c("sia.sub.activity.code" = "first.sia")) |>
  left_join(ob.parent.ids, by = c("sia.sub.activity.code" = "second.sia")) |>
  mutate(ob.code = ifelse(is.na(ob.code.x), ob.code.y, ob.code.x)) |>
  group_by(ob.code) |>
  arrange(dateonset, .by_group = T) |>
  mutate(ob.id = paste0(ob.code, "-", row_number())) |>
  ungroup() |>
  select(-c("ob.code.x", "ob.code.y", "first.sia", "second.sia"))


#percentage of provinces and districts w/ a breakthrough case
# calculate number of provinces and districts per country
long.prov <- sirfunctions::load_clean_prov_sp(type = "long")
long.prov$SHAPE <- NULL
prov.info <- long.prov |>
  group_by(ADM0_NAME, active.year.01) |>
  mutate(prov.count = n()) |>
  ungroup() |>
  select(ADM0_NAME, active.year.01, prov.count) |>
  distinct()


long.dist <- sirfunctions::load_clean_dist_sp(type = "long")
long.dist$SHAPE <- NULL
dist.info <- long.dist |>
  group_by(ADM0_NAME, active.year.01) |>
  mutate(dist.count = n()) |>
  ungroup() |>
  select(ADM0_NAME, active.year.01, dist.count) |>
  distinct()


prov.dist.info <- full_join(prov.info, dist.info) |>
  filter(!is.na(dist.count))

rm(long.prov, long.dist, prov.info, dist.info)

breakthrough.prov.dist <- all.breakthrough.to.link |>
  group_by(ob.code) |>
  mutate(num.es = sum(source == "ENV"),
         num.afp = sum(source == "AFP"),
         num.prov = length(unique(place.admin.1)),
         num.dist = length(unique(place.admin.2))) |>
  ungroup() |>
  distinct() |>
  left_join(prov.dist.info, by = c("place.admin.0" = "ADM0_NAME", "yr.onset" = "active.year.01")) |>
  mutate(per.prov = num.prov/prov.count,
         per.dist = num.dist/dist.count) |>
  group_by(place.admin.0, ob.code) |>
  arrange(yr.onset) |>
  slice(1)|>
  select(ob.code, place.admin.0, num.afp, num.es, num.prov, num.dist, per.prov, per.dist) |>
  distinct()


#bring dfs together into "ob database"
part.1 <- df.03 |>
  select(ob.id, ob.code, ob.country = place.admin.0, firstvirus_epid, serotype, source, firstvirus_onset, firstvirus_emergence,
         datehq, first.sia, first.sia.start.date, first.vax.type, second.sia, second.sia.start.date, second.vax.type) |>
  left_join(breakthrough.prov.dist, by = c("ob.code", "ob.country" = "place.admin.0"))

ob.db <- all.breakthrough.to.link |>
  select(ob.code, ob.id, ob.country = place.admin.0, epid, source, dateonset) |>
  bind_rows(part.1) |>
  mutate(dateonset = as.Date(dateonset, format = "%Y-%m-%d"),
         dateonset = as.Date(ifelse(is.na(dateonset), firstvirus_onset, dateonset)),
         epid = ifelse(is.na(epid), firstvirus_epid, epid)) |>
  arrange(ob.country, dateonset) |>
  select(ob.code, ob.id, ob.country, epid, source, dateonset, num.prov, num.es, num.prov, per.prov, num.dist,
         per.dist, firstvirus_onset, firstvirus_epid, firstvirus_emergence, datehq,
         first.sia, first.sia.start.date, first.vax.type, second.sia, second.sia.start.date, second.vax.type)




# Get NN neighbor code on edava
sirfunctions::edav_io(io = "write", default_dir = "GID/GIDMEA/giddatt", file_loc = "data_raw/outbreaks_250409.rds", obj = obs)

