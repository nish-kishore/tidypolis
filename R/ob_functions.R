#outbreak functions

#Cluster Function
#this function identifies "cluster" or OBX response so we can identify rounds

#' @export
cluster_dates <- function(x, seed = 1234, method = "kmeans", grouping_days = 365){

  if(method == "kmeans"){
    #prepare the data
    y <- x %>%
      #select only dates
      select(date = sub.activity.start.date) %>%
      #calculate distance from minimum date
      mutate(date = as.numeric(date - min(date))) %>%
      #normalize values for clustering
      scale()

    set.seed(seed)
    #calculate the optimal number of clusters
    optim_k <- y %>%
      #calculate optimal number of clusters using the
      #gap statistic
      {clusGap(., FUN = kmeans, nstart = 25, K.max = max(min(nrow(.)-1, nrow(.)/2), 2), B = 100)} %>%
      #extract gap statistic matrix
      {.$Tab[,"gap"]} %>%
      #calculate the max gap statistic, given the sparsity in the data
      #am not limiting to the first max SE method
      which.max()

    set.seed(seed)
    #calculate the clusters
    x$cluster <- kmeans(y, optim_k)$cluster %>%
      #clusters don't always come out in the order we want them to
      #so here we convert them into factors, relevel and then extract
      #the numeric value to ensure that the cluster numbers are in order
      {factor(., levels = unique(.))} %>%
      as.numeric()

    #outputting the method used
    x$cluster_method <- method

    return(x)
  }else{
    if(method == "mindate"){
      x <- x %>%
        mutate(cluster = as.numeric(sub.activity.start.date - min(sub.activity.start.date)),
               cluster = cut(cluster, seq(0, grouping_days*6, by = grouping_days), include.lowest = T),
               cluster = as.numeric(cluster),
               cluster_method = method)

      return(x)
    }
  }



}

#' Wrapper around the cluster_dates function to do some error checking
#'
#' @export
run_cluster_dates <- function(data, min_obs = 4, type){

  if(dir.exists(here("R", "assets")) == FALSE){
    dir.create(here("R", "assets"))
  }

  if(dir.exists(here("R", "assets", "cache")) == FALSE){
    dir.create(here("R", "assets", "cache"))
  }

  #check which locations meet minimum obs requirements
  in_data <- data %>%
    filter(vaccine.type == type) %>%
    group_by(adm2guid) %>%
    summarize(count = n())

  #check if cache exists
  cache_exists <- file.exists(here("R", "assets","cache",paste0(type,"_cluster_cache.rds")))

  if(cache_exists){
    cache <- read_rds(here("R", "assets","cache",paste0(type,"_cluster_cache.rds")))
    in_data <- setdiff(in_data, cache)

    print(paste0(nrow(in_data), " potentially new SIAs in [",type,"] found for clustering analysis"))

    #drop cache rows where the adm2guid is in in_data with a different count
    cache <- cache %>%
      filter(!(adm2guid %in% in_data$adm2guid))

    bind_rows(in_data, cache) %>%
      write_rds(here("R", "assets","cache",paste0(type,"_cluster_cache.rds")))
  }else{
    print(paste0("No cache found for [", type, "], creating cache and running clustering for ", nrow(in_data), " SIAs"))
    write_rds(in_data, here("R", "assets","cache",paste0(type,"_cluster_cache.rds")))
  }

  if(nrow(in_data) > 0){
    print("Clustering new SIA data")
    in_data <- in_data %>%
      filter(count >= min_obs)

    included <- data %>%
      filter(vaccine.type == type) %>%
      filter(adm2guid %in% in_data$adm2guid)

    #observations which didn't meet the minimum requirement
    dropped <- setdiff(filter(data, vaccine.type == type), included)

    #for data with at least a minimum number of observations
    out <- ungroup(included) %>%
      group_by(adm2guid) %>%
      group_split() %>%
      #apply function to each subset
      lapply(cluster_dates) %>%
      #bind output back together
      bind_rows()

    #error checking for situations where no data < min_obs
    if(nrow(dropped) > 0){
      #for data with low obs
      out2 <- ungroup(dropped) %>%
        group_by(adm2guid) %>%
        group_split() %>%
        lapply(function(x) cluster_dates(x, method = "mindate")) %>%
        bind_rows()
    }


    #error catching the return
    if(nrow(dropped) > 0){

      out <- bind_rows(out, out2)
    }

    #data cache
    data_cache_exists <- file.exists(here("R", "assets","cache",paste0(type,"data_cluster_cache.rds")))

    if(data_cache_exists){
      data_cache <- read_rds(here("R", "assets","cache",paste0(type,"data_cluster_cache.rds")))

      out <- filter(data_cache, !sia.sub.activity.code %in% unique(out$sia.sub.activity.code)) %>%
        bind_rows(out)
      # data_cache2 <- data_cache %>%
      #   anti_join(out, by=c("sia.sub.activity.code", "adm2guid"))
      # out <- data_cache2 %>%
      #   bind_rows(out)

      write_rds(out, here("R", "assets","cache",paste0(type,"data_cluster_cache.rds")))
    }else{
      print(paste0("No data cache found for [", type, "], creating data cache and saving clustering results for ", nrow(out), " SIAs"))
      write_rds(out, here("R", "assets","cache",paste0(type,"data_cluster_cache.rds")))
    }




  }else{
    print(paste0("No new SIA data found for [", type, "], loading cached data!"))
    out <- read_rds(here("R", "assets","cache",paste0(type,"data_cluster_cache.rds")))
  }

  return(out)


}


#' @description function to pull clean, de-duplicated SIA campaign data for different purposes within
#' the SIA impact report
#' @param start.date date, start date of SIA campaigns
#' @param end.date date, system date = today, last date of SIA campaigns, should be day of analysis
#' @param method chr, signifies type of SIA being pulled, reg includes n/mopv2, topv and bopv, planned
#' is limited to upcoming SIAs, IPV is for countries/regions that will only do IPV response
pull_clean_sia_data <- function(start.date=as.Date("2016-01-01"),
                                end.date = Sys.Date(),
                                method = 'reg'){
  if(method == "reg"){
    print("----BEGINNING SIA DATA PULL / CLEANING----")
    tick <- Sys.time()
    # read in SIA files
    sia.clean.01 <- raw.data$sia |>
      filter(activity.start.date >= start.date & activity.end.date<=end.date)

    tock <- Sys.time()

    print(paste0("Data loaded successfully!"))
    print(paste0(nrow(sia.clean.01), " records loaded in ", as.numeric(tock-tick),
                 " seconds."))

    sia.02 <- sia.clean.01 %>%
      #select variables of interest to ease error checking
      select(sia.sub.activity.code,
             status, phase, im.loaded, lqas.loaded, vaccine.type,
             sub.activity.start.date, adm2guid,
             linked.obx,
             # activity.comments,
             yr.sia, place.admin.0,
             place.admin.1, place.admin.2,sub.activity.end.date,
             vaccine.type, adm0guid, adm1guid, `admin.coverage.%`)

    print("[0/3]-Starting cleaning steps")

    print("[1/3]-Removing campaigns that did not occur")
    tick <- Sys.time()

    sia.02 <- sia.02 %>%
      #first step is to get rid of campaigns that did not occur
      #Unconfirmed was used pre-2014 for some older campaigns
      #missing status for campaigns 2004-2017 will be assigned as unconfirmed\
      mutate(
        complete.camp = case_when(
          status == "Done" |
            phase == "Completed" |
            im.loaded == "Yes" |
            lqas.loaded == "Yes" ~ "Completed",
          status == "Cancelled" |
            status == "Delayed" |
            (status == "Planned" & phase != "Completed") ~ "Not completed",
          status == "Unconfirmed" ~"Unconfirmed"),
        type.2.opv = ifelse(vaccine.type == "mOPV2" |
                              vaccine.type=="nOPV2" |
                              vaccine.type=="tOPV", 1, 0),
        type.1.opv = ifelse(vaccine.type == "bOPV" |
                              vaccine.type=="mOPV1" |
                              vaccine.type=="tOPV"|
                              vaccine.type=="IPV + bOPV", 1, 0),
        type.3.opv = ifelse(vaccine.type=="mOPV3" |
                              vaccine.type=="bOPV" |
                              vaccine.type=="tOPV" |
                              vaccine.type=="IPV + bOPV", 1, 0),
        ipv.vac = ifelse(vaccine.type == "f-IPV" |
                           vaccine.type == "IPV" |
                           vaccine.type == "IPV + bOPV", 1, 0)) %>%
      filter(complete.camp == "Completed" |
               complete.camp == "Unconfirmed")

    tock <- Sys.time()

    print(tock - tick)

    print("[2/3]-Removing duplicates")

    tick <- Sys.time()

    sia.02 <- sia.02 %>%

      #Second step is to remove duplicates:

      mutate(sub.activity.start.date = as_date(sub.activity.start.date)) %>%
      arrange(sub.activity.start.date) %>%
      group_by(adm2guid, vaccine.type) %>%
      #this creates variable that is days from last campaign of that vaccine
      mutate(camp.diff.days = as.numeric(sub.activity.start.date - lag(sub.activity.start.date)))%>%
      ungroup() %>%

      #identify SIAs that are duplicated because there are no difference in campaign days
      mutate(dup = case_when(
        camp.diff.days == 0 & !is.na(adm2guid) ~ 1,
        camp.diff.days > 0 | is.na(adm2guid) == T | is.na(camp.diff.days) == T ~ 0)) %>%
      #remove duplicates
      filter(dup != 1) %>%
      #manually removing extra duplicates
      #same date, vaccine and age range as another campaign in same dist
      filter(sia.sub.activity.code!="PAK-2021-006-1") %>%
      #this one no IM
      filter(sia.sub.activity.code!="SOM-2000-002-2")

    tock <- Sys.time()

    print(tock - tick)


    print("[3/3]-Final cleaning steps")


    sia.reg <- sia.02%>%
      filter(vaccine.type=="mOPV2" | vaccine.type=="nOPV2" | (vaccine.type=="tOPV" & sub.activity.start.date>"2017-01-01") | vaccine.type == "bOPV") %>%
      select(sia.sub.activity.code, place.admin.0, place.admin.1, place.admin.2, sub.activity.start.date, sub.activity.end.date, vaccine.type, adm0guid, adm1guid, adm2guid,
             yr.sia,
             # num.dist.incamp,
             # vac.round.num.count,
             # vac.total.rounds,
             # opv2.round.count,
             # total.opv2.rounds,
             #linked.obx, obx1, obx2, obx3, obx4,
             `admin.coverage.%`)

    return(sia.reg)
  }

  if(method == "planned"){
    print("----BEGINNING SIA DATA PULL / CLEANING----")
    tick <- Sys.time()
    # read in SIA files
    sia.clean.01 <- raw.data$sia |>
      filter(activity.start.date >= Sys.Date())

    tock <- Sys.time()

    print(paste0("Data loaded successfully!"))
    print(paste0(nrow(sia.clean.01), " records loaded in ", as.numeric(tock-tick),
                 " seconds."))

    sia.02 <- sia.clean.01 %>%
      #select variables of interest to ease error checking
      select(sia.sub.activity.code,
             status, phase, im.loaded, lqas.loaded, vaccine.type,
             sub.activity.start.date, adm2guid,
             linked.obx,
             # activity.comments,
             yr.sia, place.admin.0,
             place.admin.1, place.admin.2,sub.activity.end.date,
             vaccine.type, adm0guid, adm1guid, `admin.coverage.%`)

    print("[0/3]-Starting cleaning steps")

    print("[1/3]- subsetting to planned campaigns")
    tick <- Sys.time()

    sia.02 <- sia.02 %>%
      #first step is to get rid of campaigns that did not occur
      #Unconfirmed was used pre-2014 for some older campaigns
      #missing status for campaigns 2004-2017 will be assigned as unconfirmed\
      mutate(
        planned.camp = ifelse(phase %in% c("Forecasted", "Planned") &
                                status != "Canceled", "Planned", NA),
        complete.camp = case_when(
          status == "Done" |
            phase == "Completed" |
            im.loaded == "Yes" |
            lqas.loaded == "Yes" ~ "Completed",
          status == "Cancelled" |
            status == "Delayed" |
            (status == "Planned" & phase != "Completed") ~ "Not completed",
          status == "Unconfirmed" ~"Unconfirmed"),
        type.2.opv = ifelse(vaccine.type == "mOPV2" |
                              vaccine.type=="nOPV2" |
                              vaccine.type=="tOPV", 1, 0),
        type.1.opv = ifelse(vaccine.type == "bOPV" |
                              vaccine.type=="mOPV1" |
                              vaccine.type=="tOPV"|
                              vaccine.type=="IPV + bOPV", 1, 0),
        type.3.opv = ifelse(vaccine.type=="mOPV3" |
                              vaccine.type=="bOPV" |
                              vaccine.type=="tOPV" |
                              vaccine.type=="IPV + bOPV", 1, 0),
        ipv.vac = ifelse(vaccine.type == "f-IPV" |
                           vaccine.type == "IPV" |
                           vaccine.type == "IPV + bOPV", 1, 0)) %>%
      filter(planned.camp == "Planned" &
               year(Sys.Date()) <= yr.sia)

    tock <- Sys.time()

    print(tock - tick)

    print("[2/3]-Removing duplicates")

    tick <- Sys.time()

    sia.02 <- sia.02 %>%

      #Second step is to remove duplicates:

      mutate(sub.activity.start.date = as_date(sub.activity.start.date)) %>%
      arrange(sub.activity.start.date) %>%
      group_by(adm2guid, vaccine.type) %>%
      #this creates variable that is days from last campaign of that vaccine
      mutate(camp.diff.days = as.numeric(sub.activity.start.date - lag(sub.activity.start.date)))%>%
      ungroup() %>%

      #identify SIAs that are duplicated because there are no difference in campaign days
      mutate(dup = case_when(
        camp.diff.days == 0 & !is.na(adm2guid) ~ 1,
        camp.diff.days > 0 | is.na(adm2guid) == T | is.na(camp.diff.days) == T ~ 0)) %>%
      #remove duplicates
      filter(dup != 1) %>%
      #manually removing extra duplicates
      #same date, vaccine and age range as another campaign in same dist
      filter(sia.sub.activity.code!="PAK-2021-006-1") %>%
      #this one no IM
      filter(sia.sub.activity.code!="SOM-2000-002-2")

    tock <- Sys.time()

    print(tock - tick)


    print("[3/3]-Final cleaning steps")


    sia.planned <- sia.02 %>%
      # filter(vaccine.type=="mOPV2" | vaccine.type=="nOPV2" | (vaccine.type=="tOPV" & sub.activity.start.date>"2017-01-01") | vaccine.type == "bOPV") %>%
      select(sia.sub.activity.code, place.admin.0, place.admin.1, place.admin.2, sub.activity.start.date, sub.activity.end.date, vaccine.type, adm0guid, adm1guid, adm2guid,
             yr.sia, status, phase,
             # num.dist.incamp,
             # vac.round.num.count,
             # vac.total.rounds,
             # opv2.round.count,
             # total.opv2.rounds,
             #linked.obx, obx1, obx2, obx3, obx4,
             `admin.coverage.%`)


    return(sia.planned)
  }

  if(method == "ipv"){
    print("----BEGINNING SIA DATA PULL / CLEANING----")
    tick <- Sys.time()
    print("Connecting to S drive to pull latest data...")
    # read in SIA files
    sia.clean.01 <- raw.data$sia |>
      filter(activity.start.date >= start.date)

    tock <- Sys.time()

    print(paste0("Data loaded successfully!"))
    print(paste0(nrow(sia.clean.01), " records loaded in ", as.numeric(tock-tick),
                 " seconds."))

    sia.02 <- sia.clean.01 %>%
      #select variables of interest to ease error checking
      select(sia.sub.activity.code,
             status, phase, im.loaded, lqas.loaded, vaccine.type,
             sub.activity.start.date, adm2guid,
             linked.obx,
             # activity.comments,
             yr.sia, place.admin.0,
             place.admin.1, place.admin.2,sub.activity.end.date,
             vaccine.type, adm0guid, adm1guid, `admin.coverage.%`)

    print("[0/3]-Starting cleaning steps")

    print("[1/3]-Removing campaigns that did not occur")
    tick <- Sys.time()

    sia.02 <- sia.02 %>%
      #first step is to get rid of campaigns that did not occur
      #Unconfirmed was used pre-2014 for some older campaigns
      #missing status for campaigns 2004-2017 will be assigned as unconfirmed\
      mutate(
        complete.camp = case_when(
          status == "Done" |
            phase == "Completed" |
            im.loaded == "Yes" |
            lqas.loaded == "Yes" ~ "Completed",
          status == "Cancelled" |
            status == "Delayed" |
            (status == "Planned" & phase != "Completed") ~ "Not completed",
          status == "Unconfirmed" ~"Unconfirmed"),
        type.2.opv = ifelse(vaccine.type == "mOPV2" |
                              vaccine.type=="nOPV2" |
                              vaccine.type=="tOPV", 1, 0),
        type.1.opv = ifelse(vaccine.type == "bOPV" |
                              vaccine.type=="mOPV1" |
                              vaccine.type=="tOPV"|
                              vaccine.type=="IPV + bOPV", 1, 0),
        type.3.opv = ifelse(vaccine.type=="mOPV3" |
                              vaccine.type=="bOPV" |
                              vaccine.type=="tOPV" |
                              vaccine.type=="IPV + bOPV", 1, 0),
        ipv.vac = ifelse(vaccine.type == "f-IPV" |
                           vaccine.type == "IPV" |
                           vaccine.type == "IPV + bOPV", 1, 0)) %>%
      filter(complete.camp == "Completed" |
               complete.camp == "Unconfirmed")

    tock <- Sys.time()

    print(tock - tick)

    print("[2/3]-Removing duplicates")

    tick <- Sys.time()

    sia.02 <- sia.02 %>%

      #Second step is to remove duplicates:

      mutate(sub.activity.start.date = as_date(sub.activity.start.date)) %>%
      arrange(sub.activity.start.date) %>%
      group_by(adm2guid, vaccine.type) %>%
      #this creates variable that is days from last campaign of that vaccine
      mutate(camp.diff.days = as.numeric(sub.activity.start.date - lag(sub.activity.start.date)))%>%
      ungroup() %>%

      #identify SIAs that are duplicated because there are no difference in campaign days
      mutate(dup = case_when(
        camp.diff.days == 0 & !is.na(adm2guid) ~ 1,
        camp.diff.days > 0 | is.na(adm2guid) == T | is.na(camp.diff.days) == T ~ 0)) %>%
      #remove duplicates
      filter(dup != 1) %>%
      #manually removing extra duplicates
      #same date, vaccine and age range as another campaign in same dist
      filter(sia.sub.activity.code!="PAK-2021-006-1") %>%
      #this one no IM
      filter(sia.sub.activity.code!="SOM-2000-002-2")

    tock <- Sys.time()

    print(tock - tick)


    print("[3/3]-Final cleaning steps")


    sia.ipv <- sia.02%>%
      filter(vaccine.type=="IPV" | vaccine.type=="f-IPV" | vaccine.type=="IPV + bOPV") %>%
      select(sia.sub.activity.code, place.admin.0, place.admin.1, place.admin.2, sub.activity.start.date, sub.activity.end.date, vaccine.type, adm0guid, adm1guid, adm2guid,
             yr.sia,
             # num.dist.incamp,
             # vac.round.num.count,
             # vac.total.rounds,
             # opv2.round.count,
             # total.opv2.rounds,
             #linked.obx, obx1, obx2, obx3, obx4,
             `admin.coverage.%`)

    return(sia.ipv)
  }
}

cluster_dates_for_sias <- function(sia.type2){


  tick <- Sys.time()
  #original vax types
  out_mopv2 <- sia.type2 %>%
    run_cluster_dates(min_obs = 4, type = "mOPV2")

  out_nopv2 <- sia.type2 %>%
    run_cluster_dates(min_obs = 4, type = "nOPV2")

  out_topv <- sia.type2 %>%
    run_cluster_dates(min_obs = 4, type = "tOPV")

  #add bopv
  out_bopv <- sia.type2 %>%
    run_cluster_dates(min_obs = 4, type = "bOPV")


  cluster <- bind_rows(out_mopv2, out_nopv2, out_topv, out_bopv) %>%
    select(sia.sub.activity.code, adm2guid, cluster)

  #merge back with SIA data

  case.sia <- left_join(sia.type2, cluster, by = c("sia.sub.activity.code"="sia.sub.activity.code", "adm2guid"="adm2guid")) %>%
    arrange(adm2guid, sub.activity.start.date) %>%
    group_by(adm2guid, vaccine.type, cluster) %>%
    mutate(round.num = row_number()) %>%
    ungroup() %>%
    group_by(adm2guid) %>%
    mutate(max.round = max(sub.activity.start.date)) %>%
    ungroup() %>%
    mutate(last.camp = ifelse(max.round == sub.activity.start.date, 1, 0))

  tock <- Sys.time()

  print(tock - tick)

  return(case.sia)

}


calc_first_break_case <- function(case.sia.01, breakthrough_min_date=load_parameters()$breakthrough_min_date){
  first.break.case <- case.sia.01 %>%
    mutate(break.case = ifelse(timetocase >= breakthrough_min_date, 1, 0)) %>%
    filter(break.case == 1)%>%
    group_by(adm2guid, sia.sub.activity.code) %>%
    mutate(first.break.case = min(dateonset)) %>%
    filter(first.break.case == dateonset) %>%
    select(adm2guid, sia.sub.activity.code, first.break.case, timetocase) %>%
    distinct() %>%
    rename(timetofirstcase=timetocase)

  return(first.break.case)
}


calc_sia_emerge <- function(case.sia.01,
                            breakthrough_middle_date = load_parameters()$breakthrough_middle_date){

  #identifying all emergences within SIA area within 365 to 0 days BEFORE SIA round to note emergences for rounds
  #note for now we will focus only on breakthrough regardless of emergence but this can be used later

  sia.emerge <- case.sia.01 %>%
    ungroup() %>%
    filter(timetocase> -breakthrough_middle_date & timetocase< breakthrough_middle_date) %>%
    select(sia.sub.activity.code, timetocase, emergencegroup)%>%
    arrange(sia.sub.activity.code, timetocase)%>%
    select(-timetocase)%>%
    distinct()%>%
    group_by(sia.sub.activity.code)%>%
    mutate(num.emerge=row_number())%>%
    mutate(num.emerge=paste("emerge", num.emerge, sep=""))%>%
    pivot_wider(names_from=num.emerge, values_from=emergencegroup)

  return(sia.emerge)
}


create_case_sia_02 <- function(case.sia.01, breakthrough_min_date=load_parameters()$breakthrough_min_date,
                               breakthrough_middle_date = load_parameters()$breakthrough_middle_date,
                               breakthrough_max_date = load_parameters()$breakthrough_max_date){

  print("----CREATING CASE.SIA.02----")

  case.sia.02 <- full_join(case.sia.01, calc_sia_emerge(case.sia.01),
                           by = c("sia.sub.activity.code" = "sia.sub.activity.code")) %>%

    mutate(new.emerge = ifelse(emergencegroup == emerge1 | emergencegroup == emerge2 |
                                 emergencegroup == emerge3 | emergencegroup == emerge4 |
                                 emergencegroup == emerge5 | emergencegroup == emerge6 |
                                 emergencegroup == emerge7 | emergencegroup == emerge8, 0, 1)) %>%

    mutate(break.through1 = case_when(timetocase < breakthrough_min_date ~"Early Case",
                                      timetocase >= breakthrough_min_date & timetocase <= breakthrough_middle_date ~ "Break through",
                                      timetocase > breakthrough_middle_date & timetocase <= breakthrough_max_date ~"Late Break through",
                                      timetocase > breakthrough_max_date ~"Very late break through",
                                      is.na(dateonset) == T ~"Never case"),
           #new emergence in response zone likely not due to current OPV use
           new.emergence.01 = ifelse(timetocase >= -30 & timetocase < breakthrough_max_date & new.emerge == 1, 1,0),
           new.emergence.01 = ifelse(is.na(new.emergence.01) == T, 0, new.emergence.01),
           #new emergence in response zone likely due to OPV2 use
           new.emergence.02 = ifelse(timetocase >= breakthrough_max_date & new.emerge == 1, 1, 0),
           new.emergence.02 = ifelse(is.na(new.emergence.02) == T, 0, new.emergence.02)) %>%

    group_by(adm2guid, sia.sub.activity.code) %>%
    mutate(num.case21.180days = sum(break.through1 == "Break through"),
           num.case180.365 = sum(break.through1 == "Late Break through"),
           num.case.after365 = sum(break.through1 == "Very late break through"),
           num.new.emerge.01 = sum(new.emergence.01),
           num.new.emerge.02 = sum(new.emergence.02)) %>%

    select(sia.sub.activity.code, place.admin.0, place.admin.1, place.admin.2,
           sub.activity.start.date, sub.activity.end.date, vaccine.type, adm0guid,
           adm1guid, adm2guid, yr.sia,
           #sia.type,
           # num.dist.incamp,
           # round.num.01,
           # vac.round.num.count,
           # vac.total.rounds,
           # opv2.round.count,
           # total.opv2.rounds,
           `admin.coverage.%`, #linked.obx, obx1, obx2, obx3, obx4,
           emerge1, emerge2,
           emerge3, emerge4, emerge5, emerge6, emerge7, emerge8, num.case21.180days,
           num.case180.365, num.case.after365, num.new.emerge.01, num.new.emerge.02, cluster, round.num) %>%

    #add cluster and round number
    ungroup() %>%
    distinct() %>%

    #any early break through of transmission
    mutate(breakthrough.01 = ifelse(num.case21.180days > 0, 1, 0),
           #transmission 180-365 days
           breakthrough.02 = ifelse(num.case180.365 > 0, 1, 0)) %>%
    full_join(., calc_first_break_case(case.sia.01, breakthrough_min_date),
              by = c("adm2guid", "sia.sub.activity.code"))

  print("----FINISHED CREATING CASE.SIA.02----")

  return(case.sia.02)

}


#Store parameters in cache
set_parameters <- function(breakthrough_min_date = NULL,
                           start_date = NULL,
                           end_date = Sys.Date(),
                           recent_sia_start_year = lubridate::year(Sys.Date())-2,
                           breakthrough_middle_date = NULL,
                           breakthrough_max_date = NULL,
                           detection_pre_sia_date = NULL){
  parameter_list <- list(
    breakthrough_min_date = breakthrough_min_date,
    start_date = start_date,
    end_date = end_date,
    recent_sia_start_year = recent_sia_start_year,
    breakthrough_middle_date = breakthrough_middle_date,
    breakthrough_max_date = breakthrough_max_date,
    detection_pre_sia_date = detection_pre_sia_date)
  #If NULL, stop and prompt for entry
  for(i in 1:length(parameter_list)){
    value <- parameter_list[i][[1]]
    if(is.null(value)){
      stop(paste0(names(parameter_list[i]), " cannot be left blank. Please re-run set_parameters() with a value specified."))
    }
  }
  #move old parameters to cache
  if(file.exists(here("R",
                      "assets",
                      "cache",
                      "report_parameters.rds")) == TRUE){
    old_parameters <- load_parameters()
    rio::export(old_parameters, here("R",
                                     "assets",
                                     "cache",
                                     "previous_report_parameters.rds"))
  }
  rio::export(parameter_list, here("R",
                                   "assets",
                                   "cache",
                                   "report_parameters.rds"))

}


#Load parameters
load_parameters <- function(){
  if(file.exists(here("R",
                      "assets",
                      "cache",
                      "report_parameters.rds")) == FALSE){
    stop("Parameters not yet specified. Please specify parameters using set_parameters().")
  }
  parameters <- rio::import(here("R",
                                 "assets",
                                 "cache",
                                 "report_parameters.rds"))
  return(parameters)
}

#Check if key parameters have changed
parameter_change_check <- function(){
  current_parameters <- load_parameters()
  prior_parameters <- rio::import(here("R",
                                       "assets",
                                       "cache",
                                       "previous_report_parameters.rds"))
  changed_parameters <- setdiff(current_parameters[c(1,2,4,5,6,7)], prior_parameters[c(1,2,4,5,6,7)])
  if(length(changed_parameters) >= 1){
    return(TRUE)
  }
  if(length(changed_parameters) == 0){
    return(FALSE)
  }
}
