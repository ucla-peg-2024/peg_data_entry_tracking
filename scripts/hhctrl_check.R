## ---------------------------
##
## Script name: hhctrl_check.R
##
## Purpose of script: To check hhctrl prog ids
##
## Author: Yufan Gong
##
## Date Created: 2024-02-05
##
## Copyright (c) Yufan Gong, 2024
## Email: ivangong@ucla.edu
##
## ---------------------------
##
## Notes: The following code is assuming that you are using Windows OS. 
##        Make sure you have read this post before you run the code:
##        https://rpubs.com/vermanica/SQL_finalProject_MicrosoftAccess
##        If you are using Mac OS, please check this link for accdb connection:
##        https://github.com/ethoinformatics/r-database-connections/blob/master/
##        And please make sure .accbd doesn't has a password if you are using
##        Mac OS.
##        For other database types, please refer this blog:
##        https://ryanpeek.org/2019-09-17-reading-databases-in-r/
## ---------------------------

# Load libraries ----------------------------------------------------------


library(readxl)
library(here)
library(rlang)
library(magrittr)
library(arsenal)
library(lubridate)
library(RODBC)
library(labelled)
library(haven)
library(glue)
library(ggpubr)
library(tidyverse)


`%notin%` <- Negate(`%in%`)


# Connect to the database -------------------------------------------------


list("hhctrl_contact_manager", "oldhhctrl_contact_manager") %>% 
  map(function(data){
    odbcConnect(dsn = data, pwd="youknowwhatitis")
  }) %>% 
  set_names("channel_peg3hhctrl_cm", "channel_oldhhctrl_cm") %>% 
  list2env(.GlobalEnv)


# Read in data ------------------------------------------------------------

#grab tbls interested


list(channel_peg3hhctrl_cm, channel_oldhhctrl_cm) %>% 
  map(function(channel){
    sqlTables(channel)$TABLE_NAME %>% 
      grep("ctrldetail|checkra|pd_household", x=., 
           value = TRUE, ignore.case = TRUE) %>% 
      discard(~str_detect(.x, "(?i)copy"))
  }) %>% 
  set_names("list_tbl_hhctrl_cm",
            "list_tbl_oldhhctrl_cm") %>% 
  list2env(.GlobalEnv)

#read in all tbls in the database and do basic cleaning

var_keep <- c("pegid","entered", "entry", "enter", "collect", 
              "initial", "interviewer", "inputby", "consent_date", 
              "parkinsondisease", "msastatus", "peg3status1", "ra_consent", 
              "RA_MainLifeHistoryQuestion", "RA_MoCA_status", "RA_PDMedNo",
              "RA_PDMedLastDose",
              "RA_PDMedCheck_status", "RA_MedicalCheck_status",
              "RA_GDS_status", "RA_QualityofLife_status", "RA_Timeline_status",
              "RA_PatientQuestion_status", "RA_Constipation_status", 
              "RA_Bristol_status", "RA_Antibiotics_status", "RA_Diet_status",
              "RA_Zarit_status",
              "bloodstatus", "phys_apptdate_status")
var_drop <- c("collected1", "date1", "initials1", "pdmsastatus", 
              "bloodcenter", "abletoenter", "wgs", "_v2", "sleep_enter", 
              "cleaned_data", "sleep_collected_date", "hscore_collected_date",
              "phys_apptdate_status2", "mainlifehistoryquestion_date", 
              "lastdose_")


list(
  list(list_tbl_hhctrl_cm, list_tbl_oldhhctrl_cm),
  list(channel_peg3hhctrl_cm, channel_oldhhctrl_cm),
  list("hhctrl_cm", "oldhhctrl_cm")
) %>% 
  pmap(function(data1, data2, data3){
    data1 %>% 
      map(function(data){
        sqlQuery(data2,
                 glue::glue("select * from [{data}];")) %>% 
          as_tibble() %>% 
          select(-matches(paste(var_drop, sep = "|"),
                          ignore.case = TRUE)) %>%
          select(matches(paste(var_keep, sep = "|"),
                         ignore.case = TRUE)) %>%
          # modify_at(vars(contains("date")), ymd) %>% 
          mutate_at(vars(matches("date", ignore.case = TRUE)), as.character) %>%
          #rename(pegid = PEGID) %>%
          rename_all(str_to_lower) %>% 
          #mutate_at(vars(pegid), as.character) %>%
          # mutate_if(is.character, ~ str_remove_all(.x," ")) %>%
          # mutate_if(is.character, toupper) %>% 
          mutate_if(is.character, ~ replace(.x, .x %in% list(""), NA)) %>% 
          # mutate_all(~case_when(. == 0 ~ NA,
          #                       . == 1 ~ "Completed",
          #                       TRUE ~ as.character(.))) %>% 
          # filter(str_length(pegid)>8 & str_detect(pegid, "^G")) %>% 
          distinct()
      }) %>%
      set_names(paste(data1, data3, sep = "_")) %>% 
      list2env(.,envir = .GlobalEnv)
  })


folder_to_check <- tbl_checkra_hhctrl_cm %>% 
  filter(pegid %in% PD_HouseholdControl_oldhhctrl_cm$pegid)

write_csv(folder_to_check, here("tables", "hhctrl_folders_tocheck.csv"), 
          na = "")
