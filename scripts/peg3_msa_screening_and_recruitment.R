## ---------------------------
##
## Script name: peg3_msa_screening_and_recruitment.R
##
## Purpose of script: To track the progress of the screening and recruitment 
## of MSA participants in PEG3
##
## Author: Yufan Gong
##
## Date Created: 2024-12-09
##
## Copyright (c) Yufan Gong, 2024
## Email: ivangong@ucla.edu
##
## ---------------------------
##
## Notes: The following code is assuming that you are using Windows OS. 
##        Make sure you have read this post before you run the code:
##        https://rpubs.com/vermanica/SQL_finalProject_MicrosoftAccess
##        If you are using Mac OS, please check this link for .accdb connection:
##        https://github.com/ethoinformatics/r-database-connections/blob/master/
##        And please make sure .accbd doesn't have a password if you are using
##        Mac OS.
##        For other database types, please refer to this blog:
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
library(ggthemes)
library(tidyverse)

`%notin%` <- Negate(`%in%`)


# Connect to the database -------------------------------------------------


list("case_contact_manager", "case_entry1", 
     "hhctrl_contact_manager", "hhctrl_entry1", 
     "popctrl_contact_manager", "popctrl_entry1") %>% 
  map(function(data){
    odbcConnect(dsn = data, pwd = "youknowwhatitis")
  }) %>% 
  set_names("channel_peg3case_cm", "channel_peg3case_e1",
            "channel_peg3hhctrl_cm", "channel_peg3hhctrl_e1",
            "channel_peg3popctrl_cm", "channel_peg3popctrl_e1") %>% 
  list2env(.GlobalEnv)


# Read in data ------------------------------------------------------------

#grab tbls interested

tbl_drop_e1 <- c(glue("(?i)main_part_{letters[2:9]}"), "part_e_2", "part_f2",
                 "tbllist", "list_", "list -", "DSQlogin", "backup", "mainswitch",
                 "scopa", "5min", "_old", "navigation", "meddx", "medm", "medw",
                 "medo", "supplement", "test", "sub", "antibiotics_1", 
                 "antibiotics_2", "mini mental", "pd_screening")

list(channel_peg3case_e1, channel_peg3hhctrl_e1, channel_peg3popctrl_e1) %>% 
  map(function(channel){
    sqlTables(channel)$TABLE_NAME %>% 
      grep("tbl_peg3", x=., 
           value = TRUE, ignore.case = TRUE) %>% 
      discard(~str_detect(.x, paste(tbl_drop_e1, collapse = "|(?i)")))
  }) %>% 
  set_names("list_tbl_case_e1", "list_tbl_hhctrl_e1",
            "list_tbl_popctrl_e1") %>% 
  list2env(.GlobalEnv)



list(channel_peg3case_cm, channel_peg3hhctrl_cm, channel_peg3popctrl_cm) %>% 
  map(function(channel){
    sqlTables(channel)$TABLE_NAME %>% 
      grep("patientdetail|ctrldetail|checkra|screening|recruitment|peg3_fecal|demo|cpdr|visit2", x = ., 
           value = TRUE, ignore.case = TRUE) %>% 
      discard(~str_detect(
        .x, "(?i)copy|checkra2|tbllist|stool|_0|sample|old|tracking|test|diff|patients_cpdr"))
  }) %>% 
  set_names("list_tbl_case_cm", "list_tbl_hhctrl_cm",
            "list_tbl_popctrl_cm") %>% 
  list2env(.GlobalEnv)


#read in all tbls in the database and do basic cleaning

var_keep <- c("pegid", "mailid", "entered", "entry", "enter", "collect", 
              "initial", "interviewer", "inputby", 
              "mail_status", "mailstatus", "consent_date", "mail_date",
              "enroll_date", "screeningstatus", "finalstatus",
              "parkinsondisease", "msastatus", "peg3status1", "ra_consent", 
              "RA_MainLifeHistoryQuestion", "RA_MoCA_status", "RA_PDMedNo",
              "RA_PDMedLastDose", "sex", "msa", "willinganswerdemo",
              "RA_PDMedCheck_status", "RA_MedicalCheck_status",
              "RA_GDS_status", "RA_QualityofLife_status", "RA_Timeline_status",
              "RA_PatientQuestion_status", "RA_Constipation_status", 
              "RA_Bristol_status", "RA_Antibiotics_status", "RA_Diet_status",
              "RA_Zarit_status", "RA_Blood", "RA_Stool", "IncidentID_Index",
              "bloodstatus", "stoolstatus", "phys_apptdate_status", "a3")
var_drop <- c("collected1", "date1", "initials1", "pdmsastatus", 
              "bloodcenter", "abletoenter", "wgs", "sleep_enter", 
              "cleaned_data", "sleep_collected_date", "hscore_collected_date",
              "phys_apptdate_status2", "mainlifehistoryquestion_date", 
              "lastdose_", "blooddate", "bloodno", "stooldate", "stoolno",
              "mail_status_date", "returned", "refused", "interested", "reply",
              "reason", "mailstatus1date", "mailstatus2date", "mailstatus3date")
entryinitial_todetect <- c("entered_initial", "enteredby", "entry_initial")
entrydate_todetect <- c("entered_date", "date_entered", "entry_date")
collectdate_todetect <- c("collected_date", "date_collected", "collect_date", 
                          "bs_check_date")


list(
  list(list_tbl_case_e1, list_tbl_hhctrl_e1, list_tbl_popctrl_e1, 
       list_tbl_case_cm, list_tbl_hhctrl_cm, list_tbl_popctrl_cm),
  list(channel_peg3case_e1, channel_peg3hhctrl_e1, channel_peg3popctrl_e1,
       channel_peg3case_cm, channel_peg3hhctrl_cm, channel_peg3popctrl_cm),
  list("case_e1", "hhctrl_e1", "popctrl_e1",
       "case_cm", "hhctrl_cm", "popctrl_cm")
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
          rename_with(~replace(.x, str_detect(.x, 
                                              paste(entryinitial_todetect, 
                                                    collapse = "(?i)|")), 
                               "entry_initial"), 
                      contains(entryinitial_todetect)) %>%
          rename_with(~replace(.x, str_detect(.x, 
                                              paste(entrydate_todetect, 
                                                    collapse = "(?i)|")), 
                               "entry_date"), 
                      contains(entrydate_todetect)) %>%
          rename_with(~replace(.x, str_detect(.x, 
                                              paste(collectdate_todetect, 
                                                    collapse = "(?i)|")), 
                               "collect_date"), 
                      contains(collectdate_todetect)) %>%
          rename_with(~replace(.x, str_detect(.x, "(?i)mainlifehistory"), 
                               "ra_mainlifehistory_status"), 
                      contains("mainlifehistory")) %>%
          rename_with(~replace(.x, str_detect(.x, "(?i)pdmedno"), 
                               "ra_pdmedno_status"), 
                      contains("pdmedno")) %>%
          rename_with(~replace(.x, str_detect(.x, "(?i)pdmedlastdose"), 
                               "ra_pdmedlastdose_status"), 
                      contains("pdmedlastdose")) %>%
          mutate_at(vars(starts_with("ra_")), as.character) %>%
          # mutate_if(is.character, ~ str_remove_all(.x," ")) %>%
          # mutate_if(is.character, toupper) %>% 
          mutate_if(is.character, ~ replace(.x, .x %in% list(""), NA)) %>% 
          mutate_at(vars(starts_with("ra_")), ~ case_when(. == "0" ~ NA,
                                                          . == "1" ~ "Completed",
                                                          TRUE ~ as.character(.))) %>% 
          # filter(pegid %notin% c("G00001SA", "G00001SB", "G00001SC", 
          #                        "1XXXXXXXX", "G10000SW", "G10007SW",
          #                        "g10051LL70", "G20381DD32",
          #                        "777", "456", "123")) %>% 
          # filter(str_length(pegid) > 3) %>%
          distinct()
      }) %>%
      set_names(paste(data1, data3, sep = "_")) %>% 
      list2env(.,envir = .GlobalEnv)
  })

# Data cleaning -----------------------------------------------------------

tbl_peg3_mailing <- tbl_PEG3_Screening_case_cm %>% 
  select(pegid, mailid, starts_with("mail_status"), starts_with("mail_date"),
         starts_with("screening"), msa, incidentid_index) %>% 
  filter(
    # keep the records that have least 1 mail date is not NA
    rowSums(across(starts_with("mail_date"), ~ !is.na(.x)) > 0) > 0 |
      # keep the records that have least 1 screening status is not NA
      rowSums(across(starts_with("screening"), ~ !is.na(.x)) > 0) > 0
  ) %>% 
  left_join(tbl_PEG3_PatientDetail_case_cm %>% 
              select(pegid, mailid, msastatus, 
                     phys_apptdate_status, enroll_date), 
            by = c("pegid", "mailid")) %>% 
  # get the bigger value of msa and msastatus, if there is NA, use the other value
  mutate(msastatus = pmax(msa, msastatus, na.rm = TRUE)) %>%
  select(-msa) %>% 
  # filter(!(!is.na(incidentid_index) & is.na(mailid) & is.na(enroll_date))) %>% 
  arrange(mailid)

write_csv(tbl_peg3_mailing, here("tables", "tbl_peg3_case_mailing.csv"), na = "")
