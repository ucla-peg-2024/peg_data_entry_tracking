## ---------------------------
##
## Script name: udall_data_entry_tracking.R
## Purpose of script: This script is used to track the data entry process
##
## Author: Yufan Gong
##
## Date Created: 25 September, 2024
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

channel_peg1_cm <- odbcConnect(dsn = "peg1_contact_manager", pwd = "youknowwhatitis")
channel_peg1_e1 <- odbcConnect(dsn = "peg1_entry1", pwd = "youknowwhatitis")
channel_udall_cm <- odbcConnect(dsn = "udall_contact_manager", pwd = "youknowwhatitis")
channel_udall_e1 <- odbcConnect(dsn = "udall_entry1", pwd = "youknowwhatitis")




# Read in data ------------------------------------------------------------


sqlTables(channel_peg1_cm)$TABLE_NAME
sqlTables(channel_peg1_e1)$TABLE_NAME
sqlTables(channel_udall_cm)$TABLE_NAME
sqlTables(channel_udall_e1)$TABLE_NAME

tbl_drop_e1 <- c(glue("(?i)main part {letters[2:9]}"), "Find", "_2",
                 "cases", "tracking", "patients", "_SO", "Solvents", 
                 "interviewee", "_medm", "_medd", "_medw", "Antibiotics_", 
                 "_Med7", "calllog", "_resee", "diagnosis", "statistics", 
                 "xtbl", "_Data", "elig", "ExportErrors", "matching", 
                 "medical 7", "x_", "call_log")

list_tbl_peg1_contact <- sqlTables(channel_peg1_cm)$TABLE_NAME %>% 
  grep("patient", x=., 
       value = TRUE, ignore.case = TRUE) %>% 
  discard(~str_detect(.x, "(?i)copy|qry"))

list_tbl_peg1 <- sqlTables(channel_peg1_e1)$TABLE_NAME %>% 
  grep("tbl_peg1", x=., 
       value = TRUE, ignore.case = TRUE) %>% 
  discard(~str_detect(.x, paste(tbl_drop_e1, collapse = "|(?i)")))

list_tbl_UD1_contact <- sqlTables(channel_udall_cm)$TABLE_NAME %>% 
  grep("patient", x=., 
       value = TRUE, ignore.case = TRUE) %>% 
  discard(~str_detect(.x, "(?i)copy"))

list_tbl_UD1 <- sqlTables(channel_udall_e1)$TABLE_NAME %>% 
  grep("tbl_UD1", x=., 
       value = TRUE, ignore.case = TRUE) %>% 
  discard(~str_detect(.x, "(?i)copy"))


var_keep <- c("pegid","entered", "entry", "enter", "collect", 
              "initial", "interviewer", "inputby", "consent", 
              "pdstatus", "prog_status", "prog2_status", "gut_screenstatus",
              "sex", "age", "dob", "yob", "a3")
var_drop <- c("collected1", "date1", "initials1", "cleaned_data", "sleep_enter",
              "sleep_collected_date", "hscore_collected_date")

entryinitial_todetect <- c("entered_initial", "enteredby", "entry_initial", 
                           "inputby")
entrydate_todetect <- c("entered_date", "date_entered", "entry_date", 
                        "entereddate", "checklistentered", "dataentered", 
                        "testentered", "dateentered")
collectdate_todetect <- c("collected_date", "date_collected", "collect_date",
                          "checklistcollected", "datacollected", 
                          "collecteddate")


list(
  list(list_tbl_peg1_contact, list_tbl_UD1_contact, 
       list_tbl_peg1, list_tbl_UD1),
  list(channel_peg1_cm, channel_udall_cm, 
       channel_peg1_e1, channel_udall_e1)
) %>% 
  pmap(function(data1, data2){
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
          # rename_with(~replace(.x, str_detect(.x,
          #                                     paste(entryinitial_todetect,
          #                                           collapse = "|")),
          #                      "entry_initial"),
          #             contains(entryinitial_todetect)) %>%
          # rename_with(~replace(.x, str_detect(.x,
          #                                     paste(entrydate_todetect,
          #                                           collapse = "|")),
          #                      "entry_date"),
          #             contains(entrydate_todetect)) %>%
          rename_with(~replace(.x, str_detect(.x,
                                              paste(collectdate_todetect,
                                                    collapse = "|")),
                               "collect_date"),
                      contains(collectdate_todetect)) %>%
          #mutate_at(vars(pegid), as.character) %>%
          # mutate_if(is.character, ~ str_remove_all(.x," ")) %>%
          # mutate_if(is.character, toupper) %>% 
          mutate_if(is.character, ~ replace(.x, .x %in% list(""), NA))
      }) %>%
      set_names(data1) %>% 
      list2env(.,envir = .GlobalEnv)
  })


udall_demo <- tbl_UD1_Patients %>% 
  filter(ud_writtenconsentobtained == 1) %>% 
  select(pegid, sex, age) %>% 
  left_join(`tbl_PEG1_Main Part A` %>% 
              select(pegid, starts_with("a3")), by = "pegid") %>% 
  mutate(
    race_asian = if_else(rowSums(across(starts_with("a3a_")
                                        & !ends_with("specify"))) >= 1, 1, 0),
    race_black = if_else(rowSums(across(starts_with("a3b_")
                                        & !ends_with("specify"))) >= 1, 1, 0),
    race_latino = if_else(rowSums(across(starts_with("a3c_")
                                         & !ends_with("specify"))) >= 1, 1, 0),
    race_native = if_else(rowSums(across(starts_with("a3d_")
                                         & !ends_with("specify"))) >= 1, 1, 0),
    race_white = if_else(rowSums(across(starts_with("a3e_")
                                        & !ends_with("specify"))) >= 1, 1, 0),
    race = case_when(
      race_latino == 1 & race_white == 1 ~ "White",
      rowSums(across(starts_with("race_"))) > 1 ~ "More than One Race",
      race_asian == 1 ~ "Asian",
      race_black == 1 ~ "Black or African American",
      race_latino == 1 & race_asian != 1 & race_black != 1 &
        race_native != 1 ~ "White",
      race_native == 1 ~ "American Indian/ Alaska Native",
      race_white == 1 ~ "White",
      TRUE ~ "Unknown or Not Reported"
    ),
    ethnicgroup = case_when(race_latino == 1 ~ "Hispanic or Latino",
                            race_latino == 0 ~ "Not Hispanic or Latino",
                            TRUE ~ "Unknown/ Not Reported")
  ) %>% 
  set_value_labels(
    sex = c("Male" = 1,
            "Female" = 2)
  ) %>% 
  modify_if(is.labelled, to_factor) %>% 
  select(pegid, sex, race, ethnicgroup)


write_csv(udall_demo, here("tables", "udall_demo.csv"), na = "")
