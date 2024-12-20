## ---------------------------
##
## Script name: peg_data_entry_tracking.R
##
## Purpose of script: To track the entry status of the data in the prog database
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


list("peg2_contact_manager", "pegprog_contact_manager",
     "peg2_entry1", "pegprog_entry1", "pegprog2_entry1") %>% 
  map(function(data){
    odbcConnect(dsn = data, pwd="youknowwhatitis")
  }) %>% 
  set_names("channel_peg2_cm", "channel_prog_cm",
            "channel_peg2_e1", "channel_prog_e1", 
            "channel_prog2_e1") %>% 
  list2env(.GlobalEnv)



# Read in data ------------------------------------------------------------

#grab tbls interested
tbl_drop_e1 <- c(glue("(?i)main part {letters[2:9]}"), "Find", "_2",
                 "cases", "tracking", "patients", "_SO", "Solvents", 
                 "interviewee", "_medm", "_medd", "_medw", "Antibiotics_", 
                 "_Med7", "calllog", "_resee", "diagnosis", "statistics", 
                 "xtbl", "_Data", "elig", "ExportErrors", "matching", 
                 "medical 7")


list(channel_peg2_e1, channel_prog_e1, channel_prog2_e1) %>% 
  map(function(channel){
    sqlTables(channel)$TABLE_NAME %>% 
      grep("tbl_peg2|tbl_pegprog", x=., 
           value = TRUE, ignore.case = TRUE) %>% 
      discard(~str_detect(.x, paste(tbl_drop_e1, collapse = "|(?i)")))
  }) %>% 
  set_names("list_tbl_peg2", "list_tbl_pegprog",
            "list_tbl_pegprog2") %>% 
  list2env(.GlobalEnv)

list_tbl_pegprog2 %<>% 
  keep(~str_detect(.x, "(?i)prog2"))

list_tbl_pegprog
list(channel_peg2_cm, channel_prog_cm) %>% 
  map(function(channel){
    sqlTables(channel)$TABLE_NAME %>% 
      grep("_patient", x=., 
           value = TRUE, ignore.case = TRUE) %>% 
      discard(~str_detect(.x, "(?i)copy|qry|matching"))
  }) %>% 
  set_names("list_tbl_peg2_contact", "list_tbl_pegprog_contact") %>% 
  list2env(.GlobalEnv)

list_tbl_pegprog_contact <- list_tbl_pegprog_contact %>% 
  discard(~str_detect(.x, "(?i)peg2"))


#read in all tbls in the database and do basic cleaning

var_keep <- c("pegid","entered", "entry", "enter", "collect", 
              "initial", "interviewer", "inputby", "consent", 
              "pdstatus", "prog_status", "prog2_status", "gut_screenstatus",
              "sex", "dob", "yob", "a3", "phys_appt")

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
  list(list_tbl_peg2, list_tbl_pegprog, 
       list_tbl_pegprog2, list_tbl_peg2_contact, list_tbl_pegprog_contact),
  list(channel_peg2_e1, channel_prog_e1, channel_prog2_e1,
       channel_peg2_cm, channel_prog_cm)
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
          rename_with(~replace(.x, str_detect(.x,
                                              paste(entryinitial_todetect,
                                                    collapse = "|")),
                               "entry_initial"),
                      contains(entryinitial_todetect)) %>%
          rename_with(~replace(.x, str_detect(.x,
                                              paste(entrydate_todetect,
                                                    collapse = "|")),
                               "entry_date"),
                      contains(entrydate_todetect)) %>%
          rename_with(~replace(.x, str_detect(.x,
                                              paste(collectdate_todetect,
                                                    collapse = "|")),
                               "collect_date"),
                      contains(collectdate_todetect)) %>%
          #mutate_at(vars(pegid), as.character) %>%
          # mutate_if(is.character, ~ str_remove_all(.x," ")) %>%
          # mutate_if(is.character, toupper) %>% 
          mutate_if(is.character, ~ replace(.x, .x %in% list(""), NA)) %>% 
          filter(str_length(pegid)>8)
      }) %>%
      set_names(data1) %>% 
      list2env(.,envir = .GlobalEnv)
  })

# Data cleaning -----------------------------------------------------------

tbl_peg2_updrs <- `tbl_PEG2_Physician Rating Scale OFF` %>% 
  select(pegid:collect_date) %>% 
  rows_patch(`tbl_PEG2_Physician Rating Scale ON` %>% 
               filter(pegid %in% `tbl_PEG2_Physician Rating Scale OFF`$pegid), 
             by = "pegid")

rm(`tbl_PEG2_Physician Rating Scale OFF`, `tbl_PEG2_Physician Rating Scale ON`,
   `tbl_PEG2_Physician Rating Scale`)

#82479RW40: no PD, 83469AW22: ineligible, too ill
list(
  list(tbl_PEG2_Patients, tbl_PEGProg_Patients, tbl_PEGProg_Patients),
  list("status_pdstatus", "pegprog_pdstatus", "pegprog2_pdstatus"),
  list("consent_participate", "consent_participate", "consent_participate2"),
  list("phys_apptdate", "pegprog_phys_apptdate", "pegprog2_phys_apptdate")
) %>% 
  pmap(function(data1, data2, data3, data4){
    data1 %>% 
      filter(!is.na(.[[data3]])) %>% 
      # filter(pegid %in% tbl_peg2_patients_clean$pegid) %>% 
      rename(consent_date = data3) %>% 
      filter(!is.na(.[[data2]]) | !is.na(.[[data4]])) %>% 
      # filter(str_detect(.[[data2]], "(?i)definite|probable|possible")) %>% 
      # filter(pegid %notin% c("82479RW40", "83469AW22")) %>% 
      select(pegid, consent_date)
  }) %>% 
  set_names("tbl_peg2_patients_clean", "tbl_pegprog_patients_clean",
            "tbl_pegprog2_patients_clean") %>% 
  list2env(.GlobalEnv)


list(tbl_peg2_patients_clean, tbl_pegprog_patients_clean,
     tbl_pegprog2_patients_clean) %>% 
  map(function(data){
    data %>% 
      filter(pegid %in% tbl_peg2_patients_clean$pegid)
  }) %>% 
  set_names("tbl_peg2_patients_clean", "tbl_pegprog_patients_clean",
            "tbl_pegprog2_patients_clean") %>% 
  list2env(.GlobalEnv)



tbls_todrop <- c("channel", "list_tbl", "finalSocial", "finalvaccine", 
                 "antibiotics", "constipation",
                 "finalmobact", "lifestylephysical", "patients")


tbl_list_peg2 <- 
  do.call("list", mget(grep("peg2", names(.GlobalEnv), 
                            value = T, ignore.case = TRUE) %>% 
                         discard(~str_detect(.x, 
                                             paste(tbls_todrop, 
                                                   collapse = "|(?i)"))))) %>% 
  .[order(names(.))]

tbl_list_pegprog <- 
  do.call("list", mget(grep("prog_", names(.GlobalEnv), 
                            value = T, ignore.case = TRUE) %>% 
                         discard(~str_detect(.x, 
                                             paste(tbls_todrop, 
                                                   collapse = "|(?i)"))))) %>% 
  .[order(names(.))]

tbl_list_pegprog2 <- 
  do.call("list", mget(grep("prog2", names(.GlobalEnv), 
                            value = T, ignore.case = TRUE) %>% 
                         discard(~str_detect(.x, 
                                             paste(tbls_todrop, 
                                                   collapse = "|(?i)"))))) %>% 
  .[order(names(.))]


tbl_names_1 <- list("main interview", "medical checklist", "pdmedication",
                    "nmss", "gds", "mmse", "updrs")

tbl_names_2 <- list("berlin", "final interview", 
                    "lifestyle", "medical checklist", 
                    "pdmedication", "nmss", "gds", "mmse", "updrs", "sle", 
                    "patientquestionnaire")

tbl_need_entry_all <- list(
  list(tbl_list_peg2, tbl_list_pegprog, tbl_list_pegprog2),
  list(tbl_names_1, tbl_names_2, tbl_names_2)
) %>% 
  pmap(function(data1, data2){
    list(data1, data2) %>% 
      pmap(function(df1, df2){
        df1 %>% 
          mutate(
            entry_status = 
              case_when(
                str_detect(entry_initial, "(?i)x99") ~ "missing/never completed",
                str_detect(entry_initial, "9") 
                & !str_detect(entry_initial, "x") ~ "Partially completed",
                str_detect(entry_initial, "xxx") ~ "Completed without initial",
                !is.na(entry_initial) & !is.na(entry_date) & 
                  !str_detect(entry_initial, "x|9") ~ "Entry completed",
                is.na(entry_initial) & is.na(entry_date) ~ "Entry needed"
                ), 
            form = df2) %>% 
          select(pegid, entry_initial, entry_date, entry_status, form) %>% 
          group_by(pegid) %>% 
          slice(1) %>% 
          ungroup()
      })
  }) 


list(
  tbl_need_entry_all,
  list("peg2", "pegprog", "pegprog2"),
  list(tbl_names_1, tbl_names_2, tbl_names_2)
) %>% 
  pmap(function(data1, data2, data3){
    data1 %>% 
      set_names(glue("tbl_{data2}_{data3}_need_entry")) %>% 
      list2env(.,envir = .GlobalEnv)
  })

tbl_need_entry_all %>% 
  map(function(data){
    data %>% 
      bind_rows()
  }) %>% 
  set_names("tbl_peg2_tracking", "tbl_pegprog_tracking",
            "tbl_pegprog2_tracking") %>% 
  list2env(.,envir = .GlobalEnv)



# Grab demographic info ---------------------------------------------------

pegprog_demo <- tbl_pegprog_patients_clean %>% 
  left_join(tbl_PEG2_Patients %>% 
              select(pegid, sex, dob, yob), by = "pegid") %>% 
  left_join(`tbl_PEG2_Main Part A` %>% 
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

write_csv(pegprog_demo, here("tables", "pegprog_demo.csv"), na = "")


# Visualize the data ------------------------------------------------------
plotlist <- list(
  list(tbl_peg2_tracking, tbl_pegprog_tracking,
       tbl_pegprog2_tracking),
  list(tbl_peg2_patients_clean, tbl_pegprog_patients_clean,
       tbl_pegprog2_patients_clean),
  c("peg2", "pegprog", "pegprog2")
) %>% 
  pmap(function(data1, data2, data3){
    data1 %>% 
      right_join(data2 %>%
                   select(pegid), by = "pegid") %>% 
      pivot_wider(
        id_cols = pegid,
        names_from = form,
        values_from = entry_status
      ) %>%
      select(-any_of("NA")) %>% 
      mutate_all(~replace(.x, is.na(.), "missing/never completed")) %>%
      pivot_longer(
        cols = !pegid,
        names_to = "form",
        values_to = "entry_status"
      ) %>% 
      # count(form, entry_status) %>%
      # group_by(form) %>% 
      # mutate(pct = prop.table(n)*100) %>% 
      ggplot(aes(x = form, fill = entry_status)) + 
      geom_bar()+
      geom_text(stat = "count", aes(label = after_stat(count)), 
                position = position_stack(vjust = 0.5))+
      # geom_col(position = "fill") +
      scale_fill_manual(values = c(
        "Entry completed" = "#8BC496",
        "missing/never completed" = "#CEC289",
        "Entry needed" = "#E9B3AD",
        "Completed without initial" ="#D6B1CB",
        "Partially completed" = "#ADBFD9"
      )) +
      coord_flip() +
      labs(title =  str_c(
        Sys.Date(), " " ,data3,
        " entry tracking (N = ", nrow(data2
        ), ")"),
        fill = "Entry status",
        y = "Count",
        x = "Form")
  })

plotlist
png(file=here("figures", "old_entry_tracking.png"), 
    width = 1920, height = 1080, units = "px", res = 125)
ggarrange(plotlist = rev(plotlist),
          ncol = 3, nrow = 1,
          common.legend = T,
          legend = "bottom")
dev.off()


peg2_entry_tracking <- tbl_peg2_tracking %>% 
  filter(pegid %in% tbl_peg2_patients_clean$pegid) %>% 
  pivot_wider(
    id_cols = pegid,
    names_from = form,
    values_from = entry_status
  ) %>% 
  #select(-c(antibiotics, constipation)) %>% 
  mutate_all(~replace(.x, is.na(.), "Entry completed")) %>% 
  mutate(status = if_else(rowSums(select(., -pegid) == "Entry needed") > 0, 
                          "need entry", "clean")) %>% 
  filter(status == "need entry")



pegprog_entry_tracking_1 <- tbl_pegprog_tracking %>% 
  filter(pegid %in% tbl_pegprog_patients_clean$pegid) %>% 
  pivot_wider(
    id_cols = pegid,
    names_from = form,
    values_from = entry_status
  ) %>% 
  #select(-c(antibiotics, constipation)) %>% 
  mutate_all(~replace(.x, is.na(.), "Entry completed")) %>% 
  mutate(status = if_else(rowSums(select(., -pegid) == "Entry needed") > 0, 
                          "need entry", "clean")) %>% 
  filter(status == "need entry")

pegprog2_entry_tracking <- tbl_pegprog2_tracking %>% 
  filter(pegid %in% tbl_pegprog2_patients_clean$pegid) %>% 
  pivot_wider(
    id_cols = pegid,
    names_from = form,
    values_from = entry_status
  ) %>% 
  #select(-c(antibiotics, constipation)) %>% 
  mutate_all(~replace(.x, is.na(.), "Entry completed")) %>% 
  mutate(status = if_else(rowSums(select(., -pegid) == "Entry needed") > 0, 
                          "need entry", "clean")) %>% 
  filter(status == "need entry")

pegprog_entry_tracking_final <- pegprog_entry_tracking_1 %>% 
  left_join(pegprog_entry_tracking_2 %>% select(pegid, antibiotics, constipation), by = "pegid")


tbl_pegprog_antibio_constipation_tracking <- tbl_pegprog_tracking %>% 
  filter(is.na(entry_initial) & form %in% c("antibiotics", "constipation") 
         & pegid %in% tbl_pegprog_patients_clean$pegid) %>% 
  pivot_wider(
    id_cols = pegid,
    names_from = form,
    values_from = entry_status
  ) 

write_csv(pegprog_entry_tracking_final, here("tables", "pegprog_entry_tracking_final.csv"))
