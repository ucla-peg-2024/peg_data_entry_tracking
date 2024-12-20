## ---------------------------
##
## Script name: 
## Purpose of script: 
##
## Author: Yufan Gong
##
## Date Created: `Sys.Date()`
##
## Date Modified: `Sys.Date()`
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

#### 1. check peg2 form entry status for cannot complete cases

# find records where at least one form from "moca", "gds", "updrs", 
# "medicalchecklist", "timeline", "lifehistory" is Cannot complete

list(df_case_sample_entry, df_hhctrl_sample_entry, df_popctrl_sample_entry) %>% 
  map(~filter_at(.x, vars(any_of(c("moca", "gds", "updrs", "medicalchecklist", 
                                   "timeline", "lifehistory"))), 
                 any_vars(. == "Cannot complete"))) %>% 
  set_names("tbl_case_cannotcomplete", "tbl_hhctrl_cannotcomplete", 
            "tbl_popctrl_cannotcomplete") %>% 
  list2env(.GlobalEnv)


tbl_old_forms_case <- tbl_peg2_tracking %>% 
  filter(form %in% c("mmse", "gds", "updrs", 
                     "medical checklist", "main interview"))

setdiff(tbl_case_cannotcomplete$pegid, tbl_peg2_patients_clean$pegid)

tbl_peg2_form_completion <- tbl_old_forms_case %>% 
  right_join(tbl_peg2_patients_clean %>%
               select(pegid), by = "pegid") %>% 
  filter(pegid %in% tbl_case_cannotcomplete$pegid) %>%
  pivot_wider(
    id_cols = pegid,
    names_from = form,
    values_from = entry_status
  ) %>% 
  rows_insert(
    tibble(pegid = "86766NH47", 
           `main interview` = "Entry completed", 
           `medical checklist`= "Entry completed",
           gds = "Entry completed", mmse = "Entry completed", 
           updrs = "Entry completed")
  ) %>% 
  mutate(timeline = "Entry completed") %>% 
  rename(
    medicalchecklist = `medical checklist`,
    lifehistory = `main interview`
  ) %>% 
  rename_at(vars(!contains("pegid")), ~paste0(., "_old"))

write_csv(tbl_peg2_form_completion, 
          file = here("tables", "tbl_peg2_form_completion.csv"))

tbl_old_forms %>% 
  right_join(tbl_peg2_patients_clean %>%
               select(pegid), by = "pegid") %>% 
  filter(pegid %in% tbl_case_cannotcomplete$pegid) %>%
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
    Sys.Date(), " " ,"pegprog",
    " entry tracking (N = ", nrow(tbl_pegprog_patients_clean
    ), ")"),
    fill = "Entry status",
    y = "Count",
    x = "Form")

#### 2. check old hhctrl and popctrl form entry status for cannot complete ctrls

list("oldhhctrl_contact_manager", "oldhhctrl_entry1",
     "cgep_contact_manager", "cgep_entry1",
     "cgepprog_contact_manager") %>% 
  map(~odbcConnect(dsn = .x, pwd = "PD_08")) %>% 
  set_names("channel_oldhhctrl", "channel_oldhhctrl_entry1",
            "channel_cgep", "channel_cgep_entry1",
            "channel_cgep_prog") %>% 
  list2env(.GlobalEnv)

list(channel_oldhhctrl, channel_oldhhctrl_entry1,
     channel_cgep, channel_cgep_entry1,
     channel_cgep_prog) %>% 
  map(~sqlTables(.x)$TABLE_NAME) %>% 
  set_names("channel_oldhhctrl", "channel_oldhhctrl_entry1",
            "channel_cgep", "channel_cgep_entry1",
            "channel_cgep_prog")


list_tbl_oldhhctrl_entry1 <- sqlTables(channel_oldhhctrl_entry1)$TABLE_NAME %>% 
  grep("Main_Part_A|gds|medical_common|mmse|occupation|residence", 
       x=., value = TRUE, ignore.case = TRUE) %>% 
  discard(~str_detect(.x, "(?i)common2|(?i)A_2"))


list_tbl_cgep_entry1 <- sqlTables(channel_cgep_entry1)$TABLE_NAME %>% 
  grep("Main Part A|Depression Scale|Medical 1 - 2|Mini Mental|address A", 
       x=., value = TRUE, ignore.case = TRUE) %>% 
  discard(~str_detect(.x, "(?i)A_2|(?i)Medical 1 - 2_2|(?i)Scale_2|(?i)Exam_2|MM"))

list_tbl_oldhhctrl_entry1_name <- list_tbl_oldhhctrl_entry1 %>% 
  map(~str_c("tbl_", .x, "_oldhhctrl_e1"))

list_tbl_cgep_entry1_name <- list_tbl_cgep_entry1 %>%
  map(~str_c("tbl_", .x, "_cgep_e1"))



#list potential variables which need to be dropped/kept
var_drop_ctrl <- c()

var_keep_ctrl <- c("mailid", "pegid", "entered", "entry", "enter", "collect", 
              "initial", "interviewer", "inputby")

entryinitial_todetect <- c("entered_initial", "enteredby", "entry_initial")
entrydate_todetect <- c("entered_date", "date_entered", "entry_date")
collectdate_todetect <- c("collected_date", "date_collected", "collect_date")


#read in all tbls in the database and do basic cleaning
list(
  list(list_tbl_oldhhctrl_entry1, list_tbl_cgep_entry1),
  list(channel_oldhhctrl_entry1, 
       channel_cgep_entry1),
  list(list_tbl_oldhhctrl_entry1_name, list_tbl_cgep_entry1_name)
) %>% 
  pmap(function(data1, data2, data3){
    data1 %>% 
      map(function(data){
        sqlQuery(data2,
                 glue::glue("select * from [{data}];")) %>% 
          as_tibble() %>% 
          # select(-matches(paste(var_drop, sep = "|"),
          #                 ignore.case = TRUE)) %>%
          select(matches(paste(var_keep, sep = "|"), 
                         ignore.case = TRUE)) %>% 
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
          # mutate_at(vars(pegid), as.character) %>%
          # mutate_if(is.character, ~ str_remove_all(.x," ")) %>%
          # mutate_if(is.character, toupper) %>% 
          mutate_if(is.character, ~ replace(.x, .x %in% list(""), NA))
      }) %>%
      set_names(data3) %>% 
      list2env(.,envir = .GlobalEnv)
  })


tbl_old_forms_hhctrl <- list(
  list(tbl_tbl_Hh_GDS_oldhhctrl_e1, 
       tbl_tbl_Hh_MMSE_oldhhctrl_e1, 
       tbl_tbl_Hh_Medical_common_oldhhctrl_e1,
       tbl_tbl_Hh_Main_Part_A_oldhhctrl_e1),
  list("gds", "mmse", 
       "medicalchecklist", "lifehistory")) %>% 
  pmap(function(data1, data2){
    data1 %>% 
      select(any_of(c("pegid", "entry_initial", "entry_date", "inputby"))) %>% 
      mutate(form = data2)
  }) %>% 
  bind_rows() %>% 
  mutate(entry_status = if_else(!is.na(entry_initial) | !is.na(inputby), 
                                "Entry completed",
                                "Cannot complete"))


tbl_old_forms_cgep <- list(
  list(`tbl_Physician Depression Scale_cgep_e1`, 
       `tbl_Physician Mini Mental State Exam_cgep_e1`,
       `tbl_Medical 1 - 2_cgep_e1`,
       `tbl_Main Part A_cgep_e1`),
  list("gds", "mmse", 
       "medicalchecklist", "lifehistory")) %>% 
  pmap(function(data1, data2){
    data1 %>% 
      select(any_of(c("pegid", "entry_initial", "entry_date", "inputby"))) %>% 
      mutate(form = data2)
  }) %>% 
  bind_rows() %>% 
  mutate(entry_status = if_else(!is.na(entry_initial) | !is.na(inputby) | !is.na(entry_date), 
                                "Entry completed",
                                "Cannot complete"))

tbl_oldhhctrl_form_completion <- tbl_old_forms_hhctrl %>% 
  filter(pegid %in% tbl_hhctrl_cannotcomplete$pegid) %>% 
  pivot_wider(
    id_cols = pegid,
    names_from = form,
    values_from = entry_status
  ) %>% 
  mutate_all(~replace(.x, is.na(.), "Cannot complete")) %>% 
  mutate(timeline = "Entry completed") %>% 
  rows_update(
    tibble(pegid = "70093CS44", 
           timeline = "Cannot complete")
    ) %>% 
  rename_at(vars(!contains("pegid")), ~paste0(., "_old"))

tbl_cgep_form_completion <- tbl_old_forms_cgep %>% 
  filter(pegid %in% tbl_popctrl_cannotcomplete$pegid) %>% 
  pivot_wider(
    id_cols = pegid,
    names_from = form,
    values_from = entry_status
  ) %>% 
  mutate_all(~replace(.x, is.na(.), "Cannot complete")) %>% 
  mutate(timeline = "Entry completed")%>% 
  rename_at(vars(!contains("pegid")), ~paste0(., "_old"))

write_csv(tbl_oldhhctrl_form_completion, 
          file = here("tables", "tbl_oldhhctrl_form_completion.csv"))
write_csv(tbl_cgep_form_completion, 
          file = here("tables", "tbl_cgep_form_completion.csv"))
