
# read in demo data -------------------------------------------------------

demo_data_full <- read_excel(
  here("tables", "2024_peg_participants_summary_updated.xlsx"),
  sheet = "tbl_demo_2024_full"
)


# grab demo info for people with blood draw -------------------------------

list(tbl_PEG3_active_case, tbl_PEG3_active_hhctrl,
     tbl_PEG3_active_popctrl) %>% 
  map(function(data){
    data %>% 
      select(pegid, consent_date, enroll_date) %>% 
      left_join(demo_data_full, by = "pegid")
  }) %>% 
  set_names("tbl_peg3_demo_active_case", "tbl_peg3_demo_active_hhctrl",
            "tbl_peg3_demo_active_popctrl") %>% 
  list2env(envir = .GlobalEnv)
