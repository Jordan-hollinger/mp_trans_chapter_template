library(lehdr)
library(tidyverse)
library(readr)

##This workflow was developed using the sample workflows on the Census website here: https://lehd.ces.census.gov/data/lehd-code-samples/sections/lodes/basic_examples.html#loading-data


####Read LODES Data Function####

#Reads LODES RAC, WAC, or OD data based on the specified type.
read_lodes_data <- function(data_type,
                            state,
                            segment,
                            job_type,
                            year,
                            main = TRUE,
                            base = "https://lehd.ces.census.gov/data/lodes/LODES8/") {
#Notes
  #' @param data_type One of 'rac', 'wac', or 'od' to specify the dataset.
  #' @param state Lowercase two-character state abbreviation (e.g., 'nj').
  #' @param segment Segment type (e.g., 'SA01'). Ignored if 'data_type' is 'od'.
  #' @param job_type Job type (e.g., 'JT00').
  #' #' ob Type, can have a value of “JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for All
  #' Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05” for
  #' Federal Primary Jobs. 
  #' @param year Year of data (e.g., 2004).
  #' @param main Whether to read the 'main' file type. Only used if 'data_type' is 'od'.
  #' @param base Base file path to read data from. Can be URL or local file path.
  #'
  #' @returns The requested LODES dataset.
  # Validate data type
  
  if (!(data_type %in% c("rac", "wac", "od"))) {
    stop("Invalid data type. Choose from 'rac', 'wac', or 'od'.")
  }
  
  # Construct file URL based on data type
  if (data_type == "od") {
    file_url <- if (main) {
      paste0(base, state, "/od/", state, "_od_main_", job_type, "_", year, ".csv.gz")
    } else {
      paste0(base, state, "/od/", state, "_od_aux_", job_type, "_", year, ".csv.gz")
    }
    col_types <- cols(h_geocode = col_character(), w_geocode = col_character())
  } else {
    file_url <- sprintf(
      "%s%s/%s/%s_%s_%s_%s_%d.csv.gz",
      base,
      state,
      data_type,
      state,
      data_type,
      segment,
      job_type,
      year
    )
    col_types <- if (data_type == "rac") {
      cols(h_geocode = col_character())
    } else {
      cols(w_geocode = col_character())
    }
  }
  
  # Read CSV with specified column types
  df <- read_csv(file_url, col_types = col_types)
  
  return(df)
}

####Run Function For OD, RAC or WAC####
rac_data <- read_lodes_data("rac", "ma", "SA01", "JT00", 2022)
wac_data <- read_lodes_data("wac", "ma", "SA01", "JT00", 2022)
od_data <- read_lodes_data("od", "ma", NULL, "JT00", 2022)
od_data_aux <- read_lodes_data("od", "ma", NULL, "JT00", 2022, main = FALSE)




####Geo crosswalk Function####
#Reads LODES Geography Crosswalk given state.

read_crosswalk <- function(state, cols = "all", base = "https://lehd.ces.census.gov/data/lodes/LODES8/") {
  
  #' @param state Lowercase two-character state abbreviatoin (e.g., 'nj').
  #' @param cols List of columns to read from the crosswalk in addition to tabblk2020 (Block ID). If "all", all columns will be read and returned.
  #' @param base Base file path to read data from. Can be URL or local file path.
  #'
  #' @returns The requested LODES crosswalk.
  
  
  # Construct file URL
  file_url <- paste0(base, state, "/", state, "_xwalk.csv.gz")
  # Read the full dataset first if all columns are needed
  if (identical(cols, "all")) {
    return(read_csv(file_url, col_types = cols(tabblk2020 = col_character())))
  }
  # Ensure tabblk2020 (GEOID) is included in the selected columns
  cols <- unique(c("tabblk2020", cols))
  # Read only the specified columns
  return(read_csv(
    file_url,
    col_types = cols(.default = col_character()),
    col_select = all_of(cols)
  ))
}

####Run Crosswalk Function#####
#for MA towns, important to include ctycsubname which is county subdivision
crosswalk <- read_crosswalk("ma", cols = c("cty", "ctyname", "ctycsubname"))


####Aggregate data to Town Level#####

# Merge OD data with Crosswalk on block IDs
od_w_crosswalk <- od_data |> 
  left_join(crosswalk, by = c("h_geocode" = "tabblk2020")) |> 
  rename(home_town = ctycsubname,
         home_cnty_fips = cty,
         home_cnty_name = ctyname) |> 
  left_join(crosswalk, by = c("w_geocode" = "tabblk2020")) |> 
  rename(work_town = ctycsubname,
         work_cnty_fips = cty,
         work_cnty_name = ctyname)

# Aggregate at town level
od_town <- od_w_crosswalk  |> 
  group_by(home_town, work_town) |> 
  summarise(
    all_jobs = sum(S000)
  )

od_town_auburn <- od_town |> 
  filter(str_detect(home_town, "Auburn")) |> 
  arrange(desc(all_jobs))

od_town_auburn |> summarise(sum = sum(all_jobs))



#merge od aux data with crosswalk  
od_aux_w_crosswalk <- od_data_aux |> 
  left_join(crosswalk, by = c("h_geocode" = "tabblk2020")) |> 
  rename(home_town = ctycsubname,
         home_cnty_fips = cty,
         home_cnty_name = ctyname) |> 
  left_join(crosswalk, by = c("w_geocode" = "tabblk2020")) |> 
  rename(work_town = ctycsubname,
         work_cnty_fips = cty,
         work_cnty_name = ctyname)

od_aux_w_crosswalk|> 
  filter(str_detect(work_town, "Auburn"))
