repeat_sale_pair <- function(cbsa_code = NULL,fip = NULL,month) {
  require(data.table)
  #require(plyr)
  require(dplyr)
  require(lubridate)
  require(reshape2)
  require(RPostgreSQL)
  require(parallel)
  require(foreach)
  require(doParallel)
  require(zoo)
  options(datatable.auto.index = FALSE)
  options(datatable.integer64 = 'character')
  # filter sale by this date
 # month <- 'Apr 2016'
  #cbsa_code <- '41940'
  latest.date = as.Date((as.yearmon(month) + 1 / 12))
  if(!is.null(cbsa_code)){
    msa_fips_code <- map[map$cbsa_code == cbsa_code,]$fips
  } else if (!is.null(fip)){
    msa_fips_code <- fip
  }
    
  # query table; add '00001' as workaround dplyr %in%, clean data for pairing
  my_db <-
    src_postgres(
      dbname = 'econdb',host = "econ.cyrxxzyqccik.us-west-2.redshift.amazonaws.com"
      ,port = 5439,user = 'econuser',password =
        'Econ2016',options = "-c search_path=corelogic"
    )
  
  sale <- try(tbl(my_db,'corelogic_deed') %>%
    dplyr::filter(fips_code %in% c(msa_fips_code,'00001'),
                  universal_land_use_code %in% c('163','112','102','115','116','117'),
                  !is.na(pcl_id_iris_frmtd),pri_cat_code == 'A',
                  multi_apn_flag == '') %>%
    dplyr::select(fips_code,pcl_id_iris_frmtd,apn_sequence_number,
                  universal_land_use_code,sale_amount,sale_date,property_zipcode) %>%
    collect() %>%
    data.table(.) %>%
    setnames(.,old = 'apn_sequence_number',new = 'apn_seq_no') %>%
    mutate(
      .,zip = substr(property_zipcode,start = 1,stop = 5),
      sale_date = as.Date(as.character(sale_date),'%Y%m%d'),
      key_id = paste0(pcl_id_iris_frmtd,'_',apn_seq_no,'_',fips_code),
      sale_amount = as.numeric(sale_amount)
    ) %>%
    mutate(sale_amount = ifelse(sale_amount == 0,NA,sale_amount)) %>%
    #group_by(year(sale_date),fips_code) %>%
    #mutate(
    # low_q = quantile(sale_amount,0.05,na.rm = TRUE),high_q = quantile(sale_amount,0.95,na.rm =
    #                                                                    TRUE)
    #) %>%
    #ungroup() %>%
    #dplyr::filter(sale_amount >= low_q & sale_amount <= high_q) %>%
    dplyr::filter(sale_date < latest.date &
                    sale_date >= '2000-01-01'), silent = TRUE)

  
  RPostgreSQL::dbDisconnect(my_db$con)
  
  # pair function
  if (class(sale)[1] != 'try-error'){
    sale_all_dup = pair_function(sale,fips = msa_fips_code)
    rpi = calculate_index(sale_all_dup,month = month)
    rpi
    } else {
      stop('no data for this geolocation')
    }
 }