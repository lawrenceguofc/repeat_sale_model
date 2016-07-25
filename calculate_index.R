# calculate index
calculate_index <- function(sale_all_dup,month = month) {
  if (!is.data.table(sale_all_dup)) {
    sale_all_dup <- data.table(sale_all_dup)
  }
  sale_all_melt <-
    dcast.data.table(sale_all_dup,new_id + zip + fips_code ~ month_d,value.var =
                       'sale_amount')
  # since maybe not every month is available, we need to find a way to find the first month have enough data and beyond
  # create a empty table that all columns of sale_all_melt and all month
  mon_len <-
    round((as.yearmon(month) - as.yearmon('Jan 2000')) * 12,0)
  month_col <-
    format(seq(as.Date('2000-01-01'),length = mon_len + 1,by = 'months'),'%b %Y')
  all_col <- c('new_id','zip','fips_code',month_col)
  # create a empty data frame that has enough data
  sale_empty <- data.table(matrix(ncol = length(all_col)))
  colnames(sale_empty) <- all_col
  # combine sale_empty and sale_all_melt
  sale_all_melt <-
    rbindlist(list(sale_empty,sale_all_melt),fill = TRUE)
  sale_all_melt <- sale_all_melt[-1,]
  # if sale_allcalculate_index <- function(pair_dt){
  # calculate number of transaction each month
  pair_dt <- sale_all_melt
  num_trans <-
    sapply(pair_dt[,4:ncol(pair_dt),with = F],function(x)
      length(which(!is.na(x))))
  # find first month have at least 50 transactions, and all following month average 50 transactions without missing any month
  # otherwise records not enough
  last_month = which(num_trans >= 20)[length(which(num_trans >= 20))]
  #first_month <- as.numeric(last_month)
  
  counter_func <- function(int_counter){
    #int_counter <- last_month
    if(length(int_counter)!=0){
      counter <- int_counter
      while(as.numeric(num_trans[counter]>=20)){
        if (counter == 1) break;
        counter <- counter - 1
      }
      counter
    } else {NA}
  }
  first_month <- counter_func(last_month)
  
#   if(!is.na(last_month)){
#     first_month <- function(last_m)
#     while(first_month>1&as.numeric(num_trans[first_month])>=20){
#       first_month <<- first_month - 1
#       print (first_month)
#     }
#   } else  {first_month <<- NA}
#   
  first_mon_nam <- which(colnames(pair_dt) == names(num_trans)[first_month])
  if ( is.na(last_month) || names(last_month) != month || is.na(first_month) || as.numeric(last_month - first_month) <= 24) {
    stop('not enough data!')
  } else {
    pair_dt <- pair_dt[,c(1,first_mon_nam:ncol(pair_dt)),with = F]
    sale_non_na <-
      apply(pair_dt[,-1,with = F],1,function(x)
        length(which(!is.na(x))))
    pair_dt <- pair_dt[which(sale_non_na == 2),]
    price_pair <-
      t(apply(pair_dt[,-1,with = F],1,function(x)
        x[which(!is.na(x))]))
    pair_dt[,responses:= lapply(1:nrow(pair_dt),function(i) {
      first_sale_px <- as.numeric(price_pair[i,1])
      second_sale_px <- as.numeric(price_pair[[i,2]])
      log(second_sale_px) - log(first_sale_px)
    })]
    sale_all_dummy <- sale_all_dup %>%
      select(new_id,fips_code,zip,sale_amount,month_d) %>%
      arrange(new_id,month_d) %>%
      group_by(new_id) %>%
      mutate(dummy = c(-1,1)) %>%
      ungroup() %>%
      dcast.data.table(.,new_id + fips_code + zip ~ month_d,value.var = 'dummy')
    
    merge_name <-
      colnames(pair_dt)[which(colnames(pair_dt) != 'responses')]
    
    sale_matrix <-
      merge(pair_dt[,c('new_id','responses'),with = F],sale_all_dummy[,merge_name,with =
                                                                        F],by = 'new_id')
    sale_matrix[is.na(sale_matrix)] <- 0
    colnames(sale_matrix)[3:ncol(sale_matrix)] <-
      gsub(' ','_',colnames(sale_matrix)[3:ncol(sale_matrix)])
    model.col <- colnames(sale_matrix)[3:ncol(sale_matrix)]
    first_col <- model.col[1]
    model.col <- model.col[-1]
    model.col <- Reduce(function(x,y)
      paste0(x,'+',y),model.col)
    formula <- as.formula(paste0('responses~0+',model.col))
    sale_matrix$responses <- as.numeric(sale_matrix$responses)
    # lm model (step1)
    rps.lm <- lm(formula,data = sale_matrix)
    rps.coef <- coef(rps.lm)
    rps.res <- residuals(rps.lm) ^ 2
    # lm model on time diff (step2)
    date_pair <- sale_all_dup %>%
      filter(new_id %in% sale_matrix$new_id) %>%
      group_by(new_id) %>%
      mutate(pair = c('sale1','sale2')) %>%
      dcast.data.table(.,new_id + fips_code + zip ~ pair,value.var =
                         'month_d') %>%
      mutate(diff = sale2 - sale1) %>%
      select(new_id,diff)
    date_weight <- mutate(date_pair,var = rps.res)
    # fit a linear model on residual square ~ time diff
    var.lm <- lm(var ~ diff,data = date_weight)
    var.fit <- fitted.values(var.lm)
    # take sqrt root and reprocial
    weights <- 1 / var.fit
    weights[which(weights < 0)] <- 0
    # fit lm again with weights (step3)
    rps.lm2 <- lm(formula,data = sale_matrix,weights = weights)
    rps.coef <- coef(rps.lm2)
    rps.coef <- c(0,rps.coef)
    # repeat sales index
    rpi <- exp(rps.coef) * 100
    names(rpi)[1] <- first_col
    start_year = year(as.yearmon(first_col,format = '%b_%Y'))
    start_month = month(as.yearmon(first_col,format = '%b_%Y'))
    rpi.ts <-
      ts(rpi,start = c(start_year,start_month),frequency = 12)
    # use loess to smooth
    rpi.dt <- data.frame(rpi.ts)
    rownames(rpi.dt)[1] <- first_col
    rpi.dt$time <- as.yearmon(rownames(rpi.dt),format = '%b_%Y')
    rpi.lo <-
      loess(
        rpi.ts ~ as.numeric(time),data = rpi.dt,span = 0.1
      )
    rpi.lo <-
      ts(
        rpi.lo$fitted,start = c(start_year,start_month),frequency = 12
      )
    rpi.lo
  }
}