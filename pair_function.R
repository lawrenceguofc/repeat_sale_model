pair_function <- function(sale,fips = msa_fips_code){
  # fips = msa_fips_code
  pairs <- sale %>%
    group_by(key_id) %>%
    dplyr::summarise(count=n()) %>%
    filter(count>1) %>%
    dplyr::select(key_id)
  
    pairs <- pairs$key_id
    if(identical(pairs,character(0))){
      stop('there is no sale pair')
    } else {
    # subset sale to only contain sale pairs
    sale_pair <- sale[key_id %in% pairs,]
    sale_pair <- arrange(sale_pair,sale_date)
    sale_pair <- sale_pair[,c('key_id','sale_amount','sale_date','universal_land_use_code','zip','fips_code'),with=F]
    sale_pair <- dplyr::mutate(sale_pair,idx = seq_len(n()))
    # create sale pairs where pair sale with its previous sale
    pair_func <- function(idx){
      mat <- sapply(1:(length(idx)-1),function(i) c(idx[i],idx[i+1]))
      mat <- data.frame(t(mat))
      mat
    }
    # parallel processing 
    # calculate the number of cores
    no_cores <- detectCores() - 2
    # initiate cluster
    cluster <- makeCluster(no_cores)
    registerDoParallel(cluster)
    
    # function to pass fips_code to sale_pair
    pairing_func <- function(fip,df){
      require(dplyr)
      require(data.table)
      df %>%
        filter(fips_code == fip) %>%
        group_by(key_id) %>%
        do(pair_func(idx)) %>%
        ungroup() %>%
        setnames(.,old=c('X1','X2'),new=c('sale1','sale2'))
    }
    
    sale_pairs <-  foreach(fip = unique(sale_pair$fips_code),.multicombine=TRUE,.combine= rbind,.verbose=FALSE) %dopar%
      pairing_func(fip,df = sale_pair)
    
    # stop cluster
    stopCluster(cluster)
    # clean sale_pair data
    sale_com <- cbind(sale_pair[sale_pairs$sale1,],sale_pair[sale_pairs$sale2,])
    colnames(sale_com)[1:7] <- paste0(colnames(sale_com)[1:7],'_s1')
    colnames(sale_com)[8:14] <- paste0(colnames(sale_com)[8:14],'_s2')
    sale_com <- data.table(sale_com)
    sale_com[,c('idx_s1','idx_s2') := NULL, with=F]
    # remove any pairs have > 10 years or less than 6 month
    sale_com[,month_diff:= list((as.yearmon(sale_date_s2) - as.yearmon(sale_date_s1))*12)]
    sale_com<-sale_com[month_diff<120&month_diff>6,]
    # remove any obivous property type change
    sale_com[universal_land_use_code_s1 %in% c('163','109'),master_code_1:='SFR']
    sale_com[universal_land_use_code_s1 %in% c('102','106','112','111','114','115','116','117'),master_code_1:='MFR']
    sale_com[universal_land_use_code_s2 %in% c('163','109'),master_code_2:='SFR']
    sale_com[universal_land_use_code_s2 %in% c('102','106','112','111','114','115','116','117'),master_code_2:='MFR']
    sale_com <- sale_com[(master_code_1==master_code_2),]
    # calculate median price change by change
    sale_com[,px.change:= round((sale_amount_s2/sale_amount_s1)^(12/month_diff)-1,4)]
    # remove 5% to 95%
    px.ch.lim <- quantile(sale_com$px.change,probs=c(0.05,0.95),na.rm=TRUE)
    sale_com <- sale_com[px.change>px.ch.lim[1]&px.change<px.ch.lim[2],]
    sale_com$unique_id <- seq_len(nrow(sale_com))
    # remove sale if the zip doesn't equal to eachother
    sale_com <- sale_com[zip_s1==zip_s2,]
    # melt sale_com
    sale_1 <- sale_com[,c('unique_id','fips_code_s1','zip_s1','sale_date_s1','sale_amount_s1'),with=F]
    setnames(sale_1,old=colnames(sale_1),new=gsub('_s1','',colnames(sale_1)))
    sale_2 <- sale_com[,c('unique_id','fips_code_s2','zip_s2','sale_date_s2','sale_amount_s2'),with=F]
    setnames(sale_2,old=colnames(sale_2),new=gsub('_s2','',colnames(sale_2)))
    sale_all <- rbindlist(list(sale_1,sale_2))
    sale_all <- arrange(sale_all,unique_id)
    # sale_month
    sale_all$month <- as.yearmon(sale_all$sale_date)
    # for each unique pair (two sales date) create 2 duplicates, 
    # and assign dummy month so it can be included for the two following month
    sale_all_dup <- sale_all[,cbind(trail=0:2),by=c('unique_id','month')]
    sale_all_dup <- merge(sale_all_dup,sale_all,by=c('unique_id','month'))
    sale_all_dup <- mutate(sale_all_dup,new_id = paste0(unique_id,'_',V1),month_d = month + V1/12)
    # new_id have sales record beyond Dec.2015
    rm_new_id <- unique(sale_all_dup[month_d > as.yearmon(month),]$new_id)
    sale_all_dup <- sale_all_dup[!new_id %in% rm_new_id,]
    sale_all_dup
    } 
}
